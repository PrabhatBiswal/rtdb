#!/bin/bash
#
# Fire one loadsim run across every Project=rtdb load client at once, and collect their reports.
#
#   deploy/run-loadsim.sh <conns-per-client> <rate-per-client> <seconds> [extra loadsim flags...]
#
# THE POINT OF THIS SCRIPT, and why it exists rather than a one-liner: the load clients fetch the
# shard's jwt secret THEMSELVES, through their instance role, inside the command. The secret is
# never an argument to SendCommand.
#
# On 2026-08-29 I did it the other way — passed the secret as `docker run -e RTDB_DEV_SECRET=<value>`
# inside the command text — and SSM stored that command verbatim. The production jwt secret ended up
# in Run Command history twenty times over, where it is readable by any principal in the account with
# ssm:ListCommands, for the thirty days SSM retains it, with no delete API to take it back. The
# secret had to be rotated. The load clients were already running under the GATEWAY instance profile,
# which can read /rtdb/* directly, so the whole exposure bought nothing at all.
#
# Anything interpolated into $SCRIPT below is stored by AWS and readable later. Put identifiers
# there, never values.
set -euo pipefail

REGION=${AWS_REGION:-ap-south-1}
ENDPOINT=${RTDB_ENDPOINT:?set RTDB_ENDPOINT to the gateway to load, e.g. wss://rtdb.example.com}
TAG=${LOADSIM_TAG:-ac74d22}

if [ $# -lt 3 ]; then
  echo "usage: $0 <conns-per-client> <rate-per-client> <seconds> [extra loadsim flags...]" >&2
  exit 2
fi
CONNS=$1; RATE=$2; SECS=$3; shift 3
EXTRA="$*"

REGISTRY=$(aws sts get-caller-identity --query Account --output text).dkr.ecr.${REGION}.amazonaws.com

# Whichever load clients happen to exist right now. `terraform apply -var load_clients=N` creates
# them; this discovers them rather than carrying a stale list of instance ids.
# `mapfile` is bash 4; macOS ships bash 3.2, so read the list the portable way.
CLIENTS=$(aws ec2 describe-instances \
  --filters Name=tag:Project,Values=rtdb Name=tag:Name,Values='rtdb-load*' \
            Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].InstanceId' --output text | tr '\t' '\n' | grep -v '^$' || true)

if [ -z "$CLIENTS" ]; then
  echo "no running load clients. terraform apply -var load_clients=N first." >&2
  exit 1
fi
echo "dispatching to $(echo "$CLIENTS" | wc -l | tr -d ' ') client(s), ${CONNS} conns each at ${RATE} w/s for ${SECS}s"

# The \$( ) below is BACKSLASH-ESCAPED, so this shell leaves it alone and the INSTANCE evaluates it
# at run time using its own role. Everything unescaped here (region, registry, counts) is expanded
# now and stored by AWS; everything escaped runs there. The secret is only ever on the far side.
SCRIPT=$(cat <<EOF
set -o pipefail
aws ecr get-login-password --region ${REGION} \
  | docker login --username AWS --password-stdin ${REGISTRY} >/dev/null 2>&1
SECRET=\$(aws ssm get-parameter --name /rtdb/prod/jwt_secret --with-decryption \
          --query Parameter.Value --output text --region ${REGION})
if [ -z "\$SECRET" ]; then echo "could not read jwt_secret via instance role" >&2; exit 1; fi
docker run --rm --network host --ulimit nofile=1048576 \
  -e RTDB_DEV_SECRET="\$SECRET" \
  ${REGISTRY}/rtdb-loadsim:${TAG} \
  --gateways ${ENDPOINT} --conns ${CONNS} --procs 4 --rate ${RATE} --seconds ${SECS} ${EXTRA} 2>&1 | tail -22
rc=\$?
echo "[loadsim exit \$rc]"
exit \$rc
EOF
)

# The command travels as JSON via a file: shorthand --parameters cannot survive the quoting, and
# a heredoc through argv would mangle it.
IDS=""
for c in $CLIENTS; do
  J=$(mktemp)
  SCRIPT="$SCRIPT" INST="$c" OUT="$J" python3 -c '
import json, os
json.dump({"InstanceIds": [os.environ["INST"]], "DocumentName": "AWS-RunShellScript",
           "Parameters": {"commands": [os.environ["SCRIPT"]], "executionTimeout": ["3600"]}},
          open(os.environ["OUT"], "w"))'
  IDS="$IDS $(aws ssm send-command --cli-input-json "file://$J" --query 'Command.CommandId' --output text):$c"
  rm -f "$J"
done

for pair in $IDS; do
  cid=${pair%%:*}; inst=${pair##*:}
  st=Pending
  for _ in $(seq 1 400); do
    sleep 5
    st=$(aws ssm get-command-invocation --command-id "$cid" --instance-id "$inst" \
         --query Status --output text 2>/dev/null || echo Pending)
    case "$st" in InProgress|Pending|Delayed) ;; *) break ;; esac
  done
  echo "----- $inst ($st) -----"
  aws ssm get-command-invocation --command-id "$cid" --instance-id "$inst" \
    --query StandardOutputContent --output text
done
