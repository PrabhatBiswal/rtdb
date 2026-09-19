#!/bin/bash
#
# Fire one loadsim run across every Project=rtdb load client at once, and collect their reports.
#
#   deploy/run-loadsim.sh <conns-per-client> <rate-per-client> <seconds> [extra loadsim flags...]
#
# ONE DATABASE PER CLIENT (§5.30): set LOADSIM_NS_LIST=a,b,c,d and client i gets `--ns <i-th>`.
# The i is the position in `describe-instances` output SORTED by instance id, printed as a mapping
# before anything is dispatched — an unsorted list reorders itself between calls and the report
# would then name the wrong database for a client's numbers. Fewer names than clients is refused;
# unset means every client runs on the default tenant, exactly as before.
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
# `d562f6a`, built from that commit's tree on `rtdb-gateway:5fa0069` — production's own gateway image —
# and pushed 2026-09-11. The previous default `ac74d22` (Sept 2) predates `--ns` and `--leaves`
# entirely, so a §5.30 run against it would silently land every client on the default tenant.
TAG=${LOADSIM_TAG:-d562f6a}
NS_LIST=${LOADSIM_NS_LIST:-}

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
  --query 'Reservations[].Instances[].InstanceId' --output text | tr '\t' '\n' | grep -v '^$' | sort || true)

if [ -z "$CLIENTS" ]; then
  echo "no running load clients. terraform apply -var load_clients=N first." >&2
  exit 1
fi
N_CLIENTS=$(echo "$CLIENTS" | wc -l | tr -d ' ')
echo "dispatching to ${N_CLIENTS} client(s), ${CONNS} conns each at ${RATE} w/s for ${SECS}s"

if [ -n "$NS_LIST" ]; then
  N_NS=$(echo "$NS_LIST" | tr ',' '\n' | grep -c .)
  if [ "$N_NS" -lt "$N_CLIENTS" ]; then
    echo "LOADSIM_NS_LIST has $N_NS name(s) for $N_CLIENTS client(s)" >&2
    exit 2
  fi
fi

# The \$( ) below is BACKSLASH-ESCAPED, so this shell leaves it alone and the INSTANCE evaluates it
# at run time using its own role. Everything unescaped here (region, registry, counts) is expanded
# now and stored by AWS; everything escaped runs there. The secret is only ever on the far side.
#
# A function rather than one string, because `--ns` differs per client: each dispatch rebuilds it.
build_script() {
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
  --gateways ${ENDPOINT} --conns ${CONNS} --procs 4 --rate ${RATE} --seconds ${SECS} ${NSFLAG} ${EXTRA} 2>&1 | tail -22
rc=\$?
echo "[loadsim exit \$rc]"
exit \$rc
EOF
)
}

# The command travels as JSON via a file: shorthand --parameters cannot survive the quoting, and
# a heredoc through argv would mangle it.
IDS=""
i=0
for c in $CLIENTS; do
  NSFLAG=""
  if [ -n "$NS_LIST" ]; then
    NS=$(echo "$NS_LIST" | cut -d, -f$((i + 1)))
    NSFLAG="--ns $NS"
    echo "  client $i  $c  -> --ns $NS"
  else
    echo "  client $i  $c  -> default tenant"
  fi
  i=$((i + 1))
  build_script
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
  # A heartbeat, because the silence here is what got a run killed. Under saturation a client keeps
  # collecting for MINUTES after its writes were sent (C2' sent for 180 s and reported ~12 minutes
  # later); with nothing on the terminal the step looks finished, and C2''s four tails were lost to
  # a Ctrl+C that way — recoverable only from SSM history, and only because the mentor went looking.
  waited=0
  for _ in $(seq 1 400); do
    sleep 5; waited=$((waited + 5))
    st=$(aws ssm get-command-invocation --command-id "$cid" --instance-id "$inst" \
         --query Status --output text 2>/dev/null || echo Pending)
    case "$st" in
      InProgress|Pending|Delayed)
        [ $((waited % 60)) -eq 0 ] && echo "  ... $inst still $st after ${waited}s (do not interrupt)" ;;
      *) break ;;
    esac
  done
  echo "----- $inst ($st) -----"
  aws ssm get-command-invocation --command-id "$cid" --instance-id "$inst" \
    --query StandardOutputContent --output text
done
