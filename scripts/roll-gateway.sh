#!/bin/bash
#
# Roll ONE gateway to an image tag with a WRITING CLIENT across the restart — under ONE seal.
#
#   SK_FILE=/path/to/shadow-key RTDB_URL=wss://… RTDB_SHADOW_TOKEN_URL=https://…/shadow-token \
#     scripts/roll-gateway.sh <sha> <gw-1|gw-2>
#
# §5.26 rolled gw-2 with no rider because the rider and the deploy were two separate user-sealed
# commands: the roll was approved, the rider was not started, and the span number for that gateway
# is gone for good. A span measurement needs ONE seal — so the rider, the deploy and the wait live
# in this file and the user approves them together.
#
# Also the rollback path: `roll-gateway.sh <older-sha> gw-1` is RUNBOOK §4, with a rider on it.
#
# Runs on the laptop. Reads SK_FILE; never writes it, never prints it.
set -euo pipefail

SHA=${1:?usage: roll-gateway.sh <sha> <gw-1|gw-2>}
GW=${2:?usage: roll-gateway.sh <sha> <gw-1|gw-2>}

# The instance map lives here rather than in a second file: two names, and a roll aimed at the wrong
# box is the failure this script exists to make boring (§5.26's mentor arm hit the wrong tag).
case $GW in
  gw-1) INST=i-027553b309c34c228 ;;
  gw-2) INST=i-02ae35a597d9319e9 ;;
  *) echo "unknown gateway: $GW (gw-1|gw-2)" >&2; exit 2 ;;
esac

: "${SK_FILE:?SK_FILE must point at the shadow key (0600, read-only to this script)}"
[ -r "$SK_FILE" ] || { echo "SK_FILE not readable: $SK_FILE" >&2; exit 2; }
# §5.7: the key travels as a file, never as an argument, and a key a shell history could hold is
# not a key. 600 or 400 only.
PERM=$(stat -f '%Lp' "$SK_FILE" 2>/dev/null || stat -c '%a' "$SK_FILE")
case $PERM in 600|400) ;; *) echo "SK_FILE must be 0600 (is $PERM)" >&2; exit 2 ;; esac

cd "$(dirname "$0")/.."
RIDE_LOG=$(mktemp -t roll-rider)
RIDER=

# A rider left writing to production after this script dies is worse than no rider.
cleanup() { [ -n "$RIDER" ] && kill "$RIDER" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

ssm() { # ssm <shell command…> -> command id
  aws ssm send-command --instance-ids "$INST" --document-name AWS-RunShellScript \
    --parameters "commands=[\"$1\"]" --query Command.CommandId --output text
}

# Both come from the caller: this script is not tied to one deployment.
: "${RTDB_URL:?set RTDB_URL to the gateway the rider writes to, e.g. wss://rtdb.example.com}"
: "${RTDB_SHADOW_TOKEN_URL:?set RTDB_SHADOW_TOKEN_URL to the console /shadow-token endpoint}"

echo "== rider: 300 s on $RTDB_URL, log $RIDE_LOG"
SK_FILE=$SK_FILE \
RTDB_URL=$RTDB_URL \
RTDB_SHADOW_TOKEN_URL=$RTDB_SHADOW_TOKEN_URL \
  node --import tsx scripts/ride-client.ts 300 >"$RIDE_LOG" 2>&1 &
RIDER=$!

# 30 s of clean writes BEFORE the restart, so the RESULT has a before as well as an across.
sleep 30
kill -0 "$RIDER" 2>/dev/null || { echo "rider died before the roll:" >&2; cat "$RIDE_LOG" >&2; exit 1; }

echo "== deploy: $SHA on $GW ($INST)"
CMD=$(ssm "/opt/rtdb/rtdb-deploy.sh $SHA")
echo "   command id $CMD"

# rtdb-deploy.sh polls /healthz for 60 s itself and exits non-zero with the container logs, so the
# only thing to wait on is the invocation reaching a terminal state.
for _ in $(seq 1 40); do
  sleep 10
  ST=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$INST" \
        --query Status --output text)
  case $ST in Pending|InProgress|Delayed) ;; *) break ;; esac
done
OUT=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$INST" \
       --query '{S:Status,O:StandardOutputContent,E:StandardErrorContent}' --output json)
echo "$OUT"

# The witness is the sentence, not the exit status: `Success` with no `deploy ok` means the script
# changed under us.
if [ "$ST" != Success ] || ! grep -q "deploy ok: $SHA healthy" <<<"$OUT"; then
  echo "DEPLOY FAILED on $GW ($ST) — killing the rider, nothing rolled forward" >&2
  exit 1
fi

echo "== rider: waiting out the remaining ~4 min"
wait "$RIDER" || { echo "rider exited non-zero:" >&2; cat "$RIDE_LOG" >&2; exit 1; }
RIDER=
echo "== RESULT"
grep '"RESULT"' "$RIDE_LOG" || { echo "no RESULT line — rider log:" >&2; cat "$RIDE_LOG" >&2; exit 1; }

# Both of these stay in SSM history, which is where the mentor reads what actually ran. `|| true`
# because grep's exit 1 on no match would kill the whole invocation and lose the healthz line with
# it — an absent boot line is a finding to read, not a script failure.
echo "== boot line + admin healthz (SSM history)"
C2=$(ssm "docker logs \$(docker ps -q --filter name=rtdb-gateway) 2>&1 | grep -m1 multi-tenant || true; curl -fsS -m 3 http://127.0.0.1:9090/healthz || true")
sleep 8
aws ssm get-command-invocation --command-id "$C2" --instance-id "$INST" \
  --query '{S:Status,O:StandardOutputContent,E:StandardErrorContent}' --output json
