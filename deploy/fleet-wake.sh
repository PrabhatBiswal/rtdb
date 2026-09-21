#!/bin/bash
# Wake the fleet: START RDS and wait for it, then START the three EC2 instances; systemd + compose
# (`restart: unless-stopped`) bring the containers back on their own. Then wait for the console.
#   AWS_PROFILE=rtdb-deploy deploy/fleet-wake.sh
#
# THE INSTANCES ARE STARTED ONE AT A TIME, and that is not tidiness. A single
# `start-instances --instance-ids A B C` is one call: `InsufficientInstanceCapacity` for ONE of them
# fails the whole call, nothing starts, and the error does not say which instance or which AZ was
# short. That happened on 2026-09-16 — ONE zone had no `t4g.medium` while the gateway in another
# zone and the `t4g.small` ops box would both have started fine. Per-instance, the shortage costs
# you the one instance it actually applies to.
#
# AND A MISSING GATEWAY IS NOT A BLOCKER: §8 sizes EACH gateway for 100% of the load, so one healthy
# gateway is a serving fleet. What you lose is redundancy, not availability — which is worth knowing
# before you start firefighting a capacity blip that AWS will clear on its own.
# `-e` stays: only the start attempts are allowed to fail, and they say so with `if`, which `-e`
# exempts. Every array expansion below sits behind a `${#a[@]}` guard because macOS ships bash 3.2,
# where `"${a[@]}"` on an EMPTY array under `set -u` is an unbound-variable error rather than nothing.
set -euo pipefail
export AWS_REGION=${AWS_REGION:-ap-south-1}
# This deployment's names live in a gitignored `deploy/site.env`, never as a default in the code:
# a public repo that ships one operator's domain hands the next operator a wrong answer that LOOKS
# right (same rule as `deploy/*.tfvars`, and as commit 862acb1 applied to image tags).
SITE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/site.env"
# shellcheck source=/dev/null
# An `if`, not `[ -f … ] && .`: under `-e` a false test here aborts the script, and "no site file"
# is an ordinary state, not a failure. (Same shape that bit this file once already.)
if [ -f "$SITE" ]; then . "$SITE"; fi
# Which boxes. In `site.env` for the same reason as the names: these are THIS account's instances,
# and a public repo that ships them gives the next operator a script that targets nothing.
: "${GW1:?set GW1 in deploy/site.env — see site.env.example}"
: "${GW2:?set GW2 in deploy/site.env}"
: "${OPS:?set OPS in deploy/site.env}"
: "${RDS:?set RDS in deploy/site.env}"
# Tries per instance before moving on. Deliberately small: a capacity stock-out lasts minutes to
# HOURS, so a wake script that keeps trying is a script that never returns. Anything still down gets
# its own one-liner printed at the end, to leave running in another tab.
TRIES=${TRIES:-3}
GAP=${GAP:-20}

st=$(aws rds describe-db-instances --db-instance-identifier $RDS --query 'DBInstances[0].DBInstanceStatus' --output text)
if [ "$st" = stopped ]; then
  echo "== starting RDS $RDS (5–10 min)"; aws rds start-db-instance --db-instance-identifier $RDS >/dev/null
fi
aws rds wait db-instance-available --db-instance-identifier $RDS && echo "   RDS available"

echo "== starting gateways + ops, one at a time"
started=(); failed=()
for pair in "$GW1:gw-1" "$GW2:gw-2" "$OPS:ops"; do
  id=${pair%%:*}; name=${pair##*:}
  ok=0
  for try in $(seq 1 "$TRIES"); do
    # Idempotent: already-running is a success, so a retry after a partial wake is safe.
    if err=$(aws ec2 start-instances --instance-ids "$id" 2>&1 >/dev/null); then ok=1; break; fi
    reason=$(printf '%s' "$err" | grep -o 'InsufficientInstanceCapacity' || echo "see error")
    echo "   $name ($id): $reason (try $try/$TRIES)"
    [ "$try" -lt "$TRIES" ] && sleep "$GAP"
  done
  if [ "$ok" = 1 ]; then echo "   $name started"; started+=("$id"); else failed+=("$id:$name"); fi
done

if [ ${#started[@]} -gt 0 ]; then
  aws ec2 wait instance-running --instance-ids "${started[@]}" && echo "   ${#started[@]} running"
else
  echo "!! nothing started. RDS is UP and billing with nothing serving — either retry, or stop it again"
  echo "   with deploy/fleet-sleep.sh."
  exit 1
fi

if [ -n "${CONSOLE_URL:-}" ]; then
  echo "== waiting for the console (CloudFront -> ops box)"
  for _ in $(seq 1 40); do sleep 15; code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$CONSOLE_URL/healthz" || true); [ "$code" = 200 ] && { echo "   console healthz 200"; break; }; printf '.'; done
else
  # Named, not guessed: a skipped check that says nothing reads as a check that passed.
  echo "== console check SKIPPED: CONSOLE_URL unset (see deploy/site.env.example)"
fi

# Only the gateways that actually came up: asking SSM about a stopped instance does not fail loudly,
# it simply never answers, and a command that never answers reads like a broken gateway.
gws=()
for id in "${started[@]}"; do
  # An `if`, not `[ … ] && gws+=(…)`: under `-e` a false test in a loop body aborts the script, and
  # "no gateway matched" is an ordinary outcome here, not a failure.
  if [ "$id" = "$GW1" ] || [ "$id" = "$GW2" ]; then gws+=("$id"); fi
done
if [ ${#gws[@]} -gt 0 ]; then
  echo "== gateways: SSM check of the boot line + healthz"
  ID=$(aws ssm send-command --instance-ids "${gws[@]}" --document-name AWS-RunShellScript --parameters 'commands=["docker ps --format {{.Image}}; docker logs $(docker ps -q --filter name=rtdb-gateway) 2>&1 | grep -m1 multi-tenant || true; curl -fsS -m 3 http://127.0.0.1:9090/healthz || echo NOT-HEALTHY-YET"]' --query Command.CommandId --output text); sleep 45
  for i in "${gws[@]}"; do echo "-- $i"; aws ssm get-command-invocation --command-id "$ID" --instance-id "$i" --query StandardOutputContent --output text; done
fi

if [ ${#failed[@]} -gt 0 ]; then
  echo
  echo "== DID NOT START (${#failed[@]}):"
  for f in "${failed[@]}"; do echo "   ${f##*:}  ${f%%:*}"; done
  echo "   A gateway missing costs REDUNDANCY, not availability — §8 sizes each for 100% of the load."
  echo "   Capacity is an AWS-side stock-out, not a limit on this account, and it clears on its own."
  echo "   Leave this in another tab; it prints the moment one is accepted:"
  for f in "${failed[@]}"; do
    echo "     while :; do aws ec2 start-instances --instance-ids ${f%%:*} >/dev/null 2>&1 && { echo '${f##*:} up'; break; }; sleep 600; done"
  done
fi
echo "== done. If a gateway says NOT-HEALTHY-YET, give it a minute (RDS/Redis connect retry) and re-run this check."
