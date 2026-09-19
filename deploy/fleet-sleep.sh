#!/bin/bash
# Put the fleet to sleep to stop the meter: STOP the three EC2 instances, then STOP RDS.
# ElastiCache and the NLB cannot be stopped (only destroyed) and are left alone (~$1.55/day).
# RDS auto-starts after 7 days (AWS rule) — wake it on purpose before then with fleet-wake.sh.
#   AWS_PROFILE=rtdb-deploy deploy/fleet-sleep.sh
set -euo pipefail
# This deployment's names live in a gitignored `deploy/site.env`, never as a default in the code:
# a public repo that ships one operator's domain hands the next operator a wrong answer that LOOKS
# right (same rule as `deploy/*.tfvars`, and as commit 862acb1 applied to image tags).
SITE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/site.env"
# shellcheck source=/dev/null
# An `if`, not `[ -f … ] && .`: under `-e` a false test here aborts the script, and "no site file"
# is an ordinary state, not a failure. (Same shape that bit this file once already.)
if [ -f "$SITE" ]; then . "$SITE"; fi
export AWS_REGION=${AWS_REGION:-ap-south-1}
: "${GW1:?set GW1 in deploy/site.env — see site.env.example}"
: "${GW2:?set GW2 in deploy/site.env}"
: "${OPS:?set OPS in deploy/site.env}"
: "${RDS:?set RDS in deploy/site.env}"
echo "== stopping gateways + ops"
aws ec2 stop-instances --instance-ids $GW1 $GW2 $OPS --query 'StoppingInstances[].[InstanceId,CurrentState.Name]' --output text
aws ec2 wait instance-stopped --instance-ids $GW1 $GW2 $OPS && echo "   all three stopped"
echo "== stopping RDS $RDS (takes a few minutes; auto-starts after 7 days)"
aws rds stop-db-instance --db-instance-identifier $RDS --query 'DBInstance.DBInstanceStatus' --output text
echo "== done. Fleet asleep: ${RTDB_ENDPOINT:-the wire endpoint} and ${CONSOLE_URL:-the console} are DOWN until fleet-wake.sh."
