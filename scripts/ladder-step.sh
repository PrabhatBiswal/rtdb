#!/bin/bash
#
# §5.30 Gate 2: ONE ladder step, under ONE seal.
#
#   AWS_PROFILE=rtdb-deploy SK_FILE=$HOME/.rtdb-sk scripts/ladder-step.sh <step> <conns/db> <w/s per db> <hot> [secs]
#   e.g.  scripts/ladder-step.sh 1 250 50 0.1
#
# WHY ONE SCRIPT AND NOT THREE COMMANDS. The rider is the only witness to what the four load
# clients did to the DEFAULT tenant, and it has to be writing before the step starts and still
# writing after it ends. §5.26 split a rider from the roll it was measuring across two seals and
# gw-2's write-across-the-restart number does not exist and cannot be recovered (WORKLOAD §5.26).
# Same shape here: rider, dispatch, Prometheus window, all inside one approval.
#
# It reads Prometheus THROUGH the ops box (VPC-internal, never public) and dispatches loadsim
# through `deploy/run-loadsim.sh`, which is where the jwt-secret-never-in-SSM-history rule lives.
set -uo pipefail
cd "$(dirname "$0")/.."

STEP=${1:?step number}
CONNS=${2:?conns per database}
RATE=${3:?writes/sec per database}
HOT=${4:-0.1}
SECS=${5:-180}

export AWS_REGION=${AWS_REGION:-ap-south-1}
# This deployment's names and boxes come from a gitignored `deploy/site.env`, never from a default
# here: a committed domain is a wrong answer that LOOKS right to the next operator, and a committed
# instance id points at an account that is not theirs.
SITE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../deploy" && pwd)/site.env"
# shellcheck source=/dev/null
if [ -f "$SITE" ]; then . "$SITE"; fi
OPS=${OPS_INSTANCE:-${OPS:?set OPS in deploy/site.env — see site.env.example}}
export RTDB_ENDPOINT=${RTDB_ENDPOINT:?set RTDB_ENDPOINT in deploy/site.env}
export CONSOLE_URL=${CONSOLE_URL:?set CONSOLE_URL in deploy/site.env}
# The REGISTRY's spellings of the databases, not the sidebar's read of them — two of this
# deployment's were console typos, and step 1 died at hello with 4401 on the clients that got the
# "corrected" names, twice; the pre-flight leader list below is what shows the truth. The list is
# this deployment's, so it lives in deploy/site.env (LOADSIM_NS_LIST), never here
# (2026-09-11, WORKLOAD §5.30; scrubbed 2026-09-22, §5.37).
#
# `-` not `:-`, and exported: a stray `export` before this line meant the default never reached
# run-loadsim.sh (child process, unexported var), so a run that trusted it landed every client on
# `public` silently; and `:-` would turn `LOADSIM_NS_LIST=` — the Q3 "no --ns, land on public"
# spelling — back into the four names, the same wrong run in the other direction (§5.34 prep).
export LOADSIM_NS_LIST=${LOADSIM_NS_LIST?set LOADSIM_NS_LIST: a comma list of registry names, or empty for the default tenant (deploy/site.env)}
OUT=${OUT:-$(mktemp -d)}
echo "== §5.30 ladder step $STEP — ${CONNS} conns/db, ${RATE} w/s/db, hot ${HOT}, ${SECS}s, output in $OUT"

# Prometheus, through the ops box. A caller names a QUERY here and nothing else — no host, no port.
# An optional second arg is a unix-seconds EVALUATION TIME: an instant query with no `time=` anchor
# evaluates at NOW, and the post-window below runs only after the rider wait (RIDER_SECS = SECS*5+300,
# ~20 min) — so unanchored, every max_over_time[WINDOW] read an idle window ~13 min after the step
# and printed garbage. Q3 2026-09-21 shipped exactly that; the numbers were recovered by hand.
prom() {
  local q="$1" at="${2:-}"
  # Through a JSON file, exactly as `run-loadsim.sh` does and for the same reason: a PromQL
  # selector carries `{`, `}` and `"` and the shorthand --parameters form cannot survive them.
  local cid j
  j=$(mktemp)
  Q="$q" AT="$at" OUT_J="$j" INST="$OPS" python3 -c '
import json, os, shlex
cmd = "curl -sG http://127.0.0.1:9090/api/v1/query --data-urlencode " + shlex.quote("query=" + os.environ["Q"])
if os.environ.get("AT"): cmd += " --data-urlencode " + shlex.quote("time=" + os.environ["AT"])
json.dump({"InstanceIds": [os.environ["INST"]], "DocumentName": "AWS-RunShellScript",
           "Parameters": {"commands": [cmd], "executionTimeout": ["120"]}},
          open(os.environ["OUT_J"], "w"))'
  cid=$(aws ssm send-command --cli-input-json "file://$j" --query Command.CommandId --output text) || return 1
  rm -f "$j"
  for _ in $(seq 1 30); do
    sleep 2
    local st
    st=$(aws ssm get-command-invocation --command-id "$cid" --instance-id "$OPS" \
         --query Status --output text 2>/dev/null || echo Pending)
    case "$st" in InProgress|Pending|Delayed) ;; *) break ;; esac
  done
  aws ssm get-command-invocation --command-id "$cid" --instance-id "$OPS" \
    --query StandardOutputContent --output text \
  | python3 -c '
import json,sys
raw=sys.stdin.read()
try: d=json.loads(raw)
except Exception: print("    (no answer)", raw[:200]); raise SystemExit
res=d.get("data",{}).get("result",[])
for r in res:
    m={k:v for k,v in r["metric"].items() if k not in ("__name__","job")}
    print("   ", m if m else "{}", " ", r["value"][1])
if not res: print("    (empty)")'
}

echo "== PRE-FLIGHT (read-only)"
echo "  targets up:";        prom 'sum(up{job="rtdb-gateway"})'
echo "  leader per db:";     prom 'sum by (db) (rtdb_leader)'
echo "  consumer lag:";      prom 'max by (db) (rtdb_consumer_lag_revs)'
echo "  pool_waiting now:";  prom 'max(rtdb_pg_pool_waiting)'

# The rider has to outlast the DISPATCH, and the dispatch is not SECS long. Under saturation the
# acks arrive minutes after the writes were sent — C2' sent for 180 s and its clients were still
# reporting ~12 minutes later — so a fixed SECS+240 ends while run-loadsim is still collecting, the
# terminal goes quiet after the rider's RESULT, and the run looks finished when it is not. That is
# how C2''s four client tails ended up only in SSM history and not in the local transcript.
RIDER_SECS=${RIDER_SECS:-$((SECS * 5 + 300))}
echo "== rider on \`public\` for ${RIDER_SECS}s (starts BEFORE the step, ends AFTER it)"
SK_FILE=${SK_FILE:?set SK_FILE to the 0600 shadow key} \
RTDB_SHADOW_TOKEN_URL=${RTDB_SHADOW_TOKEN_URL:-$CONSOLE_URL/shadow-token} \
RTDB_URL=$RTDB_ENDPOINT \
  node --import tsx scripts/ride-client.ts "$RIDER_SECS" > "$OUT/rider.log" 2>&1 &
RIDER=$!
trap 'kill $RIDER 2>/dev/null' EXIT
sleep 20   # so the RESULT's p50 carries some quiet time on both sides of the step

T0=$(date +%s)
echo "== dispatching loadsim to four clients"
deploy/run-loadsim.sh "$CONNS" "$RATE" "$SECS" --hot "$HOT" --leaves 32 --cleanup 0 2>&1 | tee "$OUT/loadsim.txt"
T1=$(date +%s)
WINDOW=$((T1 - T0 + 30))
# The post-window is ANCHORED at T1+30: it must describe the step, not the quiet ~13 min later
# when the rider wait finally lets these lines run.
AT=$((T1 + 30))

echo "== dispatch returned after $((T1 - T0))s; waiting the rider out (it was given ${RIDER_SECS}s)"
if [ $((T1 - T0 + 20)) -ge "$RIDER_SECS" ]; then
  echo "  !! THE DISPATCH OUTLASTED THE RIDER — its RESULT below stops before the step did."
  echo "  !! Re-run with RIDER_SECS larger; this step's rider tail is missing, not zero."
fi
wait $RIDER; trap - EXIT
grep -E '"ev":"(RESULT|write.err|client.close)"' "$OUT/rider.log" | tail -20

echo "== max_over_time[${WINDOW}s] — the five numbers"
echo "  1 pool_waiting:";       prom "max_over_time(rtdb_pg_pool_waiting[${WINDOW}s])" "$AT"
echo "  2 connections_pending:";prom "max_over_time(rtdb_connections_pending[${WINDOW}s])" "$AT"
echo "  3 ack p99 (s):";        prom "histogram_quantile(0.99, sum by (le) (rate(rtdb_ack_seconds_bucket[${WINDOW}s])))" "$AT"
echo "  4 quota_rejected /db:"; prom "increase(rtdb_quota_rejected_total[${WINDOW}s]) > 0" "$AT"
# prom-client's default metrics carry their OWN `process_` in the name under our `rtdb_proc_`
# prefix (metrics.ts:331), so this is rtdb_proc_PROCESS_resident_memory_bytes. Written without it
# the query is not wrong, it is EMPTY — which is how C1 lost its RSS number entirely.
echo "  5 RSS % of 4 GiB:";     prom "max by (instance) (rtdb_proc_process_resident_memory_bytes) / (4*1024*1024*1024) * 100" "$AT"
# acq/s per db is the number the quota actually bends on: reading it beside quota_rejected says
# WHERE on its own limit each database sat, which `rejected = 0` alone never says.
echo "  6 acq/s per db:";       prom "sum by (db) (rate(rtdb_lock_acquisitions_total[${WINDOW}s]))" "$AT"
# C1's wall was delivery, not writes, and this is the delivery counter: frames leaving a sink.
echo "  7 deliveries/s per gw:";prom "sum by (instance) (rate(rtdb_fanout_seconds_count[${WINDOW}s]))" "$AT"
echo "  8 CPU cores per gw:";   prom "sum by (instance) (rate(rtdb_proc_process_cpu_seconds_total[${WINDOW}s]))" "$AT"
echo "  9 consumer lag max:";   prom "max_over_time(rtdb_consumer_lag_revs[${WINDOW}s])" "$AT"
echo " 10 event-loop lag max:"; prom "max_over_time(rtdb_proc_nodejs_eventloop_lag_seconds[${WINDOW}s])" "$AT"
echo " 11 up, min over window:";prom "min_over_time(sum(up{job=\"rtdb-gateway\"})[${WINDOW}s:15s])" "$AT"
echo "  connections per db:";   prom "sum by (db) (rtdb_connections)" "$AT"
echo "  storage bytes per db:"; prom "max by (db) (rtdb_storage_bytes)" "$AT"
echo "== step $STEP done. Output kept in $OUT"
