#!/bin/bash
#
# §5.30 Gate 0(c): the `--ns` rig proved against a REAL multi-tenant gateway, locally.
#
#   scripts/loadsim-ns-proof.sh            # ~4 minutes, needs a local Postgres
#
# Two declared databases on one gateway, which is production's shape in miniature:
#   a = quota override 100000, declared through the admin port exactly as the console's `+` does
#   b = no override, so the shard default — set to 64 here, which is production's number
#
# THE TOOTH is the last comparison: loadsim's own `write errors` against the gateway's
# `rtdb_quota_rejected_total{db="b"}` delta. They come from two sides of one refusal, and if they
# disagree one of them is lying — either loadsim is swallowing an error class that is not RATE, or
# the meter is missing refusals the client saw. Equal is the only passing answer.
set -uo pipefail
cd "$(dirname "$0")/.."
OUT=${OUT:-$(mktemp -d)}; mkdir -p "$OUT"
PG=${RTDB_PG_URL:-postgres://localhost:5432/postgres}
WS=ws://127.0.0.1:8390
ADMIN=http://127.0.0.1:9390
export RTDB_DEV_SECRET=proof-secret
echo "output in $OUT"

# Its own control schema and its own default tenant, so this never touches a `test:pg` shard.
psql "$PG" -qc 'DROP SCHEMA IF EXISTS rtdb_control_proof CASCADE; DROP SCHEMA IF EXISTS proofdef CASCADE; DROP SCHEMA IF EXISTS a CASCADE; DROP SCHEMA IF EXISTS b CASCADE;' >/dev/null 2>&1

# WRITE_RATE_PER_SEC is lifted because the PER-CONNECTION limiter is not what this measures: it
# would refuse writes with the same `RATE` code and the tooth could not tell the two apart.
RTDB_STORAGE=postgres RTDB_MULTI_TENANT=1 RTDB_PG_URL="$PG" \
RTDB_PG_SCHEMA=proofdef RTDB_CONTROL_SCHEMA=rtdb_control_proof \
RTDB_PORT=8390 RTDB_ADMIN_PORT=9390 RTDB_RULES=rules/allow-all.ts \
RTDB_QUOTA_ACQ_PER_SEC=64 RTDB_LIMITS='{"WRITE_RATE_PER_SEC":1000000,"WRITE_RATE_BURST":1000000}' \
  node --import tsx src/gateway/main.ts > "$OUT/gw.log" 2>&1 &
GW=$!
trap 'kill $GW 2>/dev/null' EXIT
for _ in $(seq 1 60); do grep -q 'rtdb admin' "$OUT/gw.log" && break; sleep 1; done
grep -q 'rtdb admin' "$OUT/gw.log" || { echo "gateway did not start"; cat "$OUT/gw.log"; exit 1; }
echo "== gateway: $(grep rtdb "$OUT/gw.log" | tr '\n' ' ')"

curl -sX POST "$ADMIN/databases" -H 'content-type: application/json' -d '{"name":"a","quotaAcqPerSec":100000}'; echo
curl -sX POST "$ADMIN/databases" -H 'content-type: application/json' -d '{"name":"b"}'; echo
echo "== /topnodes BEFORE: $(curl -s $ADMIN/topnodes)"

tok() { node --import tsx -e "import {signDevToken} from './src/gateway/auth.ts';console.log(signDevToken({sub:'$1',ns:'$2',exp:Math.floor(Date.now()/1000)+3600}))"; }
TOKB=$(tok u_rider b)
rider() { env RTDB_URL=$WS RTDB_RIDE_PATH=b/drill/ride RTDB_TOKEN="$TOKB" \
  node --import tsx scripts/ride-client.ts "$1" 2>&1 | grep RESULT; }

echo "== rider on b, BEFORE (20s)";  rider 20 | tee "$OUT/rider-before.json"
echo "== loadsim --ns a (200 conns / 300 w/s / 60s), rider on b DURING"
(rider 60 > "$OUT/rider-during.json") & RID=$!
node --import tsx scripts/loadsim.ts --gateways $WS --ns a --conns 200 --procs 4 --rate 300 \
  --seconds 60 --pg "$PG" --schema a 2>&1 | tee "$OUT/run-a.txt"
wait $RID; cat "$OUT/rider-during.json"
echo "== rider on b, AFTER (20s)";   rider 20 | tee "$OUT/rider-after.json"

rej() { curl -s "$ADMIN/metrics" | awk -v d="$1" -F' ' '$0 ~ "^rtdb_quota_rejected_total\\{db=\""d"\"\\}" {print $2}'; }
Q0=$(rej b)
echo "== loadsim --ns b (200 conns / 300 w/s / 60s) — default quota 64/s"
node --import tsx scripts/loadsim.ts --gateways $WS --ns b --conns 200 --procs 4 --rate 300 \
  --seconds 60 --pg "$PG" --schema b 2>&1 | tee "$OUT/run-b.txt"
echo "TOOTH  loadsim write errors = $(awk '/write errors/{print $3}' "$OUT/run-b.txt")   rtdb_quota_rejected_total{db=\"b\"} delta = $(python3 -c "print(int(float('$(rej b)'))-int(float('$Q0')))")"
echo "       a refused nothing:     $(rej a)"
curl -s "$ADMIN/metrics" | grep -E '^rtdb_quota_acq_per_sec'

# The amendment's shape: four clients, four databases, one endpoint. Two is enough to show that the
# tenants do not collide; `--leaves 32` is what makes each connection's subtree GROW, and
# `--cleanup 0` is what a ladder step does so the next step inherits the tree.
echo "== a and b at once, two loadsim processes, --leaves 32 (30s)"
node --import tsx scripts/loadsim.ts --gateways $WS --ns a --conns 100 --procs 2 --rate 150 \
  --seconds 30 --leaves 32 --cleanup 0 --pg "$PG" --schema a > "$OUT/par-a.txt" 2>&1 & P1=$!
node --import tsx scripts/loadsim.ts --gateways $WS --ns b --conns 100 --procs 2 --rate 150 \
  --seconds 30 --leaves 32 --cleanup 0 --pg "$PG" --schema b > "$OUT/par-b.txt" 2>&1 & P2=$!
wait $P1; wait $P2
tail -13 "$OUT/par-a.txt"; echo '---'; tail -13 "$OUT/par-b.txt"

echo "== the tree those writes left in a"
psql "$PG" -tAc "set search_path=a; select count(*) as nodes from nodes; select count(distinct path) from nodes where path like 'a/sim/own/%'; select path from nodes where path like 'a/sim/own/%' limit 2;"
node --import tsx -e "
import {RtdbClient} from './harness/client.ts';import {signDevToken} from './src/gateway/auth.ts';
for (const ns of ['a','b']) {
  const c=new RtdbClient({url:'$WS',token:signDevToken({sub:'u_clean',ns,exp:Math.floor(Date.now()/1000)+3600})});
  c.connect(); await c.ready(); await c.put(ns+'/sim',null); c.close();
}"
echo "== /topnodes AFTER:  $(curl -s $ADMIN/topnodes)"
psql "$PG" -tAc "set search_path=a; select count(*) as nodes_left from nodes;"
