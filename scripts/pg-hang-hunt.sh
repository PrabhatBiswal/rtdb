#!/bin/bash
# Hunt the intermittent suite hang with a Postgres wait-event sampler attached (WP8 Gate C §6).
#
#   scripts/pg-hang-hunt.sh [max-runs] [pg|writes]
#
# EVIDENCE ONLY. Nothing here fixes anything, changes `src/`, or touches the database's schema or
# configuration. It runs the suite in a loop with one long-lived psql session sampling
# pg_stat_activity, and the moment a run exits non-zero it stops and keeps everything.
#
# Default target is the FULL test:pg rather than the single file that hung: node:test runs files
# concurrently, so the contention may well come from that parallelism, and reproducing it alone
# would be reproducing a different thing.
#
# A bounded null result is a result. If max-runs pass clean, that is reported as a non-occurrence
# with the count, not as "no bug".
set -uo pipefail
MAX=${1:-40}
TARGET=${2:-pg}
PG_URL=${RTDB_PG_URL:-postgres://localhost:5432/postgres}
OUT="hang-hunt-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
OTHER=0

case "$TARGET" in
  pg)     CMD=(npm run test:pg) ;;
  writes) CMD=(npx tsx --test --test-timeout=30000 test/integration/writes.test.ts) ;;
  *) echo "target must be pg or writes"; exit 2 ;;
esac

echo "hunting: ${CMD[*]}   up to $MAX runs   -> $OUT/"
for n in $(seq 1 "$MAX"); do
  WAITS="$OUT/waits-$n.log"
  psql "$PG_URL" -A -F'|' -t -q -f scripts/pg-wait-sampler.sql > "$WAITS" 2>&1 &
  SAMPLER=$!
  START=$(date +%s)
  RTDB_STORAGE=postgres "${CMD[@]}" > "$OUT/run-$n.tap" 2>&1
  RC=$?
  kill "$SAMPLER" 2>/dev/null; wait "$SAMPLER" 2>/dev/null
  DUR=$(( $(date +%s) - START ))

  # Classify. The target is a testTimeoutFailure — the file-level stall. Other failures are real
  # and get counted, but they must NOT end the hunt: on the first attempt an unrelated
  # 'socket error' flake stopped it at run 17 before the thing being hunted had a chance to appear.
  # A stop condition wider than the question ends the experiment early, every time.
  if [ "$RC" -ne 0 ] && ! grep -q "testTimeoutFailure" "$OUT/run-$n.tap"; then
    OTHER=$(( OTHER + 1 ))
    WHAT=$(grep -oE "^not ok [0-9]+ - .*" "$OUT/run-$n.tap" | head -1 | cut -c1-90)
    printf "run %2d/%d  OTHER FAILURE (kept, hunt continues)  %3ds  %s\n" "$n" "$MAX" "$DUR" "$WHAT"
    mv "$WAITS" "$OUT/waits-other-$n.log"
    mv "$OUT/run-$n.tap" "$OUT/run-other-$n.tap"
    continue
  fi

  if [ "$RC" -eq 0 ]; then
    # Clean run: keep the TAP summary only. Wait logs from clean runs are gigabytes of nothing.
    rm -f "$WAITS"
    printf "run %2d/%d  ok   %3ds  %s\n" "$n" "$MAX" "$DUR" \
      "$(grep -E '^# (pass|fail)' "$OUT/run-$n.tap" | tr '\n' ' ')"
    rm -f "$OUT/run-$n.tap"
    continue
  fi

  echo
  echo "=== RUN $n TIMED OUT AFTER ${DUR}s — the target. Everything kept in $OUT/ ==="
  grep -E "^not ok [0-9]+ - test/|failureType|error: 'test timed out" "$OUT/run-$n.tap" | head -6
  echo
  echo "--- last tests to report before the stall ---"
  grep -E "^ok [0-9]+ - " "$OUT/run-$n.tap" | tail -5
  echo
  echo "--- sampled rows: $(wc -l < "$WAITS" | tr -d ' ') ---"
  echo "--- ANY BLOCKED BACKEND (column 6 non-empty is the smoking gun) ---"
  awk -F'|' '$6 != "" {print}' "$WAITS" | head -20 || true
  echo
  echo "--- wait events seen, by frequency ---"
  awk -F'|' '{print $4"/"$5}' "$WAITS" | sort | uniq -c | sort -rn | head -15
  echo
  echo "--- the last 25 samples before the sampler stopped ---"
  tail -25 "$WAITS"
  exit 1
done

echo
echo "=== $MAX runs, no timeout hang. A bounded null result — a non-occurrence, not an absence of bug. ==="
echo "    unrelated failures along the way: $OTHER (kept as run-other-*.tap / waits-other-*.log)"
