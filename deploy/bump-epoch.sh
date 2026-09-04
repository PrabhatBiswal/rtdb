#!/usr/bin/env bash
# §2's epoch bump for a restored shard — the one step in the restore whose omission is SILENT.
# Skip it and the shard serves happily while every client that was connected before the restore
# diverges (docs/wp4-restore-drill.md §5 reproduces that on purpose). Prose did not deserve to be
# the only guard, so this refuses to run when a gateway could still be reading the instance.
#
#   ./bump-epoch.sh <db-host>            bump, after proving no gateway can reach it
#   ./bump-epoch.sh <db-host> --check    run the guards only, change nothing
#
# <db-host> is always the REAL RDS endpoint — guard 1 compares it against what the gateways are
# configured with, so a tunnel address here would defeat the check. When you are reaching it through
# an SSM port-forward (the normal case from a laptop), set PGHOST_VIA/PGPORT to the tunnel:
#
#   PGHOST_VIA=127.0.0.1 PGPORT=15432 ./bump-epoch.sh rtdb-restore.xxxx.rds.amazonaws.com
#
# Expects PGPASSWORD in the environment. Run it BEFORE repointing /rtdb/prod/db_url, never after.
set -euo pipefail

HOST="${1:?usage: bump-epoch.sh <db-host> [--check]}"
CHECK_ONLY="${2:-}"
PORT="${PGPORT:-5432}"
VIA="${PGHOST_VIA:-$HOST}"
REGION="${AWS_REGION:-ap-south-1}"
PSQL=(psql "host=$VIA port=$PORT user=rtdb dbname=rtdb sslmode=require" -tAc)

fail() { echo "REFUSING: $*" >&2; exit 1; }

# Guard 1: is this instance what the gateways are configured to use? If so they will reconnect to it
# at any moment, and "no one is connected right now" is luck, not safety.
CONFIGURED="$(aws ssm get-parameter --region "$REGION" --name /rtdb/prod/db_url --with-decryption \
                --query Parameter.Value --output text 2>/dev/null || echo '')"
case "$CONFIGURED" in
  *"@$HOST:"*) fail "/rtdb/prod/db_url still points at $HOST. Bump BEFORE repointing, not after — \
otherwise a gateway can connect to the un-bumped shard and hand a client the dead generation." ;;
esac

# Guard 2: is anything actually attached right now? A gateway holds pool connections plus a
# dedicated LISTEN connection, so any client backend that is not us is a reason to stop.
OTHERS="$("${PSQL[@]}" "select count(*) from pg_stat_activity
                         where datname = current_database() and backend_type = 'client backend'
                           and pid <> pg_backend_pid()")"
[ "$OTHERS" -eq 0 ] || fail "$OTHERS other client connection(s) on $HOST. Stop every gateway first \
(§3b: a survivor keeps a consumer position from the dead generation)."

BEFORE="$("${PSQL[@]}" "select 'head='||v||'  epoch='||epoch||'  pruned='||pruned_through from rev_counter")"
echo "guards passed. $HOST: $BEFORE"

if [ "$CHECK_ONLY" = "--check" ]; then
  echo "--check: nothing changed."
  exit 0
fi

# A RANDOM epoch, not epoch+1: a restore can move the shard back to a point where +1 collides with a
# generation this shard has already used, and a colliding epoch is worse than no bump — the client
# believes it is still current. Same reasoning as a fresh store minting a random epoch (memory.ts).
"${PSQL[@]}" "update rev_counter set epoch = (floor(random() * 2147483646) + 1)::bigint where shard = 0" >/dev/null
AFTER="$("${PSQL[@]}" "select 'head='||v||'  epoch='||epoch from rev_counter")"
echo "bumped.        $HOST: $AFTER"

OLD_E="${BEFORE#*epoch=}"; OLD_E="${OLD_E%% *}"
NEW_E="${AFTER#*epoch=}";  NEW_E="${NEW_E%% *}"
[ "$OLD_E" != "$NEW_E" ] || fail "epoch did not change ($OLD_E). Do NOT let a gateway near this shard."
echo "epoch $OLD_E -> $NEW_E. Now repoint /rtdb/prod/db_url and restart EVERY gateway."
