#!/bin/bash
# Deploy (or roll back) the gateway to an image tag. Written to /opt/rtdb/rtdb-deploy.sh at boot and
# re-runnable for every deploy after it, which is what makes "a rollback names a commit" executable:
#
#   /opt/rtdb/rtdb-deploy.sh <image-tag>
#
# It re-reads the secrets from SSM every time, so rotating a parameter needs no new instance. Run it
# on ONE gateway at a time: the NLB drains the one being restarted onto the other (§8 sizes each for
# 100% of the load), so a rolling deploy costs latency, never a write.
set -euo pipefail
TAG="${1:?usage: rtdb-deploy.sh <image-tag>}"
. /opt/rtdb/deploy.conf   # REGION, REGISTRY, PRUNE_MS, LOCK_TTL_MS, PG_POOL

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null

# Secrets go from SSM straight into a root-only file. `set +x` is not enough on its own — the file
# is written under umask 077 and never echoed, and nothing below ever prints its contents.
umask 077
DB_URL="$(aws ssm get-parameter --region "$REGION" --name /rtdb/prod/db_url --with-decryption --query Parameter.Value --output text)"
JWT="$(aws ssm get-parameter --region "$REGION" --name /rtdb/prod/jwt_secret --with-decryption --query Parameter.Value --output text)"
REDIS_URL="$(aws ssm get-parameter --region "$REGION" --name /rtdb/prod/redis_url --query Parameter.Value --output text)"
cat > /opt/rtdb/gateway.env <<ENV
RTDB_PORT=8080
RTDB_ADMIN_PORT=9090
RTDB_STORAGE=postgres
RTDB_PG_URL=$DB_URL
RTDB_PG_POOL=$PG_POOL
RTDB_REDIS_URL=$REDIS_URL
RTDB_SHARD=0
RTDB_PRUNE_MS=$PRUNE_MS
RTDB_LOCK_TTL_MS=$LOCK_TTL_MS
RTDB_DEV_SECRET=$JWT
# NO BACKTICKS AND NO \$(...) BELOW THIS LINE. This heredoc is UNQUOTED because it has to expand
# the secrets above — which means it also runs command substitution, as root. The first version of
# these comments quoted file names the usual way and the rehearsal caught it: the shell executed
# them, wrote empty strings where the names should have been, and would have done that on every
# production deploy. Quoting the heredoc is not the fix (the secrets would stop expanding); keeping
# substitution characters out of it is.
#
# §5.19's fail-closed guard: main.ts will not boot on Postgres without this, because a real
# deployment that forgot it authorizes every authenticated client for every path. The module is
# resolved against the working directory and ships in the image (Dockerfile: COPY rules ./rules).
# rules/allow-all.ts says why production runs an open policy today, in the file itself.
RTDB_RULES=rules/allow-all.ts
# §5.23 faisla 5: the per-database quota bucket is PER GATEWAY, so the shard's 64/128 is divided by
# however many gateways serve it. Two here => 32/64 each. Wrong or missing, this fleet would hand
# out twice the quota it promised — there is no shared bucket, deliberately (a Redis round trip on
# the write path is the mistake §7 already paid for).
RTDB_GATEWAY_COUNT=2
# §5.26: one database per Postgres SCHEMA, chosen by the token's ns claim at hello. OFF, this
# gateway serves every token from the schema RTDB_PG_SCHEMA names (public) and ignores ns entirely,
# which is what made the §5.25 roll backwards-compatible. ON, the pool and the LISTEN connection are
# shared across tenants and sized from the registry at boot: N declared + 1 default, plus reads
# headroom. RTDB_PG_POOL above is IGNORED in this mode -- the size comes from the registry, not from
# a fixed number, so growing it means declaring a database and restarting.
# RTDB_REQUIRE_NS stays unset on purpose: that one refuses every token carrying no ns, which is
# every token minted before the claim existed. RUNBOOK section 7d calls it an outage lever.
RTDB_MULTI_TENANT=1
ENV

# STAGED, exactly as `rtdb-ops-deploy.sh` learned to do it: write the NEXT compose file, prove the
# tag is pullable, and only then make it live. Written live-first, a failed pull leaves
# /opt/rtdb/compose.yml naming an image that does not exist — the deploy fails visibly, and then
# the NEXT restart or reboot fails invisibly, because nothing re-reads the tag. That is the shape
# that turned a bad ops tag into an outage on instance replacement (§7a's F-A2), and it was left
# here for "the next natural gateway deploy". This is that deploy.
cat > /opt/rtdb/compose.yml.next <<YML
name: rtdb
services:
  gateway:
    image: $REGISTRY/rtdb-gateway:$TAG
    env_file: /opt/rtdb/gateway.env
    network_mode: host
    restart: unless-stopped
    stop_grace_period: 30s
    logging:
      driver: json-file
      options: {max-size: "50m", max-file: "3"}
YML
umask 022
chmod 600 /opt/rtdb/gateway.env

if ! docker compose -f /opt/rtdb/compose.yml.next pull; then
  rm -f /opt/rtdb/compose.yml.next
  echo "DEPLOY REFUSED: could not pull rtdb-gateway:$TAG" >&2
  echo "the live compose file and the running container are untouched" >&2
  exit 1
fi

mv /opt/rtdb/compose.yml.next /opt/rtdb/compose.yml
systemctl restart rtdb.service

# A deploy that "succeeded" because nothing checked it is how a bad image reaches both gateways.
# /healthz reaches storage on every call, so this is a real readiness check, not a liveness one.
for _ in $(seq 30); do
  if curl -fsS -m 3 http://127.0.0.1:9090/healthz >/dev/null 2>&1; then
    echo "deploy ok: $TAG healthy"
    exit 0
  fi
  sleep 2
done
echo "DEPLOY FAILED: $TAG did not become healthy in 60s" >&2
docker logs --tail 30 "$(docker ps -aq --filter name=rtdb-gateway --filter status=running --filter status=restarting | head -1)" 2>&1 >&2 || true
exit 1
