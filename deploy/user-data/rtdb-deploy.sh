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
ENV

cat > /opt/rtdb/compose.yml <<YML
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

docker compose -f /opt/rtdb/compose.yml pull
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
