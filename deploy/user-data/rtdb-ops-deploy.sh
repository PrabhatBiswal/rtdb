#!/bin/bash
# Deploy (or roll back) the monitoring stack to an image tag. Written to
# /opt/rtdb/rtdb-ops-deploy.sh at boot and re-runnable for every deploy after it — the same shape as
# the gateway's rtdb-deploy.sh:
#
#   /opt/rtdb/rtdb-ops-deploy.sh <image-tag>
#
# It exists because its ABSENCE is the finding it closes (WP7 F-A2): with no way to move this box to
# a new tag, the dashboards ran a whole work package behind the JSON committed in the repo, and the
# only "deploy" path was replacing the instance — which would have failed, because the tag Terraform
# would have bootstrapped did not exist in ECR at all.
#
# Unlike the gateway's, this one stages: the new compose file is written beside the live one and only
# moved into place once the images have actually been pulled. A refused tag therefore leaves the box
# byte-identical to how it started, still serving the dashboards it was serving before.
set -euo pipefail
TAG="${1:?usage: rtdb-ops-deploy.sh <ops-image-tag> [console-image-tag]}"
CONSOLE_TAG="${2:-}"   # §5.13; absent means "leave the console alone"
. /opt/rtdb/ops.conf   # REGION, REGISTRY

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null

# The Grafana admin password is generated ONCE and thereafter carried across every deploy. Minting a
# fresh one on redeploy would silently change the operator's credential — and an operator locked out
# of the dashboards during an incident is worse than no dashboards at all. The live compose file is
# the authority here precisely because it is what Grafana actually reads.
umask 077
if [ -f /opt/rtdb/compose.yml ]; then
  PW="$(sed -n 's/.*GF_SECURITY_ADMIN_PASSWORD: //p' /opt/rtdb/compose.yml | head -1)"
else
  PW="$(head -c 18 /dev/urandom | base64 | tr -d '/+=')"
  printf '%s\n' "$PW" > /opt/rtdb/grafana-admin-password
fi
if [ -z "$PW" ]; then
  echo "REFUSING: no Grafana admin password found and none generated" >&2
  exit 1
fi

cat > /opt/rtdb/compose.yml.next <<YML
name: rtdb-ops
services:
  prometheus:
    image: $REGISTRY/rtdb-prometheus:$TAG
    # The AWS config: ec2_sd_configs on tag Project=rtdb (WP6 Gate A ruling Q4), so replacing a
    # gateway needs neither an image rebuild nor a config edit.
    command:
      - --config.file=/etc/prometheus/prometheus.aws.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=15d
    volumes: [promdata:/prometheus]
    ports: ["127.0.0.1:9090:9090"]
    restart: unless-stopped
    logging:
      driver: json-file
      options: {max-size: "50m", max-file: "3"}
  grafana:
    image: $REGISTRY/rtdb-grafana:$TAG
    environment:
      GF_SECURITY_ADMIN_PASSWORD: $PW
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes: [grafanadata:/var/lib/grafana]
    ports: ["127.0.0.1:3000:3000"]
    restart: unless-stopped
    logging:
      driver: json-file
      options: {max-size: "50m", max-file: "3"}
volumes:
  promdata:
  grafanadata:
YML
chmod 600 /opt/rtdb/compose.yml.next
umask 022

# THE GUARD. A tag that is not pullable must stop the deploy here, with the live compose file
# untouched and both containers still running. This is the check whose absence created F-A2: nothing
# ever asked whether rtdb-grafana:<tag> existed, so a tag that named no image looked exactly like a
# tag that named a good one until an instance replacement turned it into an outage.
if ! docker compose -f /opt/rtdb/compose.yml.next pull; then
  rm -f /opt/rtdb/compose.yml.next
  echo "DEPLOY REFUSED: could not pull rtdb-prometheus:$TAG and rtdb-grafana:$TAG" >&2
  echo "the live stack is untouched" >&2
  exit 1
fi

mv /opt/rtdb/compose.yml.next /opt/rtdb/compose.yml
systemctl restart rtdb-ops.service

# Both must actually answer, or this deploy did not happen. Prometheus first: Grafana without it is
# a set of empty panels, which is the failure this whole closeout exists to make visible.
MON_OK=0
for _ in $(seq 30); do
  if curl -fsS -m 3 http://127.0.0.1:9090/-/healthy >/dev/null 2>&1 \
     && curl -fsS -m 3 http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
    echo "ops deploy ok: $TAG healthy"
    MON_OK=1
    break
  fi
  sleep 2
done
if [ "$MON_OK" != 1 ]; then
  echo "OPS DEPLOY FAILED: $TAG did not become healthy in 60s" >&2
  docker compose -f /opt/rtdb/compose.yml ps >&2 || true
  exit 1
fi

###############################################################################
# §5.13 THE CONSOLE. Last on purpose, and it CANNOT take the monitoring stack with it.
#
# The ordering is the whole design. On a first boot after an instance replacement this script runs
# from ops.sh, and the console artifact may legitimately not be in ECR yet — the repository is
# created by the same terraform that replaces this box, so the very first apply can reach here with
# nothing to pull. Under `set -euo pipefail` an unguarded failure would abort the script, and
# because the console runs LAST that would still leave Prometheus and Grafana serving; if it ran
# first it would take them down too. So: monitoring is already healthy and committed above, and
# every console step below is guarded and merely sets a non-zero exit.
#
# "The box is up and one command away from a good tag" is this file's existing philosophy (see the
# header). This extends it to a third component rather than making the third one fatal to the
# other two.
###############################################################################
if [ -z "$CONSOLE_TAG" ]; then
  echo "console: no tag given, leaving it untouched"
  exit 0
fi

console_fail() { echo "CONSOLE INSTALL FAILED: $1" >&2; exit 1; }

# The unit runs /usr/bin/node against an ES module. AL2023's `nodejs` is the floor we depend on;
# assert it rather than discover it at first sign-in.
command -v node >/dev/null 2>&1 || console_fail "node is not installed"
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
[ "$NODE_MAJOR" -ge 18 ] || console_fail "node $NODE_MAJOR is too old for auth-server.mjs (need >= 18)"

# STAGE, then move — the same shape as compose.yml.next above, for the same reason: a refused tag
# must leave the console that is already serving byte-identical to how it started.
STAGE="$(mktemp -d /opt/rtdb/console-stage.XXXXXX)"
trap 'rm -rf "$STAGE"' EXIT

# Never started, so nothing in the image executes and IMDS never enters the container. See
# console/Dockerfile.
# `2>/dev/null` here was a mistake worth naming: the first version of this line swallowed docker's
# own message, and the message WAS the diagnosis — an image with no CMD fails `create` with "no
# command specified", which has nothing to do with whether the tag is pushed. A guard that hides the
# reason it fired is barely a guard.
CREATE_ERR="$(mktemp)"
CID="$(docker create "$REGISTRY/rtdb-console:$CONSOLE_TAG" 2>"$CREATE_ERR")" \
  || console_fail "docker create failed for rtdb-console:$CONSOLE_TAG -- $(tr -d '\n' < "$CREATE_ERR")"
rm -f "$CREATE_ERR"
docker cp "$CID:/app/auth-server.mjs"   "$STAGE/auth-server.mjs"   >/dev/null 2>&1 || { docker rm -f "$CID" >/dev/null 2>&1 || true; console_fail "auth-server.mjs missing from the image"; }
docker cp "$CID:/app/rtdb-console.html" "$STAGE/rtdb-console.html" >/dev/null 2>&1 || { docker rm -f "$CID" >/dev/null 2>&1 || true; console_fail "rtdb-console.html missing from the image"; }
docker rm -f "$CID" >/dev/null 2>&1 || true

# A truncated docker cp is a file, so size alone is not proof — but an EMPTY one is proof of
# failure, and node parsing the module is proof the transfer was not corrupt.
[ -s "$STAGE/auth-server.mjs" ]   || console_fail "auth-server.mjs came out empty"
[ -s "$STAGE/rtdb-console.html" ] || console_fail "rtdb-console.html came out empty"
node --check "$STAGE/auth-server.mjs" 2>/dev/null || console_fail "auth-server.mjs does not parse"

install -o rtdb-console -g rtdb-console -m 0444 "$STAGE/auth-server.mjs"   /opt/rtdb-console/auth-server.mjs
install -o rtdb-console -g rtdb-console -m 0444 "$STAGE/rtdb-console.html" /opt/rtdb-console/rtdb-console.html
sha256sum /opt/rtdb-console/auth-server.mjs /opt/rtdb-console/rtdb-console.html

systemctl daemon-reload
systemctl enable rtdb-console.service >/dev/null 2>&1 || true
systemctl restart rtdb-console.service || console_fail "the unit would not start (journalctl -u rtdb-console)"

# It must actually answer. "The service is active" is not the same fact.
for _ in $(seq 20); do
  if curl -fsS -m 3 http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    echo "console deploy ok: $CONSOLE_TAG healthy"
    exit 0
  fi
  sleep 2
done
console_fail "$CONSOLE_TAG did not answer /healthz in 40s"
