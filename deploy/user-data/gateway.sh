#!/bin/bash
# RTDB gateway host: Docker, a deploy script, and a systemd unit. Everything version-specific lives
# in /opt/rtdb/rtdb-deploy.sh so a deploy or a rollback is one command and needs no new instance.
set -euxo pipefail

dnf -y update
dnf -y install docker
# The compose plugin is not in the AL2023 repos; install it where the docker CLI looks for plugins.
mkdir -p /usr/libexec/docker/cli-plugins
curl -fsSL -o /usr/libexec/docker/cli-plugins/docker-compose \
  "https://github.com/docker/compose/releases/download/v2.31.0/docker-compose-linux-aarch64"
chmod +x /usr/libexec/docker/cli-plugins/docker-compose
systemctl enable --now docker

install -d -m 0755 /opt/rtdb
cat > /opt/rtdb/deploy.conf <<CONF
REGION=${region}
REGISTRY=${registry}
PRUNE_MS=${prune_ms}
LOCK_TTL_MS=${lock_ttl_ms}
PG_POOL=${pg_pool}
CONF

cat > /opt/rtdb/rtdb-deploy.sh <<'DEPLOY_EOF'
${deploy_script}
DEPLOY_EOF
chmod 700 /opt/rtdb/rtdb-deploy.sh

cat > /etc/systemd/system/rtdb.service <<'UNIT'
[Unit]
Description=RTDB gateway
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/rtdb
ExecStart=/usr/bin/docker compose -f /opt/rtdb/compose.yml up -d
ExecStop=/usr/bin/docker compose -f /opt/rtdb/compose.yml down
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable rtdb.service

# First deploy. If the image is not in ECR yet this fails loudly, which is the honest outcome — the
# instance is up and reachable over SSM, and `rtdb-deploy.sh <tag>` is one command away.
/opt/rtdb/rtdb-deploy.sh ${image_tag}
