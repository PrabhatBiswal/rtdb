#!/bin/bash
# RTDB ops host: Prometheus + Grafana, both from ECR with their config baked in (WP6 Gate A §2.4).
# No inbound rule exists; Grafana is reached over SSM port forwarding.
#
# Everything version-specific lives in /opt/rtdb/rtdb-ops-deploy.sh, exactly as the gateway keeps
# its own deploy in /opt/rtdb/rtdb-deploy.sh. Before WP7 it did not: this file wrote the compose
# file inline, once, at boot, and there was no second way to move the box to a new tag (F-A2).
set -euxo pipefail

dnf -y update
# nodejs for the console (§5.13). It runs NATIVELY, not in a container: this box sets
# http_tokens=required with no hop limit, so it defaults to 1, and the console reads SSM at runtime
# to serve sign-in. Native keeps IMDS one hop away and keeps the reviewed systemd hardening.
dnf -y install docker nodejs
mkdir -p /usr/libexec/docker/cli-plugins
curl -fsSL -o /usr/libexec/docker/cli-plugins/docker-compose \
  "https://github.com/docker/compose/releases/download/v2.31.0/docker-compose-linux-aarch64"
chmod +x /usr/libexec/docker/cli-plugins/docker-compose
systemctl enable --now docker

install -d -m 0755 /opt/rtdb
cat > /opt/rtdb/ops.conf <<CONF
REGION=${region}
REGISTRY=${registry}
CONF

# §5.13: the console's own user, directory and unit. Before this, none of the three existed on a
# replaced box — the auth-server was hand-deployed over SSM and lived exactly as long as the
# instance did, which is how a load-client apply took sign-in down on 2026-09-02. The FILES are
# fetched by the deploy script below; everything that is small enough to carry is carried here.
id -u rtdb-console >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin rtdb-console
install -d -m 0755 -o rtdb-console -g rtdb-console /opt/rtdb-console

cat > /etc/systemd/system/rtdb-console.service <<'CONSOLE_UNIT'
${console_unit}
CONSOLE_UNIT

cat > /opt/rtdb/rtdb-ops-deploy.sh <<'DEPLOY_EOF'
${deploy_script}
DEPLOY_EOF
chmod 700 /opt/rtdb/rtdb-ops-deploy.sh

cat > /etc/systemd/system/rtdb-ops.service <<'UNIT'
[Unit]
Description=RTDB monitoring
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
systemctl enable rtdb-ops.service

# First deploy. The script generates the Grafana admin password on this path (there is no compose
# file yet to carry one across) and writes it to /opt/rtdb/grafana-admin-password, which is where
# the runbook says to look for it. A tag missing from ECR fails loudly here, which is the honest
# outcome: the box is up and reachable over SSM, and one command away from a good tag.
# Second argument is the console artifact tag (§5.13). The console install is deliberately the LAST
# thing the deploy script does and cannot fail the monitoring stack — see the note there.
/opt/rtdb/rtdb-ops-deploy.sh ${ops_image_tag} ${console_image_tag}
