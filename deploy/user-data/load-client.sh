#!/bin/bash
# Gate D load client. Docker only — the rig itself is an ECR image, so there is no Node install, no
# checkout and no build here, and the client runs byte-identical code to what was tested locally.
# No image is pulled at boot: ECR tags are IMMUTABLE, so there is no moving `latest` to chase and
# the run command names the exact tag it wants.
set -euxo pipefail
dnf -y update
dnf -y install docker
systemctl enable --now docker
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${registry}

install -d -m 0755 /opt/rtdb
cat > /opt/rtdb/loadsim.conf <<CONF
REGION=${region}
REGISTRY=${registry}
CONF

# 10k connections across two boxes is 5k per box, and every connection is a file descriptor on each
# side. The default 1024 would fail this run in the least interesting way possible.
cat > /etc/systemd/system/docker.service.d/nofile.conf <<'DROPIN' || true
[Service]
LimitNOFILE=1048576
DROPIN
mkdir -p /etc/systemd/system/docker.service.d
cat > /etc/systemd/system/docker.service.d/nofile.conf <<'DROPIN'
[Service]
LimitNOFILE=1048576
DROPIN
systemctl daemon-reload
systemctl restart docker
