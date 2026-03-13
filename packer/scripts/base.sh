#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y python3 python3-venv python3-pip curl ca-certificates nginx

install -d -m 0755 /opt/hailstack/base-venv
python3 -m venv /opt/hailstack/base-venv
/opt/hailstack/base-venv/bin/python -m pip install --upgrade pip uv

cat >/etc/systemd/system/hailstack-jupyterlab.service <<'EOF'
[Unit]
Description=Hailstack JupyterLab
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/hailstack/base-venv/bin/jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
test -d /opt/hailstack/base-venv
test -f /etc/systemd/system/hailstack-jupyterlab.service
test -f /lib/systemd/system/nginx.service || test -f /etc/systemd/system/nginx.service