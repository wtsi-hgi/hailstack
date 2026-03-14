#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

PYTHON_BIN="python${PYTHON_VERSION}"
PYTHON_VENV_PACKAGE="${PYTHON_BIN}-venv"

apt-get update
apt-get install -y \
	apache2-utils \
	curl \
	ca-certificates \
	cryptsetup \
	netcat-openbsd \
	nfs-common \
	nfs-kernel-server \
	nginx \
	python3-pip \
	software-properties-common

if ! apt-cache show "${PYTHON_BIN}" >/dev/null 2>&1; then
	add-apt-repository -y ppa:deadsnakes/ppa
	apt-get update
fi

apt-get install -y "${PYTHON_BIN}" "${PYTHON_VENV_PACKAGE}"

install -d -m 0755 /opt/hailstack/base-venv
"${PYTHON_BIN}" -m venv /opt/hailstack/base-venv
/opt/hailstack/base-venv/bin/python -m pip install --upgrade pip uv
/opt/hailstack/base-venv/bin/python -m venv --system-site-packages /opt/hailstack/overlay-venv
BASE_PURELIB=$(/opt/hailstack/base-venv/bin/python -c "import sysconfig; print(sysconfig.get_path('purelib'))")
OVERLAY_PURELIB=$(/opt/hailstack/overlay-venv/bin/python -c "import sysconfig; print(sysconfig.get_path('purelib'))")
printf '%s\n' "${BASE_PURELIB}" > "${OVERLAY_PURELIB}/hailstack-base-venv.pth"

cat >/etc/systemd/system/jupyter-lab.service <<'EOF'
[Unit]
Description=Hailstack JupyterLab
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/hailstack/overlay-venv/bin/python -m jupyterlab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
test -d /opt/hailstack/base-venv
test -d /opt/hailstack/overlay-venv
test -f /etc/systemd/system/jupyter-lab.service
test -f /lib/systemd/system/nginx.service || test -f /etc/systemd/system/nginx.service