#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
HAILSTACK_PACKER_APT_HELPER="${HAILSTACK_PACKER_APT_HELPER:-/tmp/hailstack-packer-apt-locks.sh}"
if [[ ! -r "${HAILSTACK_PACKER_APT_HELPER}" ]]; then
	script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
	if [[ -r "${script_dir}/apt-locks.sh" ]]; then
		HAILSTACK_PACKER_APT_HELPER="${script_dir}/apt-locks.sh"
	fi
fi
# shellcheck source=/tmp/hailstack-packer-apt-locks.sh
source "${HAILSTACK_PACKER_APT_HELPER}"

PYTHON_BIN="python${PYTHON_VERSION}"
PYTHON_VENV_PACKAGE="${PYTHON_BIN}-venv"

hailstack_apt_get update
hailstack_apt_get install -y \
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
	hailstack_add_apt_repository -y ppa:deadsnakes/ppa
	hailstack_apt_get update
fi

hailstack_apt_get install -y "${PYTHON_BIN}" "${PYTHON_VENV_PACKAGE}"

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
