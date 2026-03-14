#!/usr/bin/env bash
set -euo pipefail

test -d /opt/hailstack/base-venv

/opt/hailstack/base-venv/bin/uv pip install \
  --python /opt/hailstack/base-venv/bin/python \
  jupyterlab

systemctl enable jupyter-lab.service
test -f /etc/systemd/system/jupyter-lab.service
JUPYTER_VERSION="${JUPYTER_VERSION:-$(/opt/hailstack/base-venv/bin/jupyter lab --version)}"
/opt/hailstack/base-venv/bin/jupyter lab --version | grep -F "$JUPYTER_VERSION"