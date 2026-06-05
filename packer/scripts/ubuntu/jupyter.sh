#!/usr/bin/env bash
set -euo pipefail

test -d /opt/hailstack/base-venv

JUPYTER_VERSION="${JUPYTER_VERSION:-3.5.3}"
JUPYTER_SERVER_VERSION="${JUPYTER_SERVER_VERSION:-2.10.0}"
JUPYTERLAB_SERVER_VERSION="${JUPYTERLAB_SERVER_VERSION:-2.16.6}"
JUPYTER_EVENTS_VERSION="${JUPYTER_EVENTS_VERSION:-0.6.3}"
JSONSCHEMA_VERSION="${JSONSCHEMA_VERSION:-3.2.0}"

# gnomAD 0.8.2 pulls ga4gh-vrs 0.8.4, which requires jsonschema<4.
# Pin the compatible Jupyter stack instead of allowing JupyterLab 4.x.
/opt/hailstack/base-venv/bin/uv pip install \
  --python /opt/hailstack/base-venv/bin/python \
  --upgrade \
  "jupyterlab==${JUPYTER_VERSION}" \
  "jupyter-server==${JUPYTER_SERVER_VERSION}" \
  "jupyterlab-server==${JUPYTERLAB_SERVER_VERSION}" \
  "jupyter-events==${JUPYTER_EVENTS_VERSION}" \
  "jsonschema==${JSONSCHEMA_VERSION}"

/opt/hailstack/base-venv/bin/python -m pip check
/opt/hailstack/base-venv/bin/python -c "from importlib import metadata; from jupyterlab.labapp import LabApp; from jupyter_server.serverapp import ServerApp; assert metadata.version('jupyterlab') == '${JUPYTER_VERSION}'; assert metadata.version('jupyter-server') == '${JUPYTER_SERVER_VERSION}'; assert metadata.version('jupyterlab-server') == '${JUPYTERLAB_SERVER_VERSION}'; assert metadata.version('jupyter-events') == '${JUPYTER_EVENTS_VERSION}'; assert metadata.version('jsonschema') == '${JSONSCHEMA_VERSION}'; print(LabApp.name, ServerApp.name)"

systemctl enable jupyter-lab.service
test -f /etc/systemd/system/jupyter-lab.service
/opt/hailstack/base-venv/bin/jupyter lab --version | grep -F "$JUPYTER_VERSION"
