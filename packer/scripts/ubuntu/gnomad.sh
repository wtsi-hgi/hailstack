#!/usr/bin/env bash
set -euo pipefail

test -d /opt/hailstack/base-venv

/opt/hailstack/base-venv/bin/uv pip install \
  --python /opt/hailstack/base-venv/bin/python \
  "gnomad==${GNOMAD_VERSION}"

/opt/hailstack/base-venv/bin/python -c 'import gnomad; print(gnomad.__version__)' | grep -F "$GNOMAD_VERSION"