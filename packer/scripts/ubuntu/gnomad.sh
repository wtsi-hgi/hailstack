#!/usr/bin/env bash
set -euo pipefail

test -d /opt/hailstack/base-venv

# GNOMAD_VERSION is the gnomAD data release in the compatibility bundle.
# The Python methods package has its own 0.x release line.
GNOMAD_METHODS_VERSION="${GNOMAD_METHODS_VERSION:-0.8.2}"

/opt/hailstack/base-venv/bin/uv pip install \
  --python /opt/hailstack/base-venv/bin/python \
  "gnomad==${GNOMAD_METHODS_VERSION}"

/opt/hailstack/base-venv/bin/python -c 'import importlib.metadata, sys; import gnomad; print("gnomAD data release " + sys.argv[1] + " via gnomad package " + importlib.metadata.version("gnomad"))' "$GNOMAD_VERSION" | grep -F "$GNOMAD_VERSION"
