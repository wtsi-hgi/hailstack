#!/usr/bin/env bash
set -euo pipefail

test -d /opt/hailstack/base-venv

/opt/hailstack/base-venv/bin/uv pip install \
  --python /opt/hailstack/base-venv/bin/python \
  "hail==${HAIL_VERSION}" \
  "pyspark==${SPARK_VERSION}"

/opt/hailstack/base-venv/bin/python -c 'import hail; print(hail.__version__)' | grep -F "$HAIL_VERSION"