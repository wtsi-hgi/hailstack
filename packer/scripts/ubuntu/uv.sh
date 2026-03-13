#!/usr/bin/env bash
set -euo pipefail

test -d /opt/hailstack/base-venv

/opt/hailstack/base-venv/bin/python -m pip install --upgrade uv
ln -sfn /opt/hailstack/base-venv/bin/uv /usr/local/bin/uv

test -x /opt/hailstack/base-venv/bin/uv
uv --version