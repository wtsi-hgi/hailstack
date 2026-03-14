#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

PYTHON_BIN="python${PYTHON_VERSION}"

apt-get update
apt-get install -y openjdk-${JAVA_VERSION}-jdk scala "${PYTHON_BIN}" python3-pip

java -version 2>&1 | grep -F "$JAVA_VERSION"
"${PYTHON_BIN}" --version 2>&1 | grep -F "$PYTHON_VERSION"
scala -version 2>&1 | grep -F "$SCALA_VERSION"