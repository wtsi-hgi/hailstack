#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y openjdk-${JAVA_VERSION}-jdk scala python3 python3-pip

java -version 2>&1 | grep -F "$JAVA_VERSION"
python3 --version | grep -F "$PYTHON_VERSION"
scala -version 2>&1 | grep -F "$SCALA_VERSION"