#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
HAILSTACK_PACKER_APT_HELPER="${HAILSTACK_PACKER_APT_HELPER:-/tmp/hailstack-packer-apt-locks.sh}"
if [[ ! -r "${HAILSTACK_PACKER_APT_HELPER}" ]]; then
	script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
	if [[ -r "${script_dir}/../apt-locks.sh" ]]; then
		HAILSTACK_PACKER_APT_HELPER="${script_dir}/../apt-locks.sh"
	fi
fi
# shellcheck source=/tmp/hailstack-packer-apt-locks.sh
source "${HAILSTACK_PACKER_APT_HELPER}"

PYTHON_BIN="python${PYTHON_VERSION}"

hailstack_apt_get update
hailstack_apt_get install -y openjdk-${JAVA_VERSION}-jdk scala "${PYTHON_BIN}" python3-pip

java -version 2>&1 | grep -F "$JAVA_VERSION"
"${PYTHON_BIN}" --version 2>&1 | grep -F "$PYTHON_VERSION"
scala -version 2>&1 | grep -F "$SCALA_VERSION"
