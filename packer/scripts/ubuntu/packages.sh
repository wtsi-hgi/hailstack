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
SCALA_DEB_PATH="${TMPDIR:-/tmp}/scala-${SCALA_VERSION}.deb"

hailstack_verify_version() {
	local name="$1"
	local expected="$2"
	shift 2
	local output

	if ! output="$("$@" 2>&1)"; then
		printf '[hailstack] %s version command failed: %s\n%s\n' "${name}" "$*" "${output}" >&2
		return 1
	fi

	if ! grep -F "${expected}" <<<"${output}"; then
		printf '[hailstack] expected %s version containing %s, got:\n%s\n' "${name}" "${expected}" "${output}" >&2
		return 1
	fi
}

hailstack_apt_get update
hailstack_apt_get install -y openjdk-${JAVA_VERSION}-jdk "${PYTHON_BIN}" python3-pip

curl -fsSL "https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb" -o "${SCALA_DEB_PATH}"
hailstack_apt_get install -y "${SCALA_DEB_PATH}"
rm -f "${SCALA_DEB_PATH}"

hailstack_verify_version Java "$JAVA_VERSION" java -version
hailstack_verify_version Python "$PYTHON_VERSION" "${PYTHON_BIN}" --version
hailstack_verify_version Scala "$SCALA_VERSION" scala -version
