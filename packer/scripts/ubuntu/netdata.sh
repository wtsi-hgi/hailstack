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

hailstack_apt_get update
hailstack_apt_get install -y netdata

systemctl enable netdata.service
test -f /lib/systemd/system/netdata.service || test -f /etc/systemd/system/netdata.service
NETDATA_VERSION="${NETDATA_VERSION:-$(netdata -V 2>&1 | head -n 1)}"
netdata -V 2>&1 | head -n 1 | grep -F "$NETDATA_VERSION"
