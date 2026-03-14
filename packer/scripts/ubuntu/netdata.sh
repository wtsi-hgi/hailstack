#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y netdata

systemctl enable netdata.service
test -f /lib/systemd/system/netdata.service || test -f /etc/systemd/system/netdata.service
NETDATA_VERSION="${NETDATA_VERSION:-$(netdata -V 2>&1 | head -n 1)}"
netdata -V 2>&1 | head -n 1 | grep -F "$NETDATA_VERSION"