#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y netdata

systemctl enable netdata.service
test -f /lib/systemd/system/netdata.service || test -f /etc/systemd/system/netdata.service