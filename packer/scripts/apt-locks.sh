#!/usr/bin/env bash
set -euo pipefail

HAILSTACK_APT_LOCK_TIMEOUT_SECONDS="${HAILSTACK_APT_LOCK_TIMEOUT_SECONDS:-600}"
HAILSTACK_APT_LOCK_POLL_SECONDS="${HAILSTACK_APT_LOCK_POLL_SECONDS:-5}"
HAILSTACK_APT_LOCK_PATHS=(
	/var/lib/dpkg/lock-frontend
	/var/lib/dpkg/lock
	/var/lib/apt/lists/lock
	/var/cache/apt/archives/lock
)
HAILSTACK_APT_UNITS=(
	apt-daily.timer
	apt-daily-upgrade.timer
	apt-daily.service
	apt-daily-upgrade.service
	unattended-upgrades.service
)

hailstack_stop_unattended_apt() {
	local unit

	if ! command -v systemctl >/dev/null 2>&1; then
		return 0
	fi

	for unit in "${HAILSTACK_APT_UNITS[@]}"; do
		systemctl --no-block stop "${unit}" >/dev/null 2>&1 || true
	done
}

hailstack_apt_lock_holders_for() {
	local lock_path="$1"

	if command -v fuser >/dev/null 2>&1; then
		fuser "${lock_path}" 2>/dev/null || true
		return 0
	fi

	if command -v lsof >/dev/null 2>&1; then
		lsof -t "${lock_path}" 2>/dev/null || true
	fi
}

hailstack_apt_lock_holders() {
	local holders
	local lock_path

	for lock_path in "${HAILSTACK_APT_LOCK_PATHS[@]}"; do
		if [[ ! -e "${lock_path}" ]]; then
			continue
		fi

		holders="$(hailstack_apt_lock_holders_for "${lock_path}")"
		holders="${holders//$'\n'/ }"
		if [[ -n "${holders//[[:space:]]/}" ]]; then
			printf '%s: %s\n' "${lock_path}" "${holders}"
		fi
	done
}

hailstack_wait_for_apt_locks() {
	local deadline
	local holders
	local remaining

	deadline=$((SECONDS + HAILSTACK_APT_LOCK_TIMEOUT_SECONDS))
	hailstack_stop_unattended_apt

	while true; do
		holders="$(hailstack_apt_lock_holders)"
		if [[ -z "${holders}" ]]; then
			return 0
		fi

		remaining=$((deadline - SECONDS))
		if ((remaining <= 0)); then
			printf '[hailstack] timed out waiting for apt/dpkg locks after %s seconds\n' "${HAILSTACK_APT_LOCK_TIMEOUT_SECONDS}" >&2
			printf '%s\n' "${holders}" >&2
			return 1
		fi

		printf '[hailstack] waiting up to %s more seconds for apt/dpkg locks:\n%s\n' "${remaining}" "${holders}" >&2
		sleep "${HAILSTACK_APT_LOCK_POLL_SECONDS}"
	done
}

hailstack_apt_get() {
	hailstack_wait_for_apt_locks
	apt-get -o "DPkg::Lock::Timeout=${HAILSTACK_APT_LOCK_TIMEOUT_SECONDS}" "$@"
}

hailstack_add_apt_repository() {
	hailstack_wait_for_apt_locks
	add-apt-repository "$@"
}
