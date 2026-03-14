# Copyright (c) 2026 Genome Research Ltd.
#
# Author: Sendu Bala <sb10@sanger.ac.uk>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Run the install playbook against a Pulumi-resolved inventory."""

import json
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from hailstack.errors import AnsibleError
from hailstack.pulumi.stack import REPOSITORY_ROOT

PLAYBOOK_PATH = REPOSITORY_ROOT / "ansible" / "install.yml"
DEFAULT_BASE_VENV_PATH = "/opt/hailstack/base-venv"
DEFAULT_OVERLAY_VENV_PATH = "/opt/hailstack/overlay-venv"
DEFAULT_SOFTWARE_STATE_PATH = "/var/lib/hailstack/software-state.json"


def _empty_str_list() -> list[str]:
    """Return a typed empty string list for dataclass defaults."""
    return []


def _empty_object_dict() -> dict[str, object]:
    """Return a typed empty object dictionary for dataclass defaults."""
    return {}


@dataclass(frozen=True)
class NodeResult:
    """Represent the parsed per-node output from the install playbook."""

    hostname: str
    success: bool
    system_installed: list[str] = field(default_factory=_empty_str_list)
    python_installed: list[str] = field(default_factory=_empty_str_list)
    errors: list[str] = field(default_factory=_empty_str_list)
    verification: dict[str, object] = field(default_factory=_empty_object_dict)
    changed: bool = False


def run_install_playbook(
    inventory: Mapping[str, list[str]],
    system_packages: list[str],
    python_packages: list[str],
    ssh_username: str,
    ssh_key_path: Path | None,
    worker_jump_host: str | None = None,
    smoke_test: str | None = None,
) -> list[NodeResult]:
    """Run the install playbook and return parsed per-host results."""
    if not PLAYBOOK_PATH.is_file():
        raise AnsibleError(f"Install playbook not found: {PLAYBOOK_PATH}")

    with TemporaryDirectory(prefix="hailstack-install-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        inventory_path = temp_dir / "inventory.json"
        vars_path = temp_dir / "vars.json"
        result_path = temp_dir / "results.jsonl"
        _write_inventory_file(
            inventory_path,
            inventory,
            ssh_username,
            worker_jump_host=worker_jump_host,
        )
        _write_vars_file(
            vars_path,
            system_packages=system_packages,
            python_packages=python_packages,
            smoke_test=smoke_test,
            result_path=result_path,
        )
        completed = _run_playbook_command(
            _build_playbook_command(
                inventory_path=inventory_path,
                vars_path=vars_path,
                ssh_username=ssh_username,
                ssh_key_path=ssh_key_path,
            )
        )
        parsed_results = _read_results_file(result_path)

    if completed.returncode != 0 and not parsed_results:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise AnsibleError(f"Install playbook failed: {detail}")

    return parsed_results


def _build_playbook_command(
    *,
    inventory_path: Path,
    vars_path: Path,
    ssh_username: str,
    ssh_key_path: Path | None,
) -> list[str]:
    """Construct the ansible-playbook command line."""
    command = [
        "ansible-playbook",
        str(PLAYBOOK_PATH),
        "-i",
        str(inventory_path),
        "-u",
        ssh_username,
        "-e",
        f"@{vars_path}",
    ]
    if ssh_key_path is not None:
        command.extend(["--private-key", str(ssh_key_path)])
    return command


def _write_inventory_file(
    path: Path,
    inventory: Mapping[str, list[str]],
    ssh_username: str,
    *,
    worker_jump_host: str | None,
) -> None:
    """Write a JSON inventory for the install playbook."""
    hosts: dict[str, dict[str, str]] = {}
    children: dict[str, dict[str, dict[str, dict[str, str]]]] = {}

    for group_name, group_hosts in inventory.items():
        group_entries: dict[str, dict[str, str]] = {}
        for hostname in group_hosts:
            host_vars = {
                "ansible_host": hostname,
                "ansible_user": ssh_username,
            }
            if group_name == "worker" and worker_jump_host is not None:
                host_vars["ansible_ssh_common_args"] = (
                    f"-o ProxyJump={ssh_username}@{worker_jump_host} "
                    "-o StrictHostKeyChecking=no"
                )
            hosts[hostname] = host_vars
            group_entries[hostname] = host_vars
        children[group_name] = {"hosts": group_entries}

    payload = {
        "all": {
            "hosts": hosts,
            "children": children,
        }
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _write_vars_file(
    path: Path,
    *,
    system_packages: Sequence[str],
    python_packages: Sequence[str],
    smoke_test: str | None,
    result_path: Path,
) -> None:
    """Write install variables consumed by the playbook."""
    payload = {
        "base_venv_path": DEFAULT_BASE_VENV_PATH,
        "system_packages": list(system_packages),
        "python_packages": list(python_packages),
        "smoke_test": smoke_test,
        "overlay_venv_path": DEFAULT_OVERLAY_VENV_PATH,
        "software_state_path": DEFAULT_SOFTWARE_STATE_PATH,
        "hailstack_result_path": str(result_path),
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _run_playbook_command(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Execute ansible-playbook for install operations."""
    try:
        return subprocess.run(
            list(command),
            capture_output=True,
            check=False,
            cwd=REPOSITORY_ROOT,
            text=True,
        )
    except FileNotFoundError as error:
        raise AnsibleError("Ansible CLI not found") from error


def _read_results_file(path: Path) -> list[NodeResult]:
    """Parse the JSON-lines result file emitted by the install playbook."""
    if not path.is_file():
        return []

    results: list[NodeResult] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise AnsibleError(
                "Install playbook returned invalid result JSON"
            ) from error
        results.append(_coerce_node_result(payload))
    return results


def _coerce_node_result(payload: object) -> NodeResult:
    """Validate one node result payload from the playbook output."""
    if not isinstance(payload, dict):
        raise AnsibleError(
            "Install playbook returned a non-object node result")

    data = cast(dict[str, object], payload)
    hostname = data.get("hostname")
    success = data.get("success")
    changed = data.get("changed", False)
    verification = data.get("verification", {})
    if (
        not isinstance(hostname, str)
        or not hostname.strip()
        or not isinstance(success, bool)
    ):
        raise AnsibleError("Install playbook returned an invalid node result")
    if not isinstance(changed, bool):
        raise AnsibleError("Install playbook returned an invalid changed flag")
    if not isinstance(verification, dict):
        raise AnsibleError(
            "Install playbook returned invalid verification metadata")

    return NodeResult(
        hostname=hostname,
        success=success,
        system_installed=_coerce_string_list(data.get("system_installed")),
        python_installed=_coerce_string_list(data.get("python_installed")),
        errors=_coerce_string_list(data.get("errors")),
        verification=cast(dict[str, object], verification),
        changed=changed,
    )


def _coerce_string_list(value: object) -> list[str]:
    """Convert a JSON value into a list of strings."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise AnsibleError("Install playbook returned an invalid string list")
    items = cast(list[object], value)
    strings: list[str] = []
    for item in items:
        if not isinstance(item, str):
            raise AnsibleError(
                "Install playbook returned an invalid string list")
        strings.append(item)
    return strings


__all__ = ["NodeResult", "run_install_playbook"]
