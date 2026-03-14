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

"""Unit tests for the install ansible runner."""

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest
import yaml

from hailstack.ansible import runner as runner_module
from hailstack.errors import AnsibleError

type JsonObject = dict[str, object]


@dataclass(frozen=True)
class HostVarsPayload:
    """Represent per-host inventory variables."""

    ansible_host: str
    ansible_user: str


@dataclass(frozen=True)
class InventoryGroupPayload:
    """Represent one Ansible inventory group."""

    hosts: dict[str, HostVarsPayload]


@dataclass(frozen=True)
class InventoryPayload:
    """Represent the typed inventory JSON written by the runner."""

    hosts: dict[str, HostVarsPayload]
    children: dict[str, InventoryGroupPayload]


@dataclass(frozen=True)
class VarsPayload:
    """Represent the typed vars JSON written by the runner."""

    base_venv_path: str
    hailstack_result_path: str
    overlay_venv_path: str
    python_packages: list[str]
    smoke_test: str | None
    software_state_path: str
    system_packages: list[str]


@dataclass
class PlaybookInvocation:
    """Capture one mocked ansible-playbook invocation."""

    command: list[str]
    inventory: InventoryPayload
    vars_payload: VarsPayload


@dataclass
class InvocationCapture:
    """Store the last intercepted ansible-playbook invocation."""

    invocation: PlaybookInvocation | None = None


def _require_dict(value: object, *, context: str) -> JsonObject:
    assert isinstance(value, dict), f"{context} must be an object"
    validated: JsonObject = {}
    raw_dict = cast(dict[object, object], value)
    for key, item in raw_dict.items():
        assert isinstance(key, str), f"{context} keys must be strings"
        validated[key] = item
    return validated


def _require_list(value: object, *, context: str) -> list[object]:
    assert isinstance(value, list), f"{context} must be a list"
    return list(cast(list[object], value))


def _require_str(value: object, *, context: str) -> str:
    assert isinstance(value, str), f"{context} must be a string"
    return value


def _require_bool(value: object, *, context: str) -> bool:
    assert isinstance(value, bool), f"{context} must be a bool"
    return value


def _require_str_list(value: object, *, context: str) -> list[str]:
    return [
        _require_str(item, context=context)
        for item in _require_list(value, context=context)
    ]


def _load_playbook() -> tuple[JsonObject, dict[str, JsonObject]]:
    loaded: object = yaml.safe_load(
        runner_module.PLAYBOOK_PATH.read_text(encoding="utf-8")
    )
    plays = _require_list(loaded, context="install playbook")
    assert len(plays) == 1
    play = _require_dict(plays[0], context="install play")
    task_map: dict[str, JsonObject] = {}
    for index, raw_task in enumerate(_require_list(play["tasks"], context="tasks")):
        task = _require_dict(raw_task, context=f"task {index}")
        name = _require_str(task["name"], context=f"task {index} name")
        task_map[name] = task
    return play, task_map


def _playbook_vars(play: JsonObject) -> JsonObject:
    return _require_dict(play["vars"], context="play vars")


def _task_module(task: JsonObject, module_name: str) -> JsonObject:
    return _require_dict(task[module_name], context=f"{module_name} task body")


def _task_when(task: JsonObject) -> list[str]:
    when = task.get("when")
    if when is None:
        return []
    if isinstance(when, str):
        return [when]
    return _require_str_list(when, context="task when")


def _capture_invocation(command: list[str]) -> PlaybookInvocation:
    vars_path = Path(command[command.index("-e") + 1].removeprefix("@"))
    inventory_path = Path(command[command.index("-i") + 1])
    return PlaybookInvocation(
        command=list(command),
        inventory=_load_inventory_payload(inventory_path),
        vars_payload=_load_vars_payload(vars_path),
    )


def _load_inventory_payload(path: Path) -> InventoryPayload:
    payload = _require_dict(
        json.loads(path.read_text(encoding="utf-8")),
        context="inventory payload",
    )
    all_payload = _require_dict(payload["all"], context="inventory all")
    hosts = _load_host_mapping(all_payload["hosts"], context="inventory hosts")
    children_payload = _require_dict(
        all_payload["children"],
        context="inventory children",
    )
    children: dict[str, InventoryGroupPayload] = {}
    for group_name, raw_group in children_payload.items():
        group_payload = _require_dict(raw_group, context=f"group {group_name}")
        group_hosts = _load_host_mapping(
            group_payload["hosts"],
            context=f"group {group_name} hosts",
        )
        children[group_name] = InventoryGroupPayload(hosts=group_hosts)
    return InventoryPayload(hosts=hosts, children=children)


def _load_host_mapping(value: object, *, context: str) -> dict[str, HostVarsPayload]:
    host_payload = _require_dict(value, context=context)
    hosts: dict[str, HostVarsPayload] = {}
    for hostname, raw_host_vars in host_payload.items():
        host_vars = _require_dict(raw_host_vars, context=f"{context} host vars")
        hosts[hostname] = HostVarsPayload(
            ansible_host=_require_str(
                host_vars["ansible_host"], context="ansible_host"
            ),
            ansible_user=_require_str(
                host_vars["ansible_user"], context="ansible_user"
            ),
        )
    return hosts


def _load_vars_payload(path: Path) -> VarsPayload:
    payload = _require_dict(
        json.loads(path.read_text(encoding="utf-8")),
        context="vars payload",
    )
    smoke_test_value = payload["smoke_test"]
    return VarsPayload(
        base_venv_path=_require_str(
            payload["base_venv_path"], context="base_venv_path"
        ),
        hailstack_result_path=_require_str(
            payload["hailstack_result_path"],
            context="hailstack_result_path",
        ),
        overlay_venv_path=_require_str(
            payload["overlay_venv_path"],
            context="overlay_venv_path",
        ),
        python_packages=_require_str_list(
            payload["python_packages"], context="python_packages"
        ),
        smoke_test=(
            None
            if smoke_test_value is None
            else _require_str(smoke_test_value, context="smoke_test")
        ),
        software_state_path=_require_str(
            payload["software_state_path"],
            context="software_state_path",
        ),
        system_packages=_require_str_list(
            payload["system_packages"], context="system_packages"
        ),
    )


def _node_result_payload(
    hostname: str,
    *,
    success: bool,
    system_installed: list[str],
    python_installed: list[str],
    verification: JsonObject,
    errors: list[str] | None = None,
    changed: bool,
) -> JsonObject:
    return {
        "hostname": hostname,
        "success": success,
        "system_installed": system_installed,
        "python_installed": python_installed,
        "errors": [] if errors is None else errors,
        "verification": verification,
        "changed": changed,
    }


def _expected_node_result(
    hostname: str,
    *,
    success: bool,
    system_installed: list[str],
    python_installed: list[str],
    verification: JsonObject,
    errors: list[str] | None = None,
    changed: bool,
) -> runner_module.NodeResult:
    return runner_module.NodeResult(
        hostname=hostname,
        success=success,
        system_installed=system_installed,
        python_installed=python_installed,
        errors=[] if errors is None else errors,
        verification=verification,
        changed=changed,
    )


def _write_result_lines(result_path: str, payloads: list[JsonObject]) -> None:
    Path(result_path).write_text(
        "\n".join(json.dumps(payload) for payload in payloads),
        encoding="utf-8",
    )


def test_run_install_playbook_installs_system_packages_on_all_inventory_hosts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write the dynamic inventory and install system packages on every host."""
    _, tasks = _load_playbook()
    capture = InvocationCapture()

    def fake_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        capture.invocation = _capture_invocation(command)
        assert capture.invocation.vars_payload.system_packages == ["mc"]
        verification: JsonObject = {
            "system": {"mc": True},
            "smoke_test": True,
            "software_state_updated": True,
        }
        _write_result_lines(
            capture.invocation.vars_payload.hailstack_result_path,
            [
                _node_result_payload(
                    hostname,
                    success=True,
                    system_installed=capture.invocation.vars_payload.system_packages,
                    python_installed=[],
                    verification=verification,
                    changed=True,
                )
                for hostname in capture.invocation.inventory.hosts
            ],
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(runner_module, "_run_playbook_command", fake_run)

    results = runner_module.run_install_playbook(
        inventory={
            "master": ["198.51.100.10"],
            "worker": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
        },
        system_packages=["mc"],
        python_packages=[],
        ssh_username="ubuntu",
        ssh_key_path=Path("/tmp/test-key"),
    )

    assert capture.invocation is not None
    system_install_task = tasks["Install requested system packages"]
    verify_system_task = tasks["Verify requested system packages with dpkg-query"]
    system_install_body = _task_module(system_install_task, "ansible.builtin.apt")
    verify_system_body = _task_module(verify_system_task, "ansible.builtin.command")

    assert capture.invocation.command[:2] == [
        "ansible-playbook",
        str(runner_module.PLAYBOOK_PATH),
    ]
    assert capture.invocation.vars_payload == VarsPayload(
        base_venv_path=runner_module.DEFAULT_BASE_VENV_PATH,
        hailstack_result_path=capture.invocation.vars_payload.hailstack_result_path,
        overlay_venv_path=runner_module.DEFAULT_OVERLAY_VENV_PATH,
        python_packages=[],
        smoke_test=None,
        software_state_path=runner_module.DEFAULT_SOFTWARE_STATE_PATH,
        system_packages=["mc"],
    )
    assert set(capture.invocation.inventory.hosts) == {
        "198.51.100.10",
        "10.0.0.21",
        "10.0.0.22",
        "10.0.0.23",
    }
    assert capture.invocation.inventory.children["master"] == InventoryGroupPayload(
        hosts={
            "198.51.100.10": HostVarsPayload(
                ansible_host="198.51.100.10",
                ansible_user="ubuntu",
            )
        }
    )
    assert capture.invocation.inventory.children["worker"] == InventoryGroupPayload(
        hosts={
            "10.0.0.21": HostVarsPayload(
                ansible_host="10.0.0.21",
                ansible_user="ubuntu",
            ),
            "10.0.0.22": HostVarsPayload(
                ansible_host="10.0.0.22",
                ansible_user="ubuntu",
            ),
            "10.0.0.23": HostVarsPayload(
                ansible_host="10.0.0.23",
                ansible_user="ubuntu",
            ),
        }
    )
    assert (
        _require_str(system_install_body["name"], context="apt name")
        == "{{ system_packages }}"
    )
    assert _require_str(system_install_body["state"], context="apt state") == "present"
    assert _require_bool(
        system_install_body["update_cache"], context="apt update_cache"
    )
    assert _task_when(system_install_task) == ["system_packages | length > 0"]
    assert (
        _require_str(verify_system_task["loop"], context="verify system loop")
        == "{{ system_packages }}"
    )
    assert _require_list(verify_system_body["argv"], context="verify system argv") == [
        "dpkg-query",
        "-W",
        "-f=${Status}",
        "{{ item }}",
    ]
    expected_host_order = list(capture.invocation.inventory.hosts)
    assert results == [
        _expected_node_result(
            hostname,
            success=True,
            system_installed=["mc"],
            python_installed=[],
            verification={
                "system": {"mc": True},
                "smoke_test": True,
                "software_state_updated": True,
            },
            changed=True,
        )
        for hostname in expected_host_order
    ]


def test_run_install_playbook_only_requests_python_installs_when_system_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip system installs and drive the overlay-only Python contract."""
    play, tasks = _load_playbook()
    play_vars = _playbook_vars(play)
    capture = InvocationCapture()

    def fake_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        capture.invocation = _capture_invocation(command)
        assert capture.invocation.vars_payload.system_packages == []
        base_packages = _require_str_list(
            play_vars["hailstack_base_python_packages"],
            context="hailstack_base_python_packages",
        )
        verification: JsonObject = {
            "python": {
                package: True
                for package in capture.invocation.vars_payload.python_packages
            },
            "base_imports": {package: True for package in base_packages},
            "imports": {
                package: True
                for package in capture.invocation.vars_payload.python_packages
            },
            "versions": {
                package: True
                for package in capture.invocation.vars_payload.python_packages
            },
            "smoke_test": True,
            "software_state_updated": True,
        }
        _write_result_lines(
            capture.invocation.vars_payload.hailstack_result_path,
            [
                _node_result_payload(
                    "198.51.100.10",
                    success=True,
                    system_installed=[],
                    python_installed=capture.invocation.vars_payload.python_packages,
                    verification=verification,
                    changed=True,
                )
            ],
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(runner_module, "_run_playbook_command", fake_run)

    results = runner_module.run_install_playbook(
        inventory={"master": ["198.51.100.10"], "worker": []},
        system_packages=[],
        python_packages=["requests"],
        ssh_username="ubuntu",
        ssh_key_path=None,
    )

    assert capture.invocation is not None
    system_install_task = tasks["Install requested system packages"]
    overlay_create_task = tasks["Ensure overlay virtual environment exists"]
    python_install_task = tasks[
        "Install requested Python packages into the overlay venv"
    ]
    smoke_test_task = tasks["Run optional smoke test"]
    import_verify_task = tasks["Verify requested Python imports"]
    version_verify_task = tasks["Verify requested Python package versions"]

    assert capture.invocation.vars_payload == VarsPayload(
        base_venv_path=runner_module.DEFAULT_BASE_VENV_PATH,
        hailstack_result_path=capture.invocation.vars_payload.hailstack_result_path,
        overlay_venv_path=runner_module.DEFAULT_OVERLAY_VENV_PATH,
        python_packages=["requests"],
        smoke_test=None,
        software_state_path=runner_module.DEFAULT_SOFTWARE_STATE_PATH,
        system_packages=[],
    )
    assert "--private-key" not in capture.invocation.command
    assert _task_when(system_install_task) == ["system_packages | length > 0"]
    assert _task_when(overlay_create_task) == ["python_packages | length > 0"]
    assert _task_when(python_install_task) == ["python_packages | length > 0"]
    assert _task_when(smoke_test_task) == ["smoke_test is not none"]
    assert _task_when(import_verify_task) == ["python_packages | length > 0"]
    assert _task_when(version_verify_task) == ["python_packages | length > 0"]
    assert _require_list(
        _task_module(overlay_create_task, "ansible.builtin.command")["argv"],
        context="overlay create argv",
    ) == [
        "{{ base_venv_path }}/bin/python",
        "-m",
        "venv",
        "--system-site-packages",
        "{{ overlay_venv_path }}",
    ]
    assert _require_str(
        _task_module(python_install_task, "ansible.builtin.command")["argv"],
        context="python install argv",
    ) == (
        "{{ [base_venv_path + '/bin/uv', 'pip', 'install', '--python', "
        "overlay_venv_path + '/bin/python'] + python_packages }}"
    )
    assert (
        '. "{{ overlay_venv_path }}/bin/activate" && {{ smoke_test }}'
        in _require_str(
            smoke_test_task["ansible.builtin.shell"],
            context="smoke test shell",
        )
    )
    assert _require_list(
        _task_module(import_verify_task, "ansible.builtin.command")["argv"],
        context="python import verify argv",
    ) == [
        "{{ overlay_venv_path }}/bin/python",
        "-c",
        "{{ hailstack_import_check_script }}",
        "{{ item }}",
    ]
    assert _require_list(
        _task_module(version_verify_task, "ansible.builtin.command")["argv"],
        context="python version verify argv",
    ) == [
        "{{ overlay_venv_path }}/bin/python",
        "-c",
        "{{ hailstack_version_check_script }}",
        "{{ item }}",
    ]
    assert results == [
        _expected_node_result(
            "198.51.100.10",
            success=True,
            system_installed=[],
            python_installed=["requests"],
            verification={
                "python": {"requests": True},
                "base_imports": {"hail": True},
                "imports": {"requests": True},
                "versions": {"requests": True},
                "smoke_test": True,
                "software_state_updated": True,
            },
            changed=True,
        )
    ]


def test_run_install_playbook_returns_node_results_with_hostname_success_and_packages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse one per-host result containing both package categories."""
    capture = InvocationCapture()

    def fake_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        capture.invocation = _capture_invocation(command)
        _write_result_lines(
            capture.invocation.vars_payload.hailstack_result_path,
            [
                _node_result_payload(
                    "198.51.100.10",
                    success=True,
                    system_installed=["mc"],
                    python_installed=["pandas"],
                    verification={
                        "system": {"mc": True},
                        "python": {"pandas": True},
                        "base_imports": {"hail": True},
                        "imports": {"pandas": True},
                        "versions": {"pandas": True},
                        "smoke_test": True,
                        "software_state_updated": True,
                    },
                    changed=True,
                )
            ],
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(runner_module, "_run_playbook_command", fake_run)

    results = runner_module.run_install_playbook(
        inventory={"master": ["198.51.100.10"], "worker": []},
        system_packages=["mc"],
        python_packages=["pandas"],
        ssh_username="ubuntu",
        ssh_key_path=Path("/tmp/test-key"),
        smoke_test="python -c 'import pandas'",
    )

    assert capture.invocation is not None
    assert capture.invocation.vars_payload.smoke_test == "python -c 'import pandas'"
    assert results == [
        _expected_node_result(
            "198.51.100.10",
            success=True,
            system_installed=["mc"],
            python_installed=["pandas"],
            verification={
                "system": {"mc": True},
                "python": {"pandas": True},
                "base_imports": {"hail": True},
                "imports": {"pandas": True},
                "versions": {"pandas": True},
                "smoke_test": True,
                "software_state_updated": True,
            },
            changed=True,
        )
    ]


def test_install_playbook_keeps_base_venv_immutable_and_installs_into_overlay() -> None:
    """Use only read-only base venv commands and write all package state to overlay."""
    play, tasks = _load_playbook()
    play_vars = _playbook_vars(play)

    overlay_create_task = tasks["Ensure overlay virtual environment exists"]
    discover_base_task = tasks["Discover immutable base venv site-packages path"]
    discover_overlay_task = tasks["Discover overlay venv site-packages path"]
    overlay_base_path_task = tasks[
        "Never modify the immutable base environment; expose it via a .pth file"
    ]
    python_install_task = tasks[
        "Install requested Python packages into the overlay venv"
    ]

    assert (
        _require_str(play_vars["base_venv_path"], context="base_venv_path")
        == "/opt/hailstack/base-venv"
    )
    assert (
        _require_str(play_vars["overlay_venv_path"], context="overlay_venv_path")
        == "/opt/hailstack/overlay-venv"
    )
    assert _require_list(
        _task_module(overlay_create_task, "ansible.builtin.command")["argv"],
        context="overlay create argv",
    ) == [
        "{{ base_venv_path }}/bin/python",
        "-m",
        "venv",
        "--system-site-packages",
        "{{ overlay_venv_path }}",
    ]
    assert _require_list(
        _task_module(discover_base_task, "ansible.builtin.command")["argv"],
        context="discover base argv",
    ) == [
        "{{ base_venv_path }}/bin/python",
        "-c",
        "import sysconfig; print(sysconfig.get_path('purelib'))",
    ]
    assert _require_list(
        _task_module(discover_overlay_task, "ansible.builtin.command")["argv"],
        context="discover overlay argv",
    ) == [
        "{{ overlay_venv_path }}/bin/python",
        "-c",
        "import sysconfig; print(sysconfig.get_path('purelib'))",
    ]
    overlay_base_path_body = _task_module(
        overlay_base_path_task, "ansible.builtin.copy"
    )
    assert _require_str(
        overlay_base_path_body["dest"], context="overlay .pth dest"
    ) == ("{{ hailstack_overlay_site_packages.stdout }}/hailstack-base-venv.pth")
    assert (
        _require_str(
            overlay_base_path_body["content"],
            context="overlay .pth content",
        )
        == "{{ hailstack_base_site_packages.stdout }}\n"
    )
    assert _task_when(overlay_base_path_task) == [
        "python_packages | length > 0",
        "hailstack_base_site_packages.rc == 0",
        "hailstack_overlay_site_packages.rc == 0",
    ]
    assert _require_str(
        _task_module(python_install_task, "ansible.builtin.command")["argv"],
        context="python install argv",
    ) == (
        "{{ [base_venv_path + '/bin/uv', 'pip', 'install', '--python', "
        "overlay_venv_path + '/bin/python'] + python_packages }}"
    )


def test_install_playbook_verifies_overlay_imports_from_base_and_overlay() -> None:
    """Verify the playbook checks both inherited base imports and overlay installs."""
    play, tasks = _load_playbook()
    play_vars = _playbook_vars(play)

    base_import_task = tasks["Verify immutable base packages from the overlay venv"]
    requested_import_task = tasks["Verify requested Python imports"]
    import_status_task = tasks["Build Python import verification metadata"]
    base_import_status_task = tasks["Build base package import verification metadata"]
    update_state_task = tasks["Update node-local software state"]
    append_result_task = tasks["Append node result on the controller"]

    assert _require_str_list(
        play_vars["hailstack_base_python_packages"],
        context="hailstack_base_python_packages",
    ) == ["hail"]
    assert _require_dict(
        play_vars["hailstack_python_import_aliases"],
        context="hailstack_python_import_aliases",
    ) == {
        "pyyaml": "yaml",
        "python-dateutil": "dateutil",
        "scikit-image": "skimage",
        "scikit-learn": "sklearn",
    }
    assert _require_str(base_import_task["loop"], context="base import loop") == (
        "{{ hailstack_base_python_packages }}"
    )
    assert (
        _require_str(requested_import_task["loop"], context="requested import loop")
        == "{{ python_packages }}"
    )
    assert _require_list(
        _task_module(base_import_task, "ansible.builtin.command")["argv"],
        context="base import argv",
    ) == [
        "{{ overlay_venv_path }}/bin/python",
        "-c",
        "{{ hailstack_import_check_script }}",
        "{{ item }}",
    ]
    assert _require_list(
        _task_module(requested_import_task, "ansible.builtin.command")["argv"],
        context="requested import argv",
    ) == [
        "{{ overlay_venv_path }}/bin/python",
        "-c",
        "{{ hailstack_import_check_script }}",
        "{{ item }}",
    ]
    update_state_content = _require_str(
        _task_module(update_state_task, "ansible.builtin.copy")["content"],
        context="software state content",
    )
    append_result_line = _require_str(
        _task_module(append_result_task, "ansible.builtin.lineinfile")["line"],
        context="append result line",
    )
    import_check_script = _require_str(
        play_vars["hailstack_import_check_script"],
        context="hailstack_import_check_script",
    )
    import_status_expr = _require_str(
        _task_module(import_status_task, "ansible.builtin.set_fact")[
            "hailstack_import_status"
        ],
        context="hailstack_import_status",
    )
    base_import_status_expr = _require_str(
        _task_module(base_import_status_task, "ansible.builtin.set_fact")[
            "hailstack_base_import_status"
        ],
        context="hailstack_base_import_status",
    )
    assert (
        "aliases = {{ hailstack_python_import_aliases | to_json }}"
        in import_check_script
    )
    assert "import re" in import_check_script
    assert (
        "from importlib.metadata import PackageNotFoundError, distribution"
        in import_check_script
    )
    assert (
        'top_level = distribution(requirement.name).read_text("top_level.txt")'
        in import_check_script
    )
    assert (
        'distribution_name = re.sub(r"[-_.]+", "-", '
        "requirement.name.lower())" in import_check_script
    )
    assert (
        "'base_imports': hailstack_base_import_status | default({})"
        in update_state_content
    )
    assert (
        "'base_imports': hailstack_base_import_status | default({})"
        in append_result_line
    )
    assert "base package import verification failed" in append_result_line
    assert "python import verification failed" in append_result_line
    assert "verification.update({result.item: (result.rc == 0)})" in import_status_expr
    assert (
        "verification.update({result.item: (result.rc == 0)})"
        in base_import_status_expr
    )


def test_install_playbook_repoints_cluster_entrypoints_to_overlay_python() -> None:
    """Rewrite Spark and Jupyter to use the overlay runtime after Python installs."""
    _play, tasks = _load_playbook()

    spark_task = tasks["Ensure Spark uses overlay Python for PySpark"]
    jupyter_task = tasks["Ensure JupyterLab service uses overlay Python"]
    disable_legacy_task = tasks["Disable legacy Hailstack JupyterLab service"]
    restart_task = tasks["Restart JupyterLab after overlay Python update"]

    assert _task_when(spark_task) == ["python_packages | length > 0"]
    assert _task_module(spark_task, "ansible.builtin.lineinfile") == {
        "path": "/etc/spark/conf/spark-defaults.conf",
        "regexp": "^spark\\.pyspark\\.python\\s+",
        "line": "spark.pyspark.python {{ overlay_venv_path }}/bin/python",
    }
    assert _task_when(jupyter_task) == ["python_packages | length > 0"]
    assert "{{ overlay_venv_path }}/bin/python -m jupyterlab" in _require_str(
        _task_module(jupyter_task, "ansible.builtin.copy")["content"],
        context="jupyter overlay unit content",
    )
    assert _task_when(restart_task) == [
        "python_packages | length > 0",
        "'master' in group_names",
    ]
    assert _task_when(disable_legacy_task) == [
        "python_packages | length > 0",
        "'master' in group_names",
    ]
    assert _task_module(disable_legacy_task, "ansible.builtin.systemd_service") == {
        "name": "hailstack-jupyterlab",
        "daemon_reload": True,
        "enabled": False,
        "state": "stopped",
    }
    assert _task_module(restart_task, "ansible.builtin.systemd_service") == {
        "name": "jupyter-lab",
        "daemon_reload": True,
        "enabled": True,
        "state": "restarted",
    }


def test_install_playbook_strips_requirement_extras_and_markers_for_presence() -> None:
    """Normalize PEP 508 extras and markers before matching installed dists."""
    _play, tasks = _load_playbook()

    python_status_task = tasks["Build Python presence verification metadata"]
    python_status_expr = _require_str(
        _task_module(python_status_task, "ansible.builtin.set_fact")[
            "hailstack_python_status"
        ],
        context="hailstack_python_status",
    )

    assert "regex_replace(';.*$', '')" in python_status_expr
    assert "regex_replace('\\[.*\\]', '')" in python_status_expr
    assert "regex_replace('\\s+', '')" in python_status_expr


def test_run_install_playbook_raises_on_nonzero_without_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise an AnsibleError when the playbook fails before emitting results."""

    def fake_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        del command
        return subprocess.CompletedProcess([], 2, stdout="", stderr="permission denied")

    monkeypatch.setattr(runner_module, "_run_playbook_command", fake_run)

    with pytest.raises(AnsibleError, match="permission denied"):
        runner_module.run_install_playbook(
            inventory={"master": ["198.51.100.10"], "worker": []},
            system_packages=["mc"],
            python_packages=[],
            ssh_username="ubuntu",
            ssh_key_path=None,
        )


def test_run_install_playbook_keeps_partial_results_on_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return parsed node results even when ansible exits non-zero."""

    def fake_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        invocation = _capture_invocation(command)
        _write_result_lines(
            invocation.vars_payload.hailstack_result_path,
            [
                _node_result_payload(
                    "198.51.100.10",
                    success=True,
                    system_installed=["mc"],
                    python_installed=[],
                    verification={},
                    changed=False,
                ),
                _node_result_payload(
                    "10.0.0.21",
                    success=False,
                    system_installed=["mc"],
                    python_installed=[],
                    verification={"system": {"mc": False}},
                    errors=["host unreachable"],
                    changed=False,
                ),
            ],
        )
        return subprocess.CompletedProcess(
            command,
            2,
            stdout="",
            stderr="one host failed",
        )

    monkeypatch.setattr(runner_module, "_run_playbook_command", fake_run)

    results = runner_module.run_install_playbook(
        inventory={"master": ["198.51.100.10"], "worker": ["10.0.0.21"]},
        system_packages=["mc"],
        python_packages=[],
        ssh_username="ubuntu",
        ssh_key_path=None,
    )

    assert [result.hostname for result in results] == ["198.51.100.10", "10.0.0.21"]
    assert [result.success for result in results] == [True, False]
