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

"""Acceptance tests for the I1 reboot CLI command."""

import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import TypedDict

import pytest
from typer.testing import CliRunner

from hailstack.cli.commands import reboot as reboot_module
from hailstack.cli.main import app
from hailstack.errors import SSHError

runner = CliRunner()


class FakeRebootStackRunner:
    """Return deterministic Pulumi stack outputs for reboot tests."""

    def __init__(self) -> None:
        """Initialise fake outputs for one master and three workers."""
        self.calls = 0

    def get_reboot_outputs(self, config: object) -> dict[str, object]:
        """Return a stable reboot inventory payload."""
        del config
        self.calls += 1
        return {
            "cluster_name": "test-cluster",
            "master_public_ip": "198.51.100.10",
            "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
            "worker_names": [
                "test-cluster-worker-01",
                "test-cluster-worker-02",
                "test-cluster-worker-03",
            ],
        }


class RebootCall(TypedDict):
    """Store one reboot executor invocation for assertions."""

    inventory: list[reboot_module.RebootTarget]
    ssh_username: str
    ssh_key_path: Path | None
    timeout_seconds: float
    backoff_seconds: tuple[float, ...]


class FakeRebootExecutor:
    """Capture reboot requests without performing real SSH operations."""

    def __init__(self) -> None:
        """Initialise recorded call storage."""
        self.calls: list[RebootCall] = []

    def reboot_nodes(
        self,
        inventory: Sequence[reboot_module.RebootTarget],
        *,
        ssh_username: str,
        ssh_key_path: Path | None,
        timeout_seconds: float,
        backoff_seconds: Sequence[float],
    ) -> None:
        """Record each reboot invocation."""
        self.calls.append(
            {
                "inventory": list(inventory),
                "ssh_username": ssh_username,
                "ssh_key_path": ssh_key_path,
                "timeout_seconds": timeout_seconds,
                "backoff_seconds": tuple(backoff_seconds),
            }
        )


class _SSHRebootExecutorHarness(reboot_module.SSHRebootExecutor):
    """Expose protected SSH reboot helpers for focused unit tests."""

    def request_reboot(
        self,
        target: reboot_module.RebootTarget,
        ssh_username: str,
        ssh_key_path: Path | None = None,
    ) -> None:
        """Forward to the protected reboot-dispatch helper."""
        self._request_reboot(target, ssh_username, ssh_key_path)

    def check_connectivity(
        self,
        target: reboot_module.RebootTarget,
        ssh_username: str,
        ssh_key_path: Path | None = None,
    ) -> bool:
        """Forward to the protected connectivity helper."""
        return self._check_connectivity(target, ssh_username, ssh_key_path)


def _write_config(path: Path) -> Path:
    """Write a minimal valid reboot configuration file."""
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            'bundle = "bundle-from-config"\n'
            "num_workers = 3\n"
            'master_flavour = "m2.2xlarge"\n'
            'worker_flavour = "m2.xlarge"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n\n'
            "[ceph_s3]\n"
            'endpoint = "https://ceph.example.invalid"\n'
            'bucket = "hailstack-state"\n'
            'access_key = "state-access"\n'
            'secret_key = "state-secret"\n\n'
            "[ssh_keys]\n"
            'public_keys = ["ssh-ed25519 AAAA primary@test"]\n'
        ),
        encoding="utf-8",
    )
    return path


def _install_fakes(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[FakeRebootStackRunner, FakeRebootExecutor]:
    """Install fake reboot dependencies into the command module."""
    fake_stack_runner = FakeRebootStackRunner()
    fake_executor = FakeRebootExecutor()

    def fake_create_reboot_stack_runner(logger: object) -> FakeRebootStackRunner:
        del logger
        return fake_stack_runner

    def fake_create_reboot_executor(logger: object) -> FakeRebootExecutor:
        del logger
        return fake_executor

    monkeypatch.setattr(
        reboot_module,
        "create_reboot_stack_runner",
        fake_create_reboot_stack_runner,
    )
    monkeypatch.setattr(
        reboot_module,
        "create_reboot_executor",
        fake_create_reboot_executor,
    )
    return fake_stack_runner, fake_executor


def test_reboot_without_node_targets_all_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reboot every worker when no --node selection is provided."""
    config_path = _write_config(tmp_path / "reboot.toml")
    _, fake_executor = _install_fakes(monkeypatch)

    result = runner.invoke(app, ["reboot", "--config", str(config_path)])

    assert result.exit_code == 0
    assert [target.name for target in fake_executor.calls[0]["inventory"]] == [
        "worker-01",
        "worker-02",
        "worker-03",
    ]
    assert fake_executor.calls[0]["ssh_username"] == "ubuntu"
    assert fake_executor.calls[0]["ssh_key_path"] is None
    assert fake_executor.calls[0]["timeout_seconds"] == 300.0
    assert fake_executor.calls[0]["backoff_seconds"] == (1.0, 2.0, 4.0)


def test_reboot_with_node_targets_only_selected_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reboot only the requested worker when --node is provided."""
    config_path = _write_config(tmp_path / "reboot.toml")
    _, fake_executor = _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        ["reboot", "--config", str(config_path), "--node", "worker-01"],
    )

    assert result.exit_code == 0
    assert [target.name for target in fake_executor.calls[0]["inventory"]] == [
        "worker-01"
    ]
    assert fake_executor.calls[0]["inventory"][0].host == "10.0.0.21"
    assert fake_executor.calls[0]["inventory"][0].jump_host == "198.51.100.10"


def test_reboot_passes_explicit_ssh_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thread --ssh-key through the reboot executor."""
    config_path = _write_config(tmp_path / "reboot.toml")
    ssh_key_path = tmp_path / "cluster-key"
    ssh_key_path.write_text("PRIVATE KEY", encoding="utf-8")
    _, fake_executor = _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        [
            "reboot",
            "--config",
            str(config_path),
            "--ssh-key",
            str(ssh_key_path),
        ],
    )

    assert result.exit_code == 0
    assert fake_executor.calls[0]["ssh_key_path"] == ssh_key_path


def test_reboot_with_unknown_worker_reports_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject unknown worker names passed to --node."""
    config_path = _write_config(tmp_path / "reboot.toml")
    _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        ["reboot", "--config", str(config_path), "--node", "worker-99"],
    )

    assert result.exit_code == 2
    assert "Worker not found" in result.stderr


def test_reboot_with_master_reference_reports_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject master references because master reboot is unsupported."""
    config_path = _write_config(tmp_path / "reboot.toml")
    _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        ["reboot", "--config", str(config_path), "--node", "master"],
    )

    assert result.exit_code == 2
    assert "Cannot reboot master node" in result.stderr


def test_ssh_reboot_executor_waits_for_connectivity_to_return() -> None:
    """Wait for an observed disconnect and later SSH recovery within timeout."""
    sleep_calls: list[float] = []
    elapsed = {"now": 0.0}
    connectivity = iter([True, False, False, True])
    boot_markers = iter(["boot-1", "boot-1", "boot-2"])
    requested_reboots: list[str] = []

    executor = reboot_module.SSHRebootExecutor(
        logger=reboot_module.get_reboot_logger(),
        reboot_requester=lambda target, ssh_username, ssh_key_path: (
            requested_reboots.append(f"{ssh_username}@{target.host}")
        ),
        connectivity_checker=lambda target, ssh_username, ssh_key_path: next(
            connectivity
        ),
        boot_marker_reader=lambda target, ssh_username, ssh_key_path: next(
            boot_markers
        ),
        sleeper=lambda seconds: (
            sleep_calls.append(seconds)
            or elapsed.__setitem__(
                "now",
                elapsed["now"] + seconds,
            )
        ),
        clock=lambda: elapsed["now"],
    )

    executor.reboot_nodes(
        [reboot_module.RebootTarget(name="worker-01", host="10.0.0.21")],
        ssh_username="ubuntu",
        timeout_seconds=300.0,
        backoff_seconds=(1.0, 2.0, 4.0),
    )

    assert requested_reboots == ["ubuntu@10.0.0.21"]
    assert sleep_calls == [1.0, 2.0, 4.0]


def test_ssh_reboot_executor_accepts_boot_id_change_without_observed_disconnect() -> (
    None
):
    """Accept recovery when SSH stays up but the boot ID changes."""
    sleep_calls: list[float] = []
    elapsed = {"now": 0.0}
    connectivity = iter([True, True])
    boot_markers = iter(["boot-1", "boot-1", "boot-2"])

    executor = reboot_module.SSHRebootExecutor(
        logger=reboot_module.get_reboot_logger(),
        reboot_requester=lambda target, ssh_username, ssh_key_path: None,
        connectivity_checker=lambda target, ssh_username, ssh_key_path: next(
            connectivity
        ),
        boot_marker_reader=lambda target, ssh_username, ssh_key_path: next(
            boot_markers
        ),
        sleeper=lambda seconds: (
            sleep_calls.append(seconds)
            or elapsed.__setitem__(
                "now",
                elapsed["now"] + seconds,
            )
        ),
        clock=lambda: elapsed["now"],
    )

    executor.reboot_nodes(
        [reboot_module.RebootTarget(name="worker-01", host="10.0.0.21")],
        ssh_username="ubuntu",
        timeout_seconds=300.0,
        backoff_seconds=(1.0, 2.0, 4.0),
    )

    assert sleep_calls == [1.0]


def test_ssh_reboot_executor_times_out_when_connectivity_never_returns() -> None:
    """Raise SSHError when a rebooted worker never becomes reachable again."""
    elapsed = {"now": 0.0}

    executor = reboot_module.SSHRebootExecutor(
        logger=reboot_module.get_reboot_logger(),
        reboot_requester=lambda target, ssh_username, ssh_key_path: None,
        connectivity_checker=lambda target, ssh_username, ssh_key_path: False,
        boot_marker_reader=lambda target, ssh_username, ssh_key_path: "boot-1",
        sleeper=lambda seconds: elapsed.__setitem__(
            "now", elapsed["now"] + seconds),
        clock=lambda: elapsed["now"],
    )

    with pytest.raises(
        SSHError,
        match=(
            "Timed out waiting for SSH connectivity to return for worker-01 "
            "within 300 seconds"
        ),
    ):
        executor.reboot_nodes(
            [reboot_module.RebootTarget(name="worker-01", host="10.0.0.21")],
            ssh_username="ubuntu",
            timeout_seconds=300.0,
            backoff_seconds=(1.0, 2.0, 4.0),
        )


def test_ssh_reboot_executor_times_out_when_boot_id_never_changes() -> None:
    """Raise SSHError when the node stays reachable but never actually reboots."""
    elapsed = {"now": 0.0}

    executor = reboot_module.SSHRebootExecutor(
        logger=reboot_module.get_reboot_logger(),
        reboot_requester=lambda target, ssh_username, ssh_key_path: None,
        connectivity_checker=lambda target, ssh_username, ssh_key_path: True,
        boot_marker_reader=lambda target, ssh_username, ssh_key_path: "boot-1",
        sleeper=lambda seconds: elapsed.__setitem__(
            "now", elapsed["now"] + seconds),
        clock=lambda: elapsed["now"],
    )

    with pytest.raises(
        SSHError,
        match=(
            "Timed out waiting for SSH connectivity to return for worker-01 "
            "within 300 seconds"
        ),
    ):
        executor.reboot_nodes(
            [reboot_module.RebootTarget(name="worker-01", host="10.0.0.21")],
            ssh_username="ubuntu",
            timeout_seconds=300.0,
            backoff_seconds=(1.0, 2.0, 4.0),
        )


def test_ssh_reboot_executor_retries_when_boot_marker_probe_drops() -> None:
    """Keep waiting when the follow-up boot marker probe loses SSH transport."""
    sleep_calls: list[float] = []
    elapsed = {"now": 0.0}
    connectivity = iter([True, True])
    boot_marker_results: list[str | Exception] = [
        "boot-1",
        SSHError("Connection to 10.0.0.21 closed by remote host"),
        "boot-2",
    ]

    def boot_marker_reader(
        target: reboot_module.RebootTarget,
        ssh_username: str,
        ssh_key_path: Path | None,
    ) -> str:
        del target, ssh_username, ssh_key_path
        result = boot_marker_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    executor = reboot_module.SSHRebootExecutor(
        logger=reboot_module.get_reboot_logger(),
        reboot_requester=lambda target, ssh_username, ssh_key_path: None,
        connectivity_checker=lambda target, ssh_username, ssh_key_path: next(
            connectivity
        ),
        boot_marker_reader=boot_marker_reader,
        sleeper=lambda seconds: (
            sleep_calls.append(seconds)
            or elapsed.__setitem__(
                "now",
                elapsed["now"] + seconds,
            )
        ),
        clock=lambda: elapsed["now"],
    )

    executor.reboot_nodes(
        [reboot_module.RebootTarget(name="worker-01", host="10.0.0.21")],
        ssh_username="ubuntu",
        timeout_seconds=300.0,
        backoff_seconds=(1.0, 2.0, 4.0),
    )

    assert sleep_calls == [1.0]


def test_ssh_reboot_executor_surfaces_non_transport_boot_marker_failures() -> None:
    """Raise non-transport boot-marker errors immediately instead of retrying them."""
    elapsed = {"now": 0.0}
    connectivity = iter([True])
    boot_marker_results: list[str | Exception] = [
        "boot-1",
        reboot_module.SSHError("Permission denied reading boot marker"),
    ]

    def boot_marker_reader(
        target: reboot_module.RebootTarget,
        ssh_username: str,
        ssh_key_path: Path | None,
    ) -> str:
        del target, ssh_username, ssh_key_path
        result = boot_marker_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    executor = reboot_module.SSHRebootExecutor(
        logger=reboot_module.get_reboot_logger(),
        reboot_requester=lambda target, ssh_username, ssh_key_path: None,
        connectivity_checker=lambda target, ssh_username, ssh_key_path: next(
            connectivity
        ),
        boot_marker_reader=boot_marker_reader,
        sleeper=lambda seconds: elapsed.__setitem__(
            "now", elapsed["now"] + seconds),
        clock=lambda: elapsed["now"],
    )

    with pytest.raises(reboot_module.SSHError, match="Permission denied"):
        executor.reboot_nodes(
            [reboot_module.RebootTarget(name="worker-01", host="10.0.0.21")],
            ssh_username="ubuntu",
            timeout_seconds=300.0,
            backoff_seconds=(1.0, 2.0, 4.0),
        )


def test_ssh_reboot_executor_tolerates_transport_drop_when_reboot_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat the SSH session closing during reboot dispatch as expected."""
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        commands.append(command)
        return subprocess.CompletedProcess(
            command,
            255,
            stdout="",
            stderr="Connection to 10.0.0.21 closed by remote host",
        )

    monkeypatch.setattr(reboot_module.subprocess, "run", fake_run)

    executor = _SSHRebootExecutorHarness(
        logger=reboot_module.get_reboot_logger())

    executor.request_reboot(
        reboot_module.RebootTarget(
            name="worker-01",
            host="10.0.0.21",
            jump_host="198.51.100.10",
        ),
        "ubuntu",
    )

    assert commands[0][:14] == [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=5",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-J",
        "ubuntu@198.51.100.10",
        "ubuntu@10.0.0.21",
    ]
    assert commands[0][14] == "sudo"


def test_ssh_reboot_executor_raises_when_reboot_cannot_be_dispatched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail immediately when SSH cannot reach the target for reboot dispatch."""

    def fake_run(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del command, kwargs
        return subprocess.CompletedProcess(
            [],
            255,
            stdout="",
            stderr="No route to host",
        )

    monkeypatch.setattr(reboot_module.subprocess, "run", fake_run)

    executor = _SSHRebootExecutorHarness(
        logger=reboot_module.get_reboot_logger())

    with pytest.raises(SSHError, match="Failed to dispatch reboot to worker-01"):
        executor.request_reboot(
            reboot_module.RebootTarget(name="worker-01", host="10.0.0.21"),
            "ubuntu",
        )


def test_ssh_reboot_executor_raises_on_auth_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail fast when SSH authentication fails during connectivity checks."""

    def fake_run(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del command, kwargs
        return subprocess.CompletedProcess(
            [],
            255,
            stdout="",
            stderr="Permission denied (publickey)",
        )

    monkeypatch.setattr(reboot_module.subprocess, "run", fake_run)

    executor = _SSHRebootExecutorHarness(
        logger=reboot_module.get_reboot_logger(),
    )

    with pytest.raises(SSHError, match="Permission denied"):
        executor.check_connectivity(
            reboot_module.RebootTarget(name="worker-01", host="10.0.0.21"),
            "ubuntu",
        )
