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

"""Acceptance tests for the H2 SSH health probe module."""

import asyncio
from pathlib import Path

import pytest

from hailstack.errors import SSHError
from hailstack.ssh import health as health_module


def test_check_service_health_reports_active_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an active service status when systemctl reports active."""

    async def fake_run_ssh_command(
        host: object,
        ssh_username: str,
        command: tuple[str, ...],
        *,
        allowed_returncodes: tuple[int, ...] = (),
        ssh_key_path: Path | None = None,
    ) -> str:
        del ssh_username, ssh_key_path
        assert isinstance(host, health_module.HealthProbeTarget)
        assert host.name == "master"
        assert host.address == "master"
        assert command == ("systemctl", "is-active", "spark-master")
        assert allowed_returncodes == (3,)
        return "active\n"

    monkeypatch.setattr(health_module, "_run_ssh_command", fake_run_ssh_command)

    result = asyncio.run(
        health_module.check_service_health(
            hosts=["master"],
            ssh_username="ubuntu",
            services={"master": ["spark-master"]},
        )
    )

    assert result == [
        health_module.ServiceStatus(name="spark-master", active=True, node="master")
    ]


def test_check_service_health_reports_inactive_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return active=False when the remote service is stopped."""

    async def fake_run_ssh_command(
        host: object,
        ssh_username: str,
        command: tuple[str, ...],
        *,
        allowed_returncodes: tuple[int, ...] = (),
        ssh_key_path: Path | None = None,
    ) -> str:
        del ssh_username, ssh_key_path
        assert isinstance(host, health_module.HealthProbeTarget)
        assert host.name == "master"
        assert host.address == "master"
        assert command == ("systemctl", "is-active", "spark-master")
        assert allowed_returncodes == (3,)
        return "inactive\n"

    monkeypatch.setattr(health_module, "_run_ssh_command", fake_run_ssh_command)

    result = asyncio.run(
        health_module.check_service_health(
            hosts=["master"],
            ssh_username="ubuntu",
            services={"master": ["spark-master"]},
        )
    )

    assert result == [
        health_module.ServiceStatus(name="spark-master", active=False, node="master")
    ]


def test_check_service_health_surfaces_unreachable_host_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an SSHError for the unreachable host while preserving other results."""

    async def fake_run_ssh_command(
        host: object,
        ssh_username: str,
        command: tuple[str, ...],
        *,
        allowed_returncodes: tuple[int, ...] = (),
        ssh_key_path: Path | None = None,
    ) -> str:
        del ssh_username, command, ssh_key_path, allowed_returncodes
        assert isinstance(host, health_module.HealthProbeTarget)
        if host.name == "worker-01":
            raise SSHError("Unable to reach node worker-01")
        return "active\n"

    monkeypatch.setattr(health_module, "_run_ssh_command", fake_run_ssh_command)

    result = asyncio.run(
        health_module.check_service_health(
            hosts=["master", "worker-01"],
            ssh_username="ubuntu",
            services={
                "master": ["spark-master"],
                "worker-01": ["spark-worker"],
            },
        )
    )

    assert result[0] == health_module.ServiceStatus(
        name="spark-master",
        active=True,
        node="master",
    )
    assert isinstance(result[1], SSHError)
    assert str(result[1]) == "worker-01: Unable to reach node worker-01"


def test_gather_resource_usage_returns_float_percentages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse CPU, memory, and disk usage as float percentages."""

    async def fake_run_ssh_command(
        host: object,
        ssh_username: str,
        command: tuple[str, ...],
        *,
        ssh_key_path: Path | None = None,
    ) -> str:
        del ssh_username, ssh_key_path
        assert isinstance(host, health_module.HealthProbeTarget)
        assert host.name == "master"
        assert host.address == "master"
        assert command == (
            "sh",
            "-lc",
            "top -bn1 | awk '/^%Cpu/ {print 100 - $8}' && "
            "free | awk '/Mem:/ {print ($3/$2)*100}' && "
            "df -P / | awk 'NR==2 {gsub(/%/, \"\", $5); print $5}'",
        )
        return "23\n45.5\n12\n"

    monkeypatch.setattr(health_module, "_run_ssh_command", fake_run_ssh_command)

    result = asyncio.run(
        health_module.gather_resource_usage(
            hosts=["master"],
            ssh_username="ubuntu",
        )
    )

    assert result == [
        health_module.NodeResources(
            hostname="master",
            cpu_percent=23.0,
            memory_percent=45.5,
            disk_percent=12.0,
        )
    ]


def test_run_ssh_command_surfaces_auth_failures_without_relabeling_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve SSH auth failures so callers can report credential problems clearly."""

    class FakeProcess:
        returncode = 255

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"Permission denied (publickey)"

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return FakeProcess()

    monkeypatch.setattr(
        health_module.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    with pytest.raises(SSHError, match="Permission denied"):
        asyncio.run(
            health_module._run_ssh_command(
                health_module.HealthProbeTarget(name="master", address="master"),
                "ubuntu",
                ("true",),
            )
        )


def test_run_ssh_command_passes_explicit_ssh_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Add -i when callers provide an explicit SSH private key path."""

    captured_args: list[object] = []

    class FakeProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok\n", b""

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> object:
        del kwargs
        captured_args.extend(args)
        return FakeProcess()

    monkeypatch.setattr(
        health_module.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    ssh_key_path = tmp_path / "cluster-key"
    ssh_key_path.write_text("PRIVATE KEY", encoding="utf-8")

    result = asyncio.run(
        health_module._run_ssh_command(
            health_module.HealthProbeTarget(name="master", address="master"),
            "ubuntu",
            ("true",),
            ssh_key_path=ssh_key_path,
        )
    )

    assert result == "ok\n"
    assert captured_args[:9] == [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=5",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
    ]
    assert "-i" in captured_args
    assert str(ssh_key_path) in captured_args


def test_run_ssh_command_allows_expected_non_zero_exit_codes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permit callers to accept command-specific non-zero statuses like inactive."""

    class FakeProcess:
        returncode = 3

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"inactive\n", b""

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return FakeProcess()

    monkeypatch.setattr(
        health_module.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    result = asyncio.run(
        health_module._run_ssh_command(
            health_module.HealthProbeTarget(name="master", address="master"),
            "ubuntu",
            ("systemctl", "is-active", "spark-master"),
            allowed_returncodes=(3,),
        )
    )

    assert result == "inactive\n"
