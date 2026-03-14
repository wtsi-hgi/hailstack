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
    ) -> str:
        del ssh_username
        assert isinstance(host, health_module.HealthProbeTarget)
        assert host.name == "master"
        assert host.address == "master"
        assert command == ("systemctl", "is-active", "spark-master")
        return "active\n"

    monkeypatch.setattr(health_module, "_run_ssh_command",
                        fake_run_ssh_command)

    result = asyncio.run(
        health_module.check_service_health(
            hosts=["master"],
            ssh_username="ubuntu",
            services={"master": ["spark-master"]},
        )
    )

    assert result == [
        health_module.ServiceStatus(
            name="spark-master", active=True, node="master")
    ]


def test_check_service_health_reports_inactive_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return active=False when the remote service is stopped."""

    async def fake_run_ssh_command(
        host: object,
        ssh_username: str,
        command: tuple[str, ...],
    ) -> str:
        del ssh_username
        assert isinstance(host, health_module.HealthProbeTarget)
        assert host.name == "master"
        assert host.address == "master"
        assert command == ("systemctl", "is-active", "spark-master")
        return "inactive\n"

    monkeypatch.setattr(health_module, "_run_ssh_command",
                        fake_run_ssh_command)

    result = asyncio.run(
        health_module.check_service_health(
            hosts=["master"],
            ssh_username="ubuntu",
            services={"master": ["spark-master"]},
        )
    )

    assert result == [
        health_module.ServiceStatus(
            name="spark-master", active=False, node="master")
    ]


def test_check_service_health_surfaces_unreachable_host_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an SSHError for the unreachable host while preserving other results."""

    async def fake_run_ssh_command(
        host: object,
        ssh_username: str,
        command: tuple[str, ...],
    ) -> str:
        del ssh_username, command
        assert isinstance(host, health_module.HealthProbeTarget)
        if host.name == "worker-01":
            raise SSHError("Unable to reach node worker-01")
        return "active\n"

    monkeypatch.setattr(health_module, "_run_ssh_command",
                        fake_run_ssh_command)

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
    ) -> str:
        del ssh_username
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

    monkeypatch.setattr(health_module, "_run_ssh_command",
                        fake_run_ssh_command)

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
