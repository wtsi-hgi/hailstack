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

"""Async SSH health probes for cluster services and resource usage."""

import asyncio
from collections.abc import Mapping, Sequence

from pydantic import BaseModel, ConfigDict

from hailstack.errors import SSHError


class HealthProbeTarget(BaseModel):
    """Describe one SSH probe target."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    address: str
    jump_host: str | None = None


class ServiceStatus(BaseModel):
    """Describe one service state observed on one node."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    active: bool
    node: str


class NodeResources(BaseModel):
    """Describe resource usage percentages observed for one node."""

    model_config = ConfigDict(extra="forbid", strict=True)

    hostname: str
    cpu_percent: float
    memory_percent: float
    disk_percent: float


async def check_service_health(
    hosts: Sequence[str],
    ssh_username: str,
    services: Mapping[str, Sequence[str]],
) -> list[ServiceStatus | SSHError]:
    """SSH to each host and collect systemd service activity states."""
    return await check_service_health_targets(
        hosts=_hosts_from_strings(hosts),
        ssh_username=ssh_username,
        services=services,
    )


async def check_service_health_targets(
    hosts: Sequence[HealthProbeTarget],
    ssh_username: str,
    services: Mapping[str, Sequence[str]],
) -> list[ServiceStatus | SSHError]:
    """SSH to structured hosts and collect systemd service activity states."""
    probe_results = await asyncio.gather(
        *[
            _probe_host_services(
                host,
                ssh_username=ssh_username,
                service_names=services.get(host.name, ()),
            )
            for host in hosts
        ],
        return_exceptions=True,
    )
    return _service_results_from_gather(hosts, probe_results)


async def gather_resource_usage(
    hosts: Sequence[str],
    ssh_username: str,
) -> list[NodeResources | SSHError]:
    """SSH to each host and collect CPU, memory, and disk usage."""
    return await gather_resource_usage_targets(
        hosts=_hosts_from_strings(hosts),
        ssh_username=ssh_username,
    )


async def gather_resource_usage_targets(
    hosts: Sequence[HealthProbeTarget],
    ssh_username: str,
) -> list[NodeResources | SSHError]:
    """SSH to structured hosts and collect CPU, memory, and disk usage."""
    probe_results = await asyncio.gather(
        *[_probe_host_resources(host, ssh_username=ssh_username)
          for host in hosts],
        return_exceptions=True,
    )
    return _resource_results_from_gather(hosts, probe_results)


def _hosts_from_strings(hosts: Sequence[str]) -> list[HealthProbeTarget]:
    """Adapt the documented public host-string API to internal SSH targets."""
    return [HealthProbeTarget(name=host, address=host) for host in hosts]


async def _probe_host_services(
    host: HealthProbeTarget,
    *,
    ssh_username: str,
    service_names: Sequence[str],
) -> list[ServiceStatus]:
    """Probe all configured services for one host in parallel."""
    service_outputs = await asyncio.gather(
        *[
            _run_ssh_command(
                host,
                ssh_username,
                ("systemctl", "is-active", service_name),
            )
            for service_name in service_names
        ]
    )
    return [
        ServiceStatus(
            name=service_name,
            active=_parse_service_state(service_output),
            node=host.name,
        )
        for service_name, service_output in zip(
            service_names,
            service_outputs,
            strict=True,
        )
    ]


async def _probe_host_resources(
    host: HealthProbeTarget,
    *,
    ssh_username: str,
) -> NodeResources:
    """Probe resource usage for one host."""
    output = await _run_ssh_command(
        host,
        ssh_username,
        (
            "sh",
            "-lc",
            "top -bn1 | awk '/^%Cpu/ {print 100 - $8}' && "
            "free | awk '/Mem:/ {print ($3/$2)*100}' && "
            "df -P / | awk 'NR==2 {gsub(/%/, \"\", $5); print $5}'",
        ),
    )
    return _parse_resource_output(host, output)


async def _run_ssh_command(
    host: HealthProbeTarget,
    ssh_username: str,
    command: tuple[str, ...],
) -> str:
    """Run one SSH command and return stdout or raise on transport failure."""
    try:
        ssh_command = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            "-o",
            "StrictHostKeyChecking=no",
        ]
        if host.jump_host:
            ssh_command.extend(["-J", f"{ssh_username}@{host.jump_host}"])
        ssh_command.extend([f"{ssh_username}@{host.address}", *command])
        process = await asyncio.create_subprocess_exec(
            *ssh_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as error:
        raise SSHError("SSH CLI not found") from error

    stdout_bytes, stderr_bytes = await process.communicate()
    stdout = stdout_bytes.decode("utf-8")
    stderr = stderr_bytes.decode("utf-8").strip()
    if process.returncode == 255 or _looks_like_ssh_transport_error(stderr):
        raise SSHError(stderr or f"Unable to reach node {host.address}")
    return stdout


def _service_results_from_gather(
    hosts: Sequence[HealthProbeTarget],
    probe_results: Sequence[list[ServiceStatus] | BaseException],
) -> list[ServiceStatus | SSHError]:
    """Flatten gathered service results and preserve per-host SSH failures."""
    services: list[ServiceStatus | SSHError] = []
    for host, result in zip(hosts, probe_results, strict=True):
        if isinstance(result, SSHError):
            services.append(SSHError(f"{host.name}: {result}"))
            continue
        if isinstance(result, BaseException):
            raise result
        services.extend(result)
    return services


def _resource_results_from_gather(
    hosts: Sequence[HealthProbeTarget],
    probe_results: Sequence[NodeResources | BaseException],
) -> list[NodeResources | SSHError]:
    """Collect resource results and preserve per-host SSH failures."""
    resources: list[NodeResources | SSHError] = []
    for host, result in zip(hosts, probe_results, strict=True):
        if isinstance(result, SSHError):
            resources.append(SSHError(f"{host.name}: {result}"))
            continue
        if isinstance(result, BaseException):
            raise result
        resources.append(result)
    return resources


def _parse_service_state(output: str) -> bool:
    """Return whether the service state is active."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return bool(lines) and lines[0] == "active"


def _parse_resource_output(host: HealthProbeTarget, output: str) -> NodeResources:
    """Validate and parse CPU, memory, and disk percentages."""
    values = [line.strip() for line in output.splitlines() if line.strip()]
    if len(values) != 3:
        raise SSHError(f"Unable to parse resource usage for {host.name}")
    cpu_percent = _parse_percent(values[0], host=host)
    memory_percent = _parse_percent(values[1], host=host)
    disk_percent = _parse_percent(values[2], host=host)
    return NodeResources(
        hostname=host.name,
        cpu_percent=cpu_percent,
        memory_percent=memory_percent,
        disk_percent=disk_percent,
    )


def _parse_percent(value: str, *, host: HealthProbeTarget) -> float:
    """Convert one percentage string to a bounded float."""
    try:
        percent = float(value)
    except ValueError as error:
        raise SSHError(
            f"Unable to parse resource usage for {host.name}") from error
    if percent < 0.0 or percent > 100.0:
        raise SSHError(f"Unable to parse resource usage for {host.name}")
    return percent


def _looks_like_ssh_transport_error(stderr: str) -> bool:
    """Return whether stderr indicates an SSH transport failure."""
    lowered = stderr.lower()
    return any(
        fragment in lowered
        for fragment in (
            "connection refused",
            "could not resolve hostname",
            "connection timed out",
            "operation timed out",
            "no route to host",
            "permission denied",
            "connection closed",
            "closed by remote host",
            "connection reset",
        )
    )


__all__ = [
    "NodeResources",
    "ServiceStatus",
    "check_service_health",
    "gather_resource_usage",
]
