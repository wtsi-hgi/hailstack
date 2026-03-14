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

"""Show cluster summary and health information."""

import asyncio
import json
import logging
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Annotated, Protocol, cast

import typer

from hailstack.config.parser import load_config
from hailstack.config.schema import ClusterConfig
from hailstack.errors import PulumiError, SSHError
from hailstack.pulumi.stack import REPOSITORY_ROOT, AutomationStackRunner
from hailstack.ssh.health import (
    HealthProbeTarget,
    check_service_health_targets,
    gather_resource_usage_targets,
)

_MASTER_SERVICE_NAMES = (
    "spark-master",
    "hdfs-namenode",
    "yarn-rm",
    "hailstack-jupyterlab",
    "spark-history-server",
    "nginx",
    "mapred-history",
)
_WORKER_SERVICE_NAMES = (
    "spark-worker",
    "hdfs-datanode",
    "yarn-nm",
)
_SERVICE_STATUS_ORDER = {"active": 0, "inactive": 1, "unreachable": 2}


@dataclass(frozen=True)
class ServiceStatus:
    """Represent one service status observed on one node."""

    name: str
    status: str
    node: str


@dataclass(frozen=True)
class NodeResources:
    """Represent resource usage observed for one node."""

    node: str
    status: str
    cpu_percent: float | None
    memory_percent: float | None
    disk_percent: float | None


@dataclass(frozen=True)
class DetailedClusterStatus:
    """Represent detailed SSH-derived cluster status information."""

    services: list[ServiceStatus]
    resources: list[NodeResources]


@dataclass(frozen=True)
class StatusWorker:
    """Represent one worker in the summary output."""

    name: str
    ip: str


@dataclass(frozen=True)
class VolumeStatus:
    """Represent the configured shared volume summary."""

    name: str
    size_gb: int | None


@dataclass(frozen=True)
class ClusterSummary:
    """Represent the command's summary view of cluster state."""

    cluster_name: str
    bundle: str
    master_name: str
    master_ip: str
    master_flavour: str
    workers: list[StatusWorker]
    volume: VolumeStatus | None


@dataclass(frozen=True)
class StatusNode:
    """Represent one SSH probe target for detailed status."""

    name: str
    host: str
    role: str
    services: tuple[str, ...]
    jump_host: str | None = None


class StatusStackRunner(Protocol):
    """Define the Pulumi stack lookup used by the status command."""

    def get_status_outputs(self, config: ClusterConfig) -> Mapping[str, object]:
        """Return the outputs required to render cluster status."""
        ...


class StatusProbe(Protocol):
    """Define the detailed SSH probe seam used by the status command."""

    def probe(
        self,
        inventory: Sequence[StatusNode],
        *,
        ssh_username: str,
    ) -> DetailedClusterStatus:
        """Return service and resource state for the provided inventory."""
        ...


class PulumiStatusStackRunner:
    """Resolve status inputs from Pulumi stack outputs."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initialise the runner with a shared logger."""
        self._logger = logger
        self._automation_runner = AutomationStackRunner(logger)

    def get_status_outputs(self, config: ClusterConfig) -> Mapping[str, object]:
        """Return JSON stack outputs for the configured cluster."""
        self._automation_runner.check_backend_access(config)
        try:
            result = subprocess.run(
                [
                    "pulumi",
                    "stack",
                    "output",
                    "--json",
                    "--stack",
                    f"hailstack-{config.cluster.name}",
                ],
                capture_output=True,
                check=False,
                cwd=REPOSITORY_ROOT,
                env=_pulumi_env(config),
                text=True,
            )
        except FileNotFoundError as error:
            raise PulumiError("Pulumi CLI not found") from error

        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            if _is_cluster_not_found(detail):
                raise PulumiError("Cluster not found")
            raise PulumiError(f"Unable to read Pulumi stack outputs: {detail}")

        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise PulumiError(
                "Pulumi stack output was not valid JSON") from error

        if not isinstance(payload, dict):
            raise PulumiError("Pulumi stack output must be a JSON object")

        return cast(Mapping[str, object], payload)


class SSHStatusProbe:
    """Collect detailed service and resource status over SSH."""

    def probe(
        self,
        inventory: Sequence[StatusNode],
        *,
        ssh_username: str,
    ) -> DetailedClusterStatus:
        """Probe the requested nodes and return structured health details."""
        return asyncio.run(self._probe_details(inventory, ssh_username=ssh_username))

    async def _probe_details(
        self,
        inventory: Sequence[StatusNode],
        *,
        ssh_username: str,
    ) -> DetailedClusterStatus:
        """Probe services and resources concurrently via the ssh health module."""
        hosts = [
            HealthProbeTarget(
                name=node.name,
                address=node.host,
                jump_host=node.jump_host,
            )
            for node in inventory
        ]
        services_by_node = {node.name: node.services for node in inventory}
        service_results, resource_results = await asyncio.gather(
            check_service_health_targets(
                hosts=hosts,
                ssh_username=ssh_username,
                services=services_by_node,
            ),
            gather_resource_usage_targets(
                hosts=hosts,
                ssh_username=ssh_username,
            ),
        )

        unreachable_service_nodes = _unreachable_nodes(service_results)
        services = [
            ServiceStatus(
                name=service.name,
                status="active" if service.active else "inactive",
                node=service.node,
            )
            for service in service_results
            if not isinstance(service, SSHError)
        ]
        services.extend(
            ServiceStatus(name=service, status="unreachable", node=node.name)
            for node in inventory
            if node.name in unreachable_service_nodes
            for service in node.services
        )

        unreachable_resource_nodes = _unreachable_nodes(resource_results)
        resources = [
            NodeResources(
                node=resource.hostname,
                status="ok",
                cpu_percent=resource.cpu_percent,
                memory_percent=resource.memory_percent,
                disk_percent=resource.disk_percent,
            )
            for resource in resource_results
            if not isinstance(resource, SSHError)
        ]
        resources.extend(
            NodeResources(
                node=node_name,
                status="unreachable",
                cpu_percent=None,
                memory_percent=None,
                disk_percent=None,
            )
            for node_name in unreachable_resource_nodes
        )

        return DetailedClusterStatus(
            services=sorted(services, key=_service_sort_key),
            resources=sorted(resources, key=_resource_sort_key),
        )


def _unreachable_nodes(
    results: Sequence[object],
) -> set[str]:
    """Extract unreachable hostnames from per-host SSHError placeholders."""
    unreachable: set[str] = set()
    for result in results:
        if not isinstance(result, SSHError):
            continue
        node_name, _, _message = str(result).partition(":")
        unreachable.add(node_name)
    return unreachable


def get_status_logger() -> logging.Logger:
    """Return a dedicated stderr logger for status progress messages."""
    logger = logging.getLogger("hailstack.status")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def create_status_stack_runner(logger: logging.Logger) -> StatusStackRunner:
    """Create the default Pulumi output resolver for status."""
    return PulumiStatusStackRunner(logger)


def create_status_probe() -> StatusProbe:
    """Create the default detailed SSH probe implementation."""
    return SSHStatusProbe()


def _pulumi_env(config: ClusterConfig) -> dict[str, str]:
    """Build the environment required for Pulumi backend access."""
    env = dict(os.environ)
    env["AWS_ACCESS_KEY_ID"] = config.ceph_s3.access_key
    env["AWS_SECRET_ACCESS_KEY"] = config.ceph_s3.secret_key
    return env


def _is_cluster_not_found(detail: str) -> bool:
    """Report whether Pulumi stderr indicates the stack is absent."""
    lowered = detail.lower()
    return any(
        fragment in lowered
        for fragment in ("no stack named", "stack not found", "does not exist")
    )


def _resolve_summary(
    outputs: Mapping[str, object],
    config: ClusterConfig,
) -> ClusterSummary:
    """Build the summary view from Pulumi outputs and static config."""
    cluster_name = _require_output_str(
        outputs,
        "cluster_name",
        default=config.cluster.name,
    )
    bundle = _require_output_str(
        outputs,
        "bundle_id",
        default=config.cluster.bundle,
    )
    master_ip = _require_output_str(
        outputs,
        "master_public_ip",
        default=_require_output_str(outputs, "master_private_ip"),
    )
    worker_names = _require_output_str_list(outputs, "worker_names")
    worker_ips = _require_output_str_list(outputs, "worker_private_ips")
    if len(worker_names) != len(worker_ips):
        raise PulumiError(
            "Pulumi stack outputs contain mismatched worker names and IPs"
        )

    workers = [
        StatusWorker(name=_display_name(
            cluster_name, worker_name), ip=worker_ip)
        for worker_name, worker_ip in zip(worker_names, worker_ips, strict=True)
    ]
    return ClusterSummary(
        cluster_name=cluster_name,
        bundle=bundle,
        master_name="master",
        master_ip=master_ip,
        master_flavour=config.cluster.master_flavour,
        workers=workers,
        volume=_resolve_volume(config),
    )


def _resolve_inventory(
    summary: ClusterSummary,
    config: ClusterConfig,
    *,
    master_jump_host: str | None,
) -> list[StatusNode]:
    """Build the detailed-probe inventory from summary data."""
    inventory = [
        StatusNode(
            name=summary.master_name,
            host=summary.master_ip,
            role="master",
            services=_master_services(config),
        )
    ]
    inventory.extend(
        StatusNode(
            name=worker.name,
            host=worker.ip,
            role="worker",
            services=_worker_services(config),
            jump_host=master_jump_host,
        )
        for worker in summary.workers
    )
    return inventory


def _optional_output_str(outputs: Mapping[str, object], key: str) -> str | None:
    """Extract an optional string output and return None when absent."""
    value = outputs.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise PulumiError(
            f"Pulumi stack output '{key}' was missing or invalid")
    return value


def _master_services(config: ClusterConfig) -> tuple[str, ...]:
    """Return the master services to inspect for detailed status."""
    services: list[str] = list(_MASTER_SERVICE_NAMES)
    if config.cluster.monitoring == "netdata":
        services.append("netdata")
    if config.volumes.create or bool(config.volumes.existing_volume_id.strip()):
        services.append("nfs-server")
    return tuple(services)


def _worker_services(config: ClusterConfig) -> tuple[str, ...]:
    """Return the worker services to inspect for detailed status."""
    services: list[str] = list(_WORKER_SERVICE_NAMES)
    if config.cluster.monitoring == "netdata":
        services.append("netdata")
    return tuple(services)


def _resolve_volume(config: ClusterConfig) -> VolumeStatus | None:
    """Return the configured shared-volume summary when one is attached."""
    if config.volumes.create:
        name = config.volumes.name.strip() or f"{config.cluster.name}-vol"
        return VolumeStatus(name=name, size_gb=config.volumes.size_gb)
    if config.volumes.existing_volume_id.strip():
        name = config.volumes.name.strip() or config.volumes.existing_volume_id.strip()
        return VolumeStatus(name=name, size_gb=None)
    return None


def _require_output_str(
    outputs: Mapping[str, object],
    key: str,
    *,
    default: str | None = None,
) -> str:
    """Extract a string output or raise a PulumiError."""
    value = outputs.get(key)
    if value is None:
        value = default
    if not isinstance(value, str) or not value.strip():
        raise PulumiError(
            f"Pulumi stack output '{key}' was missing or invalid")
    return value.strip()


def _require_output_str_list(outputs: Mapping[str, object], key: str) -> list[str]:
    """Extract a list of strings from stack outputs."""
    value = outputs.get(key)
    if not isinstance(value, list):
        raise PulumiError(
            f"Pulumi stack output '{key}' was missing or invalid")
    items = cast(list[object], value)
    strings: list[str] = []
    for item in items:
        if not isinstance(item, str) or not item.strip():
            raise PulumiError(
                f"Pulumi stack output '{key}' was missing or invalid")
        strings.append(item.strip())
    return strings


def _display_name(cluster_name: str, raw_name: str) -> str:
    """Reduce full resource names to the documented human-readable node labels."""
    prefix = f"{cluster_name}-"
    if raw_name.startswith(prefix):
        return raw_name.removeprefix(prefix)
    return raw_name


def _service_sort_key(service: ServiceStatus) -> tuple[str, int, str]:
    """Return a stable sort key for service entries."""
    return (
        service.name,
        _SERVICE_STATUS_ORDER.get(service.status, len(_SERVICE_STATUS_ORDER)),
        service.node,
    )


def _resource_sort_key(resource: NodeResources) -> tuple[int, str]:
    """Return a stable sort key for node resources."""
    return (0, resource.node) if resource.node == "master" else (1, resource.node)


def _group_services(
    services: Sequence[ServiceStatus],
) -> list[dict[str, object]]:
    """Group service rows by service name and status for rendering and JSON."""
    grouped: dict[tuple[str, str], list[str]] = {}
    for service in services:
        key = (service.name, service.status)
        grouped.setdefault(key, []).append(service.node)
    rows: list[dict[str, object]] = [
        {
            "name": name,
            "status": status,
            "nodes": sorted(nodes, key=_node_name_sort_key),
        }
        for (name, status), nodes in grouped.items()
    ]
    return sorted(
        rows,
        key=lambda row: (
            cast(str, row["name"]),
            _SERVICE_STATUS_ORDER.get(
                cast(str, row["status"]),
                len(_SERVICE_STATUS_ORDER),
            ),
        ),
    )


def _node_name_sort_key(node_name: str) -> tuple[int, str]:
    """Return a stable sort key for display node names."""
    return (0, node_name) if node_name == "master" else (1, node_name)


def _format_summary(summary: ClusterSummary) -> str:
    """Render the human-readable summary block."""
    lines = [
        f"Cluster: {summary.cluster_name}",
        f"Bundle:  {summary.bundle}",
        f"Master:  {summary.master_ip} ({summary.master_flavour})",
        f"Workers: {len(summary.workers)}",
    ]
    lines.extend(f"  {worker.name}: {worker.ip}" for worker in summary.workers)
    if summary.volume is not None:
        volume_line = f"Volume:  {summary.volume.name}"
        if summary.volume.size_gb is not None:
            volume_line += f" ({summary.volume.size_gb}GB)"
        lines.append(volume_line)
    return "\n".join(lines)


def _format_detailed(details: DetailedClusterStatus) -> str:
    """Render the human-readable detailed services and resources blocks."""
    service_rows = _group_services(details.services)
    lines = ["Services:"]
    current_service = ""
    current_parts: list[str] = []
    for row in service_rows:
        name = cast(str, row["name"])
        status = cast(str, row["status"])
        nodes = cast(list[str], row["nodes"])
        part = f"{status} ({', '.join(nodes)})"
        if name != current_service and current_service:
            lines.append(
                f"  {current_service + ':':<22} {'; '.join(current_parts)}")
            current_parts = []
        current_service = name
        current_parts.append(part)
    if current_service:
        lines.append(
            f"  {current_service + ':':<22} {'; '.join(current_parts)}")

    lines.append("")
    lines.append("Resources:")
    for resource in sorted(details.resources, key=_resource_sort_key):
        if resource.status == "unreachable":
            lines.append(f"  {resource.node}: unreachable")
            continue
        lines.append(
            "  "
            + f"{resource.node + ':':<10} "
            + f"CPU {_format_percent(resource.cpu_percent)}  "
            + f"MEM {_format_percent(resource.memory_percent)}  "
            + f"DISK {_format_percent(resource.disk_percent)}"
        )
    return "\n".join(lines)


def _format_percent(value: float | None) -> str:
    """Render a percentage in the documented human-readable form."""
    if value is None:
        return "-"
    if value.is_integer():
        return f"{int(value)}%"
    return f"{value:g}%"


def _json_payload(
    summary: ClusterSummary,
    details: DetailedClusterStatus | None,
) -> dict[str, object]:
    """Build the machine-readable JSON payload for the command."""
    payload: dict[str, object] = {
        "cluster_name": summary.cluster_name,
        "bundle": summary.bundle,
        "master": {
            "name": summary.master_name,
            "ip": summary.master_ip,
            "flavour": summary.master_flavour,
        },
        "worker_count": len(summary.workers),
        "workers": [asdict(worker) for worker in summary.workers],
        "volume": None if summary.volume is None else _volume_payload(summary.volume),
    }
    if details is not None:
        payload["services"] = _group_services(details.services)
        payload["resources"] = [
            asdict(resource)
            for resource in sorted(details.resources, key=_resource_sort_key)
        ]
    return payload


def _volume_payload(volume: VolumeStatus) -> dict[str, object]:
    """Return the JSON payload for a configured volume summary."""
    payload: dict[str, object] = {"name": volume.name}
    if volume.size_gb is not None:
        payload["size_gb"] = volume.size_gb
    return payload


def status(
    config: Annotated[
        Path,
        typer.Option(
            "--config", help="Path to cluster configuration TOML file."),
    ] = Path("./hailstack.toml"),
    detailed: Annotated[
        bool,
        typer.Option(
            "--detailed", help="Include SSH health probes and resources."),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="JSON output."),
    ] = False,
    dotenv: Annotated[
        Path | None,
        typer.Option(
            "--dotenv",
            help="Load environment variables from a .env file before parsing config.",
        ),
    ] = None,
) -> None:
    """Show cluster status."""
    logger = get_status_logger()
    loaded_config = load_config(config, dotenv)
    logger.info("config loaded")

    stack_runner = create_status_stack_runner(logger)
    logger.info("resolving Pulumi outputs")
    outputs = stack_runner.get_status_outputs(loaded_config)
    summary = _resolve_summary(outputs, loaded_config)

    details: DetailedClusterStatus | None = None
    if detailed:
        logger.info("probing cluster health")
        details = create_status_probe().probe(
            _resolve_inventory(
                summary,
                loaded_config,
                master_jump_host=_optional_output_str(
                    outputs, "master_public_ip"),
            ),
            ssh_username=loaded_config.cluster.ssh_username,
        )

    if json_output:
        typer.echo(json.dumps(_json_payload(summary, details), sort_keys=True))
        return

    output = _format_summary(summary)
    if details is not None:
        output = output + "\n\n" + _format_detailed(details)
    typer.echo(output)


status_command = status

__all__ = [
    "DetailedClusterStatus",
    "NodeResources",
    "PulumiStatusStackRunner",
    "SSHStatusProbe",
    "ServiceStatus",
    "StatusProbe",
    "StatusStackRunner",
    "create_status_probe",
    "create_status_stack_runner",
    "get_status_logger",
    "status",
    "status_command",
]
