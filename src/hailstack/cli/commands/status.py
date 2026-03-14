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
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Annotated, Protocol, cast

import typer

from hailstack.config.parser import load_config
from hailstack.config.schema import ClusterConfig
from hailstack.errors import PulumiError, SSHError
from hailstack.pulumi.stack import AutomationStackRunner
from hailstack.ssh.health import (
    HealthProbeTarget,
    check_service_health_targets,
    gather_resource_usage_targets,
)

_MASTER_SERVICE_NAMES = (
    "spark-master",
    "hdfs-namenode",
    "yarn-rm",
    "jupyter-lab",
    "spark-history-server",
    "nginx",
    "mapred-history",
)
_WORKER_SERVICE_NAMES = (
    "spark-worker",
    "hdfs-datanode",
    "yarn-nm",
)
_SERVICE_NAME_ALIASES = {
    "jupyter-lab": ("jupyter-lab", "hailstack-jupyterlab"),
}
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
        ssh_key_path: Path | None = None,
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
            return self._automation_runner.current_stack_outputs(config)
        except PulumiError as error:
            if _is_cluster_not_found(str(error)):
                raise PulumiError("Cluster not found") from error
            raise


class SSHStatusProbe:
    """Collect detailed service and resource status over SSH."""

    def probe(
        self,
        inventory: Sequence[StatusNode],
        *,
        ssh_username: str,
        ssh_key_path: Path | None = None,
    ) -> DetailedClusterStatus:
        """Probe the requested nodes and return structured health details."""
        return asyncio.run(
            self._probe_details(
                inventory,
                ssh_username=ssh_username,
                ssh_key_path=ssh_key_path,
            )
        )

    async def _probe_details(
        self,
        inventory: Sequence[StatusNode],
        *,
        ssh_username: str,
        ssh_key_path: Path | None,
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
                ssh_key_path=ssh_key_path,
            ),
            gather_resource_usage_targets(
                hosts=hosts,
                ssh_username=ssh_username,
                ssh_key_path=ssh_key_path,
            ),
        )

        _raise_for_non_transport_ssh_errors(service_results)
        _raise_for_non_transport_ssh_errors(resource_results)

        unreachable_service_nodes = _unreachable_nodes(service_results)
        services = [
            ServiceStatus(
                name=_canonical_service_name(service.name),
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
            for service in _display_service_names(node.services)
        )
        services = _normalize_service_statuses(services)

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
        if not isinstance(result, SSHError) or not _is_transport_ssh_error(result):
            continue
        node_name, _, _message = str(result).partition(":")
        unreachable.add(node_name)
    return unreachable


def _raise_for_non_transport_ssh_errors(results: Sequence[object]) -> None:
    """Propagate SSH failures that are not plain transport outages."""
    for result in results:
        if isinstance(result, SSHError) and not _is_transport_ssh_error(result):
            raise result


def _is_transport_ssh_error(error: SSHError) -> bool:
    """Report whether an SSH error should be rendered as an unreachable node."""
    lowered = str(error).lower()
    return any(
        fragment in lowered
        for fragment in (
            "unable to reach node",
            "connection refused",
            "could not resolve hostname",
            "connection timed out",
            "operation timed out",
            "no route to host",
            "connection closed",
            "closed by remote host",
            "connection reset",
        )
    )


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
    master_ip = _optional_output_str(outputs, "master_public_ip")
    if master_ip is None:
        master_ip = _require_output_str(outputs, "master_private_ip")
    worker_names = _require_output_str_list(outputs, "worker_names")
    worker_ips = _require_output_str_list(outputs, "worker_private_ips")
    if len(worker_names) != len(worker_ips):
        raise PulumiError(
            "Pulumi stack outputs contain mismatched worker names and IPs"
        )

    workers = [
        StatusWorker(name=_display_name(cluster_name, worker_name), ip=worker_ip)
        for worker_name, worker_ip in zip(worker_names, worker_ips, strict=True)
    ]
    return ClusterSummary(
        cluster_name=cluster_name,
        bundle=bundle,
        master_name="master",
        master_ip=master_ip,
        master_flavour=_require_output_str(
            outputs,
            "master_flavour",
            default=config.cluster.master_flavour,
        ),
        workers=workers,
        volume=_resolve_volume(outputs, config),
    )


def _resolve_inventory(
    summary: ClusterSummary,
    outputs: Mapping[str, object],
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
            services=_master_services(outputs, config),
        )
    ]
    inventory.extend(
        StatusNode(
            name=worker.name,
            host=worker.ip,
            role="worker",
            services=_worker_services(outputs, config),
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
        raise PulumiError(f"Pulumi stack output '{key}' was missing or invalid")
    return value.strip()


def _optional_output_int(outputs: Mapping[str, object], key: str) -> int | None:
    """Extract an optional integer output and return None when absent."""
    value = outputs.get(key)
    if value is None:
        return None
    if not isinstance(value, int):
        raise PulumiError(f"Pulumi stack output '{key}' was missing or invalid")
    return value


def _optional_output_bool(outputs: Mapping[str, object], key: str) -> bool | None:
    """Extract an optional boolean output and return None when absent."""
    value = outputs.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise PulumiError(f"Pulumi stack output '{key}' was missing or invalid")
    return value


def _monitoring_enabled(outputs: Mapping[str, object], config: ClusterConfig) -> bool:
    """Return the deployed monitoring state, falling back to config when absent."""
    resolved = _optional_output_bool(outputs, "monitoring_enabled")
    if resolved is not None:
        return resolved
    return config.cluster.monitoring == "netdata"


def _volume_enabled(outputs: Mapping[str, object], config: ClusterConfig) -> bool:
    """Return whether the deployed cluster has a shared volume attached."""
    if _optional_output_str(outputs, "attached_volume_name") is not None:
        return True
    if _has_deployed_volume_metadata(outputs):
        return False
    return _resolve_configured_volume(config) is not None


def _master_services(
    outputs: Mapping[str, object],
    config: ClusterConfig,
) -> tuple[str, ...]:
    """Return the master services to inspect for detailed status."""
    services: list[str] = []
    for service in _MASTER_SERVICE_NAMES:
        services.extend(_service_probe_names(service))
    if _monitoring_enabled(outputs, config):
        services.append("netdata")
    if _volume_enabled(outputs, config):
        services.append("nfs-server")
    return tuple(services)


def _worker_services(
    outputs: Mapping[str, object],
    config: ClusterConfig,
) -> tuple[str, ...]:
    """Return the worker services to inspect for detailed status."""
    services: list[str] = list(_WORKER_SERVICE_NAMES)
    if _monitoring_enabled(outputs, config):
        services.append("netdata")
    return tuple(services)


def _resolve_volume(
    outputs: Mapping[str, object],
    config: ClusterConfig,
) -> VolumeStatus | None:
    """Return the deployed shared-volume summary when one is attached."""
    output_name = _optional_output_str(outputs, "attached_volume_name")
    if output_name is not None:
        output_size_gb = _optional_output_int(outputs, "managed_volume_size_gb")
        return VolumeStatus(
            name=output_name,
            size_gb=output_size_gb if output_size_gb and output_size_gb > 0 else None,
        )

    if _has_deployed_volume_metadata(outputs):
        return None

    return _resolve_configured_volume(config)


def _has_deployed_volume_metadata(outputs: Mapping[str, object]) -> bool:
    """Return whether stack outputs explicitly describe deployed volume state."""
    return any(
        key in outputs
        for key in (
            "attached_volume_name",
            "attached_volume_id",
            "managed_volume_size_gb",
        )
    )


def _resolve_configured_volume(config: ClusterConfig) -> VolumeStatus | None:
    """Return the config-derived volume summary when stack metadata is absent."""
    if config.volumes.create:
        name = config.volumes.name.strip() or f"{config.cluster.name}-vol"
        return VolumeStatus(name=name, size_gb=config.volumes.size_gb)
    if config.volumes.existing_volume_id.strip():
        name = config.volumes.name.strip() or config.volumes.existing_volume_id.strip()
        return VolumeStatus(name=name, size_gb=None)
    return None


def _service_probe_names(service_name: str) -> tuple[str, ...]:
    """Return the remote service names that satisfy one logical service."""
    return _SERVICE_NAME_ALIASES.get(service_name, (service_name,))


def _canonical_service_name(service_name: str) -> str:
    """Return the display name for a probed systemd unit."""
    for canonical_name, aliases in _SERVICE_NAME_ALIASES.items():
        if service_name in aliases:
            return canonical_name
    return service_name


def _display_service_names(service_names: Sequence[str]) -> tuple[str, ...]:
    """Collapse probe aliases to the logical service names shown to users."""
    ordered_names: list[str] = []
    seen: set[str] = set()
    for service_name in service_names:
        canonical_name = _canonical_service_name(service_name)
        if canonical_name in seen:
            continue
        seen.add(canonical_name)
        ordered_names.append(canonical_name)
    return tuple(ordered_names)


def _normalize_service_statuses(
    services: Sequence[ServiceStatus],
) -> list[ServiceStatus]:
    """Collapse aliased service probes into one user-facing row per node/status."""
    best_by_name: dict[tuple[str, str], ServiceStatus] = {}
    for service in services:
        key = (service.name, service.node)
        existing = best_by_name.get(key)
        if existing is None or (
            _SERVICE_STATUS_ORDER[service.status]
            < _SERVICE_STATUS_ORDER[existing.status]
        ):
            best_by_name[key] = service
    return list(best_by_name.values())


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
        raise PulumiError(f"Pulumi stack output '{key}' was missing or invalid")
    return value.strip()


def _require_output_str_list(outputs: Mapping[str, object], key: str) -> list[str]:
    """Extract a list of strings from stack outputs."""
    value = outputs.get(key)
    if not isinstance(value, list):
        raise PulumiError(f"Pulumi stack output '{key}' was missing or invalid")
    items = cast(list[object], value)
    strings: list[str] = []
    for item in items:
        if not isinstance(item, str) or not item.strip():
            raise PulumiError(f"Pulumi stack output '{key}' was missing or invalid")
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
            lines.append(f"  {current_service + ':':<22} {'; '.join(current_parts)}")
            current_parts = []
        current_service = name
        current_parts.append(part)
    if current_service:
        lines.append(f"  {current_service + ':':<22} {'; '.join(current_parts)}")

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
        typer.Option("--config", help="Path to cluster configuration TOML file."),
    ] = Path("./hailstack.toml"),
    detailed: Annotated[
        bool,
        typer.Option("--detailed", help="Include SSH health probes and resources."),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="JSON output."),
    ] = False,
    ssh_key: Annotated[
        Path | None,
        typer.Option("--ssh-key", help="SSH private key path (default: agent)."),
    ] = None,
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
                outputs,
                loaded_config,
                master_jump_host=_optional_output_str(outputs, "master_public_ip"),
            ),
            ssh_username=loaded_config.cluster.ssh_username,
            ssh_key_path=ssh_key,
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
