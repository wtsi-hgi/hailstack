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

"""Reboot worker nodes in an existing cluster."""

import logging
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep
from typing import Annotated, Protocol, cast

import typer

from hailstack.config.parser import load_config
from hailstack.config.schema import ClusterConfig
from hailstack.errors import PulumiError, SSHError
from hailstack.pulumi.stack import AutomationStackRunner

type RebootRequester = Callable[["RebootTarget", str, Path | None], None]
type ConnectivityChecker = Callable[["RebootTarget", str, Path | None], bool]
type BootMarkerReader = Callable[["RebootTarget", str, Path | None], str]
type Sleeper = Callable[[float], None]
type Clock = Callable[[], float]

_REBOOT_TIMEOUT_SECONDS = 300.0
_RETRY_BACKOFF_SECONDS = (1.0, 2.0, 4.0)
_SSH_HOST_KEY_OPTIONS = (
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "GlobalKnownHostsFile=/dev/null",
)


@dataclass(frozen=True)
class RebootTarget:
    """Represent one worker reboot target resolved from stack outputs."""

    name: str
    host: str
    jump_host: str | None = None


class RebootStackRunner(Protocol):
    """Define the Pulumi stack lookup used by the reboot command."""

    def get_reboot_outputs(self, config: ClusterConfig) -> Mapping[str, object]:
        """Return the outputs required to build a reboot inventory."""
        ...


class RebootExecutor(Protocol):
    """Define the reboot-execution seam used by the command."""

    def reboot_nodes(
        self,
        inventory: Sequence[RebootTarget],
        *,
        ssh_username: str,
        ssh_key_path: Path | None = None,
        timeout_seconds: float,
        backoff_seconds: Sequence[float],
    ) -> None:
        """Reboot the requested nodes and wait for SSH recovery."""
        ...


class PulumiRebootStackRunner:
    """Resolve reboot targets from Pulumi stack outputs."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initialise the runner with a shared logger."""
        self._logger = logger
        self._automation_runner = AutomationStackRunner(logger)

    def get_reboot_outputs(self, config: ClusterConfig) -> Mapping[str, object]:
        """Return JSON stack outputs for the configured cluster."""
        self._automation_runner.check_backend_access(config)
        return self._automation_runner.current_stack_outputs(config)


class SSHRebootExecutor:
    """Reboot nodes over SSH and wait for connectivity to return."""

    def __init__(
        self,
        *,
        logger: logging.Logger,
        reboot_requester: RebootRequester | None = None,
        connectivity_checker: ConnectivityChecker | None = None,
        boot_marker_reader: BootMarkerReader | None = None,
        sleeper: Sleeper = sleep,
        clock: Clock = monotonic,
    ) -> None:
        """Initialise reboot execution dependencies."""
        self._logger = logger
        self._reboot_requester = reboot_requester or self._request_reboot
        self._connectivity_checker = connectivity_checker or self._check_connectivity
        self._boot_marker_reader = boot_marker_reader or self._read_boot_marker
        self._sleeper = sleeper
        self._clock = clock

    def reboot_nodes(
        self,
        inventory: Sequence[RebootTarget],
        *,
        ssh_username: str,
        ssh_key_path: Path | None = None,
        timeout_seconds: float,
        backoff_seconds: Sequence[float],
    ) -> None:
        """Reboot each target and verify SSH returns before the timeout."""
        for target in inventory:
            self._logger.info("Rebooting %s", target.name)
            initial_boot_marker = self._boot_marker_reader(
                target,
                ssh_username,
                ssh_key_path,
            )
            self._reboot_requester(target, ssh_username, ssh_key_path)
            self._wait_for_recovery(
                target,
                initial_boot_marker=initial_boot_marker,
                ssh_username=ssh_username,
                ssh_key_path=ssh_key_path,
                timeout_seconds=timeout_seconds,
                backoff_seconds=backoff_seconds,
            )

    def _wait_for_recovery(
        self,
        target: RebootTarget,
        *,
        initial_boot_marker: str,
        ssh_username: str,
        ssh_key_path: Path | None,
        timeout_seconds: float,
        backoff_seconds: Sequence[float],
    ) -> None:
        """Wait until a node exposes a new boot marker after reboot."""
        deadline = self._clock() + timeout_seconds
        saw_disconnect = False
        attempt = 0

        while self._clock() < deadline:
            if self._connectivity_checker(target, ssh_username, ssh_key_path):
                try:
                    current_boot_marker = self._boot_marker_reader(
                        target,
                        ssh_username,
                        ssh_key_path,
                    )
                except SSHError as error:
                    if not _looks_like_ssh_transport_error(str(error)):
                        raise
                    saw_disconnect = True
                    current_boot_marker = None
                if (
                    current_boot_marker is not None
                    and current_boot_marker != initial_boot_marker
                ):
                    if saw_disconnect:
                        self._logger.info(
                            "SSH connectivity restored for %s",
                            target.name,
                        )
                    else:
                        self._logger.info(
                            "Observed reboot completion for %s without an "
                            "SSH disconnect window",
                            target.name,
                        )
                    return
                if saw_disconnect:
                    self._logger.info(
                        "SSH connectivity returned for %s but reboot has not "
                        "completed yet",
                        target.name,
                    )
            else:
                saw_disconnect = True

            remaining = deadline - self._clock()
            delay = _retry_delay(backoff_seconds, attempt, remaining)
            if delay <= 0.0:
                break
            self._sleeper(delay)
            attempt += 1

        raise SSHError(
            "Timed out waiting for SSH connectivity to return for "
            f"{target.name} within {int(timeout_seconds)} seconds"
        )

    def _request_reboot(
        self,
        target: RebootTarget,
        ssh_username: str,
        ssh_key_path: Path | None,
    ) -> None:
        """Request an asynchronous reboot on one target node."""
        dispatched = self._run_ssh_command(
            target,
            ssh_username,
            (
                "sudo",
                "sh",
                "-c",
                "nohup reboot >/dev/null 2>&1 &",
            ),
            treat_transport_error_as_unreachable=True,
            allow_remote_close_as_success=True,
            ssh_key_path=ssh_key_path,
        )
        if not dispatched:
            raise SSHError(f"Failed to dispatch reboot to {target.name}")

    def _check_connectivity(
        self,
        target: RebootTarget,
        ssh_username: str,
        ssh_key_path: Path | None,
    ) -> bool:
        """Return whether a node currently accepts SSH connections."""
        return self._run_ssh_command(
            target,
            ssh_username,
            ("true",),
            treat_transport_error_as_unreachable=True,
            ssh_key_path=ssh_key_path,
        )

    def _read_boot_marker(
        self,
        target: RebootTarget,
        ssh_username: str,
        ssh_key_path: Path | None,
    ) -> str:
        """Read the node boot ID so reboot completion can be confirmed."""
        boot_marker = self._run_ssh_command_for_output(
            target,
            ssh_username,
            ("cat", "/proc/sys/kernel/random/boot_id"),
            ssh_key_path=ssh_key_path,
        )
        stripped_marker = boot_marker.strip()
        if not stripped_marker:
            raise SSHError(f"Unable to read boot marker for {target.name}")
        return stripped_marker

    def _run_ssh_command_for_output(
        self,
        target: RebootTarget,
        ssh_username: str,
        command: tuple[str, ...],
        *,
        ssh_key_path: Path | None,
    ) -> str:
        """Run one SSH command and return stdout or raise on failure."""
        result = self._run_ssh_subprocess(
            target,
            ssh_username,
            command,
            ssh_key_path=ssh_key_path,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise SSHError(stderr or f"SSH command failed for {target.name}")
        return result.stdout

    def _run_ssh_command(
        self,
        target: RebootTarget,
        ssh_username: str,
        command: tuple[str, ...],
        *,
        treat_transport_error_as_unreachable: bool,
        allow_remote_close_as_success: bool = False,
        ssh_key_path: Path | None = None,
    ) -> bool:
        """Run one SSH command and either raise or report transport failures."""
        try:
            result = self._run_ssh_subprocess(
                target,
                ssh_username,
                command,
                ssh_key_path=ssh_key_path,
            )
        except subprocess.TimeoutExpired:
            if treat_transport_error_as_unreachable:
                return False
            raise SSHError(f"SSH command timed out for {target.name}") from None

        stderr = result.stderr.strip()
        if result.returncode == 0:
            return True
        if allow_remote_close_as_success and _looks_like_reboot_disconnect(stderr):
            return True
        if treat_transport_error_as_unreachable and _looks_like_ssh_transport_error(
            stderr
        ):
            return False
        raise SSHError(stderr or f"SSH command failed for {target.name}")

    def _run_ssh_subprocess(
        self,
        target: RebootTarget,
        ssh_username: str,
        command: tuple[str, ...],
        *,
        ssh_key_path: Path | None,
    ) -> subprocess.CompletedProcess[str]:
        """Run an SSH subprocess with shared transport settings."""
        try:
            ssh_command = [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                *_SSH_HOST_KEY_OPTIONS,
            ]
            if ssh_key_path is not None:
                ssh_command.extend(["-i", str(ssh_key_path)])
            if target.jump_host:
                ssh_command.extend(["-J", f"{ssh_username}@{target.jump_host}"])
            ssh_command.extend([f"{ssh_username}@{target.host}", *command])
            return subprocess.run(
                ssh_command,
                capture_output=True,
                check=False,
                text=True,
                timeout=10,
            )
        except FileNotFoundError as error:
            raise SSHError("SSH CLI not found") from error


def get_reboot_logger() -> logging.Logger:
    """Return a dedicated stderr logger for reboot progress messages."""
    logger = logging.getLogger("hailstack.reboot")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def create_reboot_stack_runner(logger: logging.Logger) -> RebootStackRunner:
    """Create the default Pulumi output resolver for reboot."""
    return PulumiRebootStackRunner(logger)


def create_reboot_executor(logger: logging.Logger) -> RebootExecutor:
    """Create the default SSH reboot executor for the command."""
    return SSHRebootExecutor(logger=logger)


def reboot_command(
    config: Annotated[str, typer.Option("--config")] = "./hailstack.toml",
    node: Annotated[str | None, typer.Option("--node")] = None,
    ssh_key: Annotated[str | None, typer.Option("--ssh-key")] = None,
    dotenv: Annotated[str | None, typer.Option("--dotenv")] = None,
) -> None:
    """Reboot worker nodes."""
    logger = get_reboot_logger()
    loaded_config = load_config(
        Path(config),
        dotenv_file=Path(dotenv) if dotenv is not None else None,
    )
    stack_runner = create_reboot_stack_runner(logger)
    executor = create_reboot_executor(logger)
    outputs = stack_runner.get_reboot_outputs(loaded_config)
    inventory = _resolve_inventory(outputs, loaded_config.cluster.name)
    targets = _select_targets(
        inventory,
        cluster_name=loaded_config.cluster.name,
        requested_node=node,
    )
    executor.reboot_nodes(
        targets,
        ssh_username=loaded_config.cluster.ssh_username,
        ssh_key_path=Path(ssh_key) if ssh_key is not None else None,
        timeout_seconds=_REBOOT_TIMEOUT_SECONDS,
        backoff_seconds=_RETRY_BACKOFF_SECONDS,
    )


def _resolve_inventory(
    outputs: Mapping[str, object],
    cluster_name: str,
) -> list[RebootTarget]:
    """Build worker reboot targets from the required Pulumi output values."""
    resolved_cluster_name = _require_output_str(
        outputs,
        "cluster_name",
        default=cluster_name,
    )
    master_jump_host = _optional_output_str(outputs, "master_public_ip")
    worker_names = _require_output_str_list(outputs, "worker_names")
    worker_hosts = _require_output_str_list(outputs, "worker_private_ips")

    if len(worker_names) != len(worker_hosts):
        raise PulumiError(
            "Pulumi stack outputs contain mismatched worker names and IPs"
        )

    return [
        RebootTarget(
            name=_display_name(resolved_cluster_name, name),
            host=host,
            jump_host=master_jump_host,
        )
        for name, host in zip(worker_names, worker_hosts, strict=True)
    ]


def _optional_output_str(outputs: Mapping[str, object], key: str) -> str | None:
    """Extract an optional string output and return None when absent."""
    value = outputs.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise PulumiError(f"Pulumi stack output '{key}' was missing or invalid")
    return value


def _select_targets(
    inventory: Sequence[RebootTarget],
    *,
    cluster_name: str,
    requested_node: str | None,
) -> list[RebootTarget]:
    """Return either all workers or the selected worker target."""
    if requested_node is None:
        return list(inventory)

    candidate = requested_node.strip()
    if _is_master_reference(candidate, cluster_name):
        raise typer.BadParameter("Cannot reboot master node", param_hint="--node")

    for target in inventory:
        if _matches_target(target, candidate, cluster_name=cluster_name):
            return [target]

    raise typer.BadParameter("Worker not found", param_hint="--node")


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
    """Extract a list of non-empty strings from stack outputs."""
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
    """Reduce full resource names to the documented worker labels."""
    prefix = f"{cluster_name}-"
    if raw_name.startswith(prefix):
        return raw_name.removeprefix(prefix)
    return raw_name


def _matches_target(
    target: RebootTarget,
    candidate: str,
    *,
    cluster_name: str,
) -> bool:
    """Return whether a CLI node selector matches one worker target."""
    normalized_candidate = candidate.strip().lower()
    full_name = f"{cluster_name}-{target.name}".lower()
    return normalized_candidate in {target.name.lower(), full_name}


def _is_master_reference(candidate: str, cluster_name: str) -> bool:
    """Return whether a CLI selector points at the master node."""
    normalized_candidate = candidate.strip().lower()
    cluster_prefix = f"{cluster_name.lower()}-"
    if normalized_candidate.startswith(cluster_prefix):
        normalized_candidate = normalized_candidate.removeprefix(cluster_prefix)
    return normalized_candidate == "master" or normalized_candidate.startswith(
        "master-"
    )


def _retry_delay(
    backoff_seconds: Sequence[float],
    attempt: int,
    remaining: float,
) -> float:
    """Return the next capped retry delay for reboot recovery polling."""
    if remaining <= 0.0 or not backoff_seconds:
        return 0.0
    delay = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
    return min(delay, remaining)


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
            "connection closed",
            "closed by remote host",
            "connection reset",
        )
    )


def _looks_like_reboot_disconnect(stderr: str) -> bool:
    """Return whether stderr matches the expected reboot dispatch disconnect."""
    lowered = stderr.lower()
    return any(
        fragment in lowered
        for fragment in (
            "connection closed",
            "closed by remote host",
            "connection reset",
        )
    )


__all__ = ["reboot_command"]
