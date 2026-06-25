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

"""Run Packer builds for Hailstack images."""

import json
import logging
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Protocol
from uuid import UUID, uuid4

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError

from hailstack.config.compatibility import Bundle
from hailstack.config.schema import ClusterConfig, PackerConfig
from hailstack.errors import PackerError
from hailstack.runtime_paths import (
    PACKER_ROOT,
)
from hailstack.runtime_paths import (
    PACKER_SCRIPTS_PATH as RUNTIME_PACKER_SCRIPTS_PATH,
)
from hailstack.runtime_paths import (
    PACKER_TEMPLATE_PATH as RUNTIME_PACKER_TEMPLATE_PATH,
)

PACKER_ROOT_PATH = PACKER_ROOT
PACKER_TEMPLATE_PATH = RUNTIME_PACKER_TEMPLATE_PATH
PACKER_SCRIPTS_PATH = RUNTIME_PACKER_SCRIPTS_PATH
PACKER_APT_LOCK_HELPER_RELATIVE_PATH = Path("scripts/apt-locks.sh")
REQUIRED_PACKER_SCRIPT_RELATIVE_PATHS = (
    Path("scripts/base.sh"),
    Path("scripts/ubuntu/packages.sh"),
    Path("scripts/ubuntu/hadoop.sh"),
    Path("scripts/ubuntu/spark.sh"),
    Path("scripts/ubuntu/hail.sh"),
    Path("scripts/ubuntu/jupyter.sh"),
    Path("scripts/ubuntu/gnomad.sh"),
    Path("scripts/ubuntu/uv.sh"),
    Path("scripts/ubuntu/netdata.sh"),
)
REQUIRED_PACKER_ASSET_RELATIVE_PATHS = (
    PACKER_APT_LOCK_HELPER_RELATIVE_PATH,
    *REQUIRED_PACKER_SCRIPT_RELATIVE_PATHS,
)
REQUIRED_PACKER_ASSET_PATHS = tuple(
    PACKER_ROOT_PATH / relative_path
    for relative_path in REQUIRED_PACKER_ASSET_RELATIVE_PATHS
)
REQUIRED_PACKER_SCRIPT_PATHS = tuple(
    PACKER_ROOT_PATH / relative_path
    for relative_path in REQUIRED_PACKER_SCRIPT_RELATIVE_PATHS
)
_MAX_PACKER_DIAGNOSTIC_LINES = 8
_PACKER_MONITOR_POLL_SECONDS = 0.05
_PACKER_INTERRUPT_GRACE_SECONDS = 5.0
_PACKER_SSH_SECURITY_GROUP_NAME_PREFIX = "hailstack-packer-ssh-"
_PACKER_SSH_SECURITY_GROUP_DESCRIPTION = (
    "Temporary Hailstack Packer SSH access for image build"
)
_PACKER_MANAGEMENT_PORT_NAME_PREFIX = "hailstack-packer-management-"
_NO_ROUTE_TO_HOST_RE = re.compile(
    r"dial tcp (?P<host>[^:\s]+):(?P<port>\d+): connect: no route to host",
    re.IGNORECASE,
)
_SSH_CONNECT_RE = re.compile(
    r"Using SSH communicator to connect: (?P<host>[^\s,]+)",
    re.IGNORECASE,
)


class PackerRunner(Protocol):
    """Define the callable shape used to execute the Packer CLI."""

    def __call__(
        self,
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        """Run a prepared Packer command and return its completed process."""
        ...


class NetworkResolver(Protocol):
    """Define the callable shape used to resolve OpenStack network IDs."""

    def __call__(self, network_name: str) -> str:
        """Resolve a configured network name to an OpenStack UUID."""
        ...


class BuildSecurityGroupManager(Protocol):
    """Define how build-image creates temporary SSH security-group access."""

    def create(self) -> str:
        """Create temporary SSH ingress and return its security-group name."""
        ...

    def cleanup(self, security_group_name: str) -> None:
        """Delete temporary SSH ingress by security-group name."""
        ...


class BuildPortManager(Protocol):
    """Define how build-image creates temporary OpenStack build ports."""

    def create_management_port(
        self,
        *,
        network_id: str,
        security_group_name: str,
    ) -> str:
        """Create the management port and return its port UUID."""
        ...

    def cleanup(self, port_id: str) -> None:
        """Delete a temporary build port by UUID."""
        ...


@dataclass(frozen=True)
class _PackerOutputEvent:
    """Represent one line captured from a live Packer output stream."""

    stream_name: str
    line: str


class _OpenStackNetworkShow(BaseModel):
    """Represent the fields Hailstack needs from OpenStack network JSON."""

    model_config = ConfigDict(extra="allow")

    id: str = Field(validation_alias=AliasChoices("id", "ID"))


class _OpenStackPortCreate(BaseModel):
    """Represent the fields Hailstack needs from OpenStack port JSON."""

    model_config = ConfigDict(extra="allow")

    id: str = Field(validation_alias=AliasChoices("id", "ID"))


class _OpenStackBuildSecurityGroupManager:
    """Manage temporary OpenStack SSH security-group access for Packer."""

    def create(self) -> str:
        """Create a temporary TCP/22 ingress security group."""
        name = _temporary_packer_ssh_security_group_name()
        _run_openstack_security_group_command(
            [
                "openstack",
                "security",
                "group",
                "create",
                "--description",
                _PACKER_SSH_SECURITY_GROUP_DESCRIPTION,
                name,
                "-f",
                "json",
            ],
            action=f"create temporary Packer SSH security group `{name}`",
        )
        try:
            _run_openstack_security_group_command(
                [
                    "openstack",
                    "security",
                    "group",
                    "rule",
                    "create",
                    "--ingress",
                    "--ethertype",
                    "IPv4",
                    "--protocol",
                    "tcp",
                    "--dst-port",
                    "22",
                    "--remote-ip",
                    "0.0.0.0/0",
                    name,
                    "-f",
                    "json",
                ],
                action=f"create temporary Packer SSH ingress rule on `{name}`",
            )
        except PackerError:
            _cleanup_security_group_after_creation_failure(self, name)
            raise

        return name

    def cleanup(self, security_group_name: str) -> None:
        """Delete a temporary OpenStack security group."""
        _run_openstack_security_group_command(
            [
                "openstack",
                "security",
                "group",
                "delete",
                security_group_name,
            ],
            action=(
                f"delete temporary Packer SSH security group `{security_group_name}`"
            ),
        )


class _OpenStackBuildPortManager:
    """Manage temporary OpenStack ports for Packer build instances."""

    def create_management_port(
        self,
        *,
        network_id: str,
        security_group_name: str,
    ) -> str:
        """Create the management port with default plus temporary SSH ingress."""
        name = _temporary_packer_port_name(_PACKER_MANAGEMENT_PORT_NAME_PREFIX)
        return _run_openstack_port_create_command(
            [
                "openstack",
                "port",
                "create",
                "--network",
                network_id,
                "--security-group",
                "default",
                "--security-group",
                security_group_name,
                "--enable-port-security",
                name,
                "-f",
                "json",
            ],
            action=f"create temporary Packer management port `{name}`",
        )

    def cleanup(self, port_id: str) -> None:
        """Delete a temporary OpenStack port."""
        _run_openstack_port_delete_command(
            [
                "openstack",
                "port",
                "delete",
                port_id,
            ],
            action=f"delete temporary Packer port `{port_id}`",
        )


def _temporary_packer_ssh_security_group_name() -> str:
    """Return a short unique security-group name for a Packer build."""
    return f"{_PACKER_SSH_SECURITY_GROUP_NAME_PREFIX}{uuid4().hex[:8]}"


def _temporary_packer_port_name(prefix: str) -> str:
    """Return a short unique port name for a Packer build."""
    return f"{prefix}{uuid4().hex[:8]}"


def _run_openstack_security_group_command(
    command: list[str],
    *,
    action: str,
) -> None:
    """Run an OpenStack security-group command with user-facing errors."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as error:
        raise _security_group_provisioning_error(
            action,
            "The `openstack` CLI was not found on PATH.",
        ) from error

    if result.returncode == 0:
        return

    detail = _raw_packer_output(result)
    if detail:
        detail = f"OpenStack CLI output: {detail}"
    else:
        detail = f"OpenStack CLI exited with status {result.returncode}."
    raise _security_group_provisioning_error(action, detail)


def _run_openstack_port_create_command(
    command: list[str],
    *,
    action: str,
) -> str:
    """Run an OpenStack port create command and return the created port UUID."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as error:
        raise _port_provisioning_error(
            action,
            "The `openstack` CLI was not found on PATH.",
        ) from error

    if result.returncode != 0:
        raise _port_provisioning_error(
            action,
            _openstack_command_failure_detail(result),
        )

    return _parse_openstack_port_id(action, result.stdout)


def _run_openstack_port_delete_command(
    command: list[str],
    *,
    action: str,
) -> None:
    """Run an OpenStack port delete command with user-facing errors."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as error:
        raise _port_provisioning_error(
            action,
            "The `openstack` CLI was not found on PATH.",
        ) from error

    if result.returncode == 0:
        return

    raise _port_provisioning_error(
        action,
        _openstack_command_failure_detail(result),
    )


def _openstack_command_failure_detail(result: subprocess.CompletedProcess[str]) -> str:
    """Return consistent OpenStack CLI failure detail."""
    detail = _raw_packer_output(result)
    if detail:
        return f"OpenStack CLI output: {detail}"
    return f"OpenStack CLI exited with status {result.returncode}."


def _parse_openstack_port_id(action: str, output: str) -> str:
    """Parse and validate an OpenStack port UUID from CLI JSON output."""
    try:
        port = _OpenStackPortCreate.model_validate_json(output)
    except (json.JSONDecodeError, ValidationError) as error:
        raise _port_provisioning_error(
            action,
            "OpenStack CLI returned JSON without an `id` field.",
        ) from error

    port_id = port.id.strip()
    if not _is_uuid(port_id):
        raise _port_provisioning_error(
            action,
            f"OpenStack CLI returned non-UUID port id `{port.id}`.",
        )

    return port_id


def _port_provisioning_error(action: str, detail: str) -> PackerError:
    """Build a clear PackerError for temporary port setup failures."""
    return PackerError(
        f"Could not {action} before launching Packer. Check OpenStack "
        f"port/security group quota/permissions and credentials. {detail}"
    )


def _security_group_provisioning_error(action: str, detail: str) -> PackerError:
    """Build a clear PackerError for SSH security-group setup failures."""
    return PackerError(
        f"Could not {action} before launching Packer. Check OpenStack "
        f"security group quota/permissions and credentials. {detail}"
    )


def _cleanup_security_group_after_creation_failure(
    security_group_manager: BuildSecurityGroupManager,
    security_group_name: str,
) -> None:
    """Best-effort delete after creating a group but failing to add ingress."""
    try:
        security_group_manager.cleanup(security_group_name)
    except PackerError:
        return


def _run_packer(
    command: list[str],
    *,
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    """Execute a Packer build command in a mockable wrapper."""
    with _packer_early_failure_log_environment() as (
        environment,
        monitored_log_path,
    ):
        process: subprocess.Popen[str] = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=cwd,
            env=environment,
            start_new_session=os.name == "posix",
        )
        return _collect_packer_process(
            command,
            process,
            monitored_log_path=monitored_log_path,
        )


@contextmanager
def _packer_early_failure_log_environment() -> Iterator[
    tuple[dict[str, str], Path | None]
]:
    """Enable a temporary Packer log so known SSH routing failures surface early."""
    environment = os.environ.copy()
    if _packer_log_enabled(environment.get("PACKER_LOG")):
        yield environment, _existing_packer_log_path(environment)
        return
    if environment.get("PACKER_LOG") is not None:
        yield environment, None
        return

    with tempfile.TemporaryDirectory(prefix="hailstack-packer-") as temp_dir:
        log_path = Path(temp_dir) / "packer-debug.log"
        environment["PACKER_LOG"] = "1"
        environment["PACKER_LOG_PATH"] = str(log_path)
        yield environment, log_path


def _packer_log_enabled(value: str | None) -> bool:
    """Return whether PACKER_LOG enables debug logging."""
    if value is None:
        return False
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def _existing_packer_log_path(environment: Mapping[str, str]) -> Path | None:
    """Return the user-configured Packer log path, if logs are file-backed."""
    log_path = environment.get("PACKER_LOG_PATH")
    if not log_path:
        return None

    path = Path(log_path)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def _collect_packer_process(
    command: list[str],
    process: subprocess.Popen[str],
    *,
    monitored_log_path: Path | None,
) -> subprocess.CompletedProcess[str]:
    """Collect live Packer output while preserving known SSH routing failures."""
    assert process.stdout is not None
    assert process.stderr is not None

    events: queue.Queue[_PackerOutputEvent] = queue.Queue()
    stdout_thread = _start_packer_output_reader("stdout", process.stdout, events)
    stderr_thread = _start_packer_output_reader("stderr", process.stderr, events)
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    log_position = 0
    no_route_line: str | None = None

    while process.poll() is None:
        stream_no_route_line = _drain_packer_output_events(
            events,
            stdout_lines,
            stderr_lines,
        )
        if no_route_line is None:
            no_route_line = stream_no_route_line

        log_no_route_line, log_position = _read_packer_log_for_no_route(
            monitored_log_path,
            log_position,
        )
        if no_route_line is None:
            no_route_line = log_no_route_line
        time.sleep(_PACKER_MONITOR_POLL_SECONDS)

    returncode = process.wait()
    final_no_route_line, _ = _read_packer_log_for_no_route(
        monitored_log_path,
        log_position,
    )
    if no_route_line is None:
        no_route_line = final_no_route_line

    stdout_thread.join()
    stderr_thread.join()
    stream_no_route_line = _drain_packer_output_events(
        events,
        stdout_lines,
        stderr_lines,
    )
    if no_route_line is None:
        no_route_line = stream_no_route_line
    if no_route_line is not None:
        _append_uncaptured_packer_failure_line(
            no_route_line,
            stdout_lines,
            stderr_lines,
        )

    return subprocess.CompletedProcess(
        args=command,
        returncode=returncode,
        stdout="".join(stdout_lines),
        stderr="".join(stderr_lines),
    )


def _start_packer_output_reader(
    stream_name: str,
    stream: IO[str],
    events: queue.Queue[_PackerOutputEvent],
) -> threading.Thread:
    """Start a background reader for one Packer pipe."""
    thread = threading.Thread(
        target=_enqueue_packer_output_lines,
        args=(stream_name, stream, events),
        daemon=True,
    )
    thread.start()
    return thread


def _enqueue_packer_output_lines(
    stream_name: str,
    stream: IO[str],
    events: queue.Queue[_PackerOutputEvent],
) -> None:
    """Send each line from a Packer pipe to the monitor queue."""
    try:
        for line in stream:
            events.put(_PackerOutputEvent(stream_name, line))
    finally:
        stream.close()


def _drain_packer_output_events(
    events: queue.Queue[_PackerOutputEvent],
    stdout_lines: list[str],
    stderr_lines: list[str],
) -> str | None:
    """Drain queued Packer output and return the first SSH no-route line."""
    no_route_line: str | None = None
    while True:
        try:
            event = events.get_nowait()
        except queue.Empty:
            return no_route_line

        if event.stream_name == "stdout":
            stdout_lines.append(event.line)
        else:
            stderr_lines.append(event.line)
        if no_route_line is None and _is_packer_ssh_no_route_line(event.line):
            no_route_line = event.line.strip()


def _read_packer_log_for_no_route(
    log_path: Path | None,
    position: int,
) -> tuple[str | None, int]:
    """Read new Packer log lines and return an unreachable SSH route if present."""
    if log_path is None:
        return None, position

    try:
        with log_path.open(encoding="utf-8", errors="replace") as log_file:
            log_file.seek(position)
            lines = log_file.readlines()
            next_position = log_file.tell()
    except FileNotFoundError:
        return None, position

    return _first_packer_ssh_no_route_line(lines), next_position


def _first_packer_ssh_no_route_line(lines: list[str]) -> str | None:
    """Return the first line showing Packer SSH has no route to the build host."""
    for line in lines:
        if _is_packer_ssh_no_route_line(line):
            return line.strip()
    return None


def _append_uncaptured_packer_failure_line(
    line: str,
    stdout_lines: list[str],
    stderr_lines: list[str],
) -> None:
    """Preserve a matching log-file-only failure line in returned stderr."""
    normalized_line = line if line.endswith("\n") else f"{line}\n"
    captured_output = "".join((*stdout_lines, *stderr_lines))
    if line not in captured_output:
        stderr_lines.append(normalized_line)


def _packer_vars(
    config: ClusterConfig,
    bundle: Bundle,
    *,
    network_id: str,
    lustre_network_id: str,
    port_ids: tuple[str, ...] = (),
) -> dict[str, str]:
    """Build the documented Packer variable mapping for a bundle."""
    packer_config = config.validate_for_command("build-image").packer
    assert packer_config is not None
    floating_ip_pool, _ = _packer_floating_ip_pool(config, packer_config)

    return {
        "bundle_id": bundle.id,
        "hail_version": bundle.hail,
        "spark_version": bundle.spark,
        "hadoop_version": bundle.hadoop,
        "java_version": bundle.java,
        "python_version": bundle.python,
        "scala_version": bundle.scala,
        "gnomad_version": bundle.gnomad,
        "gnomad_methods_version": packer_config.gnomad_methods_version,
        "base_image": packer_config.base_image,
        "ssh_username": config.cluster.ssh_username,
        "flavor": packer_config.flavour,
        "network": network_id,
        "lustre_network": lustre_network_id,
        "floating_ip_pool": floating_ip_pool,
        "ports": ",".join(port_ids),
    }


def _resolve_packer_network_id(
    configured_network: str,
    network_resolver: NetworkResolver,
) -> str:
    """Return the OpenStack network UUID expected by Packer."""
    network_name_or_id = configured_network.strip()
    if not network_name_or_id:
        raise PackerError("cluster.network_name is required for build-image")
    if _is_uuid(network_name_or_id):
        return network_name_or_id

    network_id = network_resolver(network_name_or_id).strip()
    if not _is_uuid(network_id):
        raise _network_resolution_error(
            network_name_or_id,
            f"OpenStack network resolver returned non-UUID id `{network_id}`.",
        )
    return network_id


def _resolve_optional_packer_network_id(
    configured_network: str,
    network_resolver: NetworkResolver,
) -> str:
    """Return an optional OpenStack network UUID, or blank when unset."""
    if not configured_network.strip():
        return ""

    return _resolve_packer_network_id(configured_network, network_resolver)


def _is_uuid(value: str) -> bool:
    """Return whether a string is already a UUID-shaped network ID."""
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _resolve_openstack_network_id(network_name: str) -> str:
    """Resolve an OpenStack network name to the UUID Packer requires."""
    try:
        result = subprocess.run(
            ["openstack", "network", "show", network_name, "-f", "json"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as error:
        raise _network_resolution_error(
            network_name,
            "The `openstack` CLI was not found on PATH.",
        ) from error

    if result.returncode != 0:
        detail = _raw_packer_output(result)
        if detail:
            detail = f"OpenStack CLI output: {detail}"
        else:
            detail = f"OpenStack CLI exited with status {result.returncode}."
        raise _network_resolution_error(network_name, detail)

    return _parse_openstack_network_id(network_name, result.stdout)


def _parse_openstack_network_id(network_name: str, output: str) -> str:
    """Parse and validate an OpenStack network UUID from CLI JSON output."""
    try:
        network = _OpenStackNetworkShow.model_validate_json(output)
    except (json.JSONDecodeError, ValidationError) as error:
        raise _network_resolution_error(
            network_name,
            "OpenStack CLI returned JSON without an `id` field.",
        ) from error

    network_id = network.id.strip()
    if not _is_uuid(network_id):
        raise _network_resolution_error(
            network_name,
            f"OpenStack CLI returned non-UUID network id `{network.id}`.",
        )

    return network_id


def _network_resolution_error(network_name: str, detail: str) -> PackerError:
    """Build a clear network-resolution PackerError."""
    return PackerError(
        f"Could not resolve OpenStack network '{network_name}' to the UUID "
        "Packer requires. Run `openstack network list` to verify the network "
        f"name and check OpenStack credentials. {detail}"
    )


def _packer_floating_ip_pool(
    config: ClusterConfig,
    packer_config: PackerConfig,
) -> tuple[str, str]:
    """Resolve the image-build floating IP pool and its config source."""
    packer_pool = packer_config.floating_ip_pool.strip()
    if packer_pool:
        return packer_pool, "packer.floating_ip_pool"

    cluster_pool = config.cluster.floating_ip_pool.strip()
    if cluster_pool:
        return cluster_pool, "cluster.floating_ip_pool"

    return "", "none"


def _log_packer_networking(
    logger: logging.Logger,
    config: ClusterConfig,
    network_id: str,
    lustre_network_id: str,
) -> None:
    """Log Packer networking choices without exposing credentials."""
    packer_config = config.validate_for_command("build-image").packer
    assert packer_config is not None
    floating_ip_pool, source = _packer_floating_ip_pool(config, packer_config)

    logger.info("Packer OpenStack network: %s", config.cluster.network_name)
    logger.info("Packer OpenStack network UUID: %s", network_id)
    if lustre_network_id:
        logger.info(
            "Packer OpenStack Lustre network: %s",
            config.cluster.lustre_network,
        )
        logger.info("Packer OpenStack Lustre network UUID: %s", lustre_network_id)
    if floating_ip_pool:
        logger.info("Packer floating IP pool: %s (%s)", floating_ip_pool, source)
        return

    logger.info("Packer floating IP pool: none configured")


def _packer_command(template_path: Path, variables: Mapping[str, str]) -> list[str]:
    """Render the Packer CLI invocation for a variable set."""
    command = ["packer", "build", "-machine-readable"]
    for name, value in variables.items():
        command.extend(["-var", f"{name}={value}"])
    command.append(str(template_path))

    return command


def _extract_image_id(stdout: str) -> str:
    """Parse a Packer machine-readable artifact ID from stdout."""
    for line in reversed(stdout.splitlines()):
        if "artifact,0,id," in line:
            return line.rsplit("artifact,0,id,", maxsplit=1)[1].strip()

        stripped = line.strip()
        if stripped and "," not in stripped:
            return stripped

    raise PackerError("Packer build completed without reporting an image ID")


def _packer_failure_detail(
    result: subprocess.CompletedProcess[str],
    *,
    network_name: str | None = None,
) -> str:
    """Render readable Packer diagnostics while preserving raw command output."""
    raw_output = _raw_packer_output(result)
    if not raw_output:
        return "Packer build failed: unknown error"

    no_route_detail = _packer_ssh_no_route_failure_detail(
        raw_output,
        network_name=network_name,
    )
    if no_route_detail is not None:
        return no_route_detail

    diagnostics = _extract_packer_diagnostics(raw_output)
    if not diagnostics:
        return f"Packer build failed: {raw_output}"

    diagnostic_lines = "\n".join(f"- {line}" for line in diagnostics)
    return (
        "Packer build failed.\n"
        f"Packer diagnostics:\n{diagnostic_lines}\n"
        f"Raw Packer output:\n{raw_output}"
    )


def _raw_packer_output(result: subprocess.CompletedProcess[str]) -> str:
    """Return all captured Packer output in the order users expect to inspect."""
    outputs = [
        output.strip() for output in (result.stderr, result.stdout) if output.strip()
    ]
    return "\n".join(outputs)


def _packer_ssh_no_route_failure_detail(
    raw_output: str,
    *,
    network_name: str | None,
) -> str | None:
    """Explain an unreachable fixed-IP SSH route from Packer's debug output."""
    if not any(_is_packer_ssh_no_route_line(line) for line in raw_output.splitlines()):
        return None

    host = _extract_packer_ssh_no_route_host(raw_output)
    target = (
        f"temporary build instance SSH address `{host}`"
        if host is not None
        else "temporary build instance SSH address"
    )
    network = (
        f"`cluster.network_name` (`{network_name}`)"
        if network_name is not None
        else "`cluster.network_name`"
    )
    return (
        f"Packer could not SSH to the {target}: no route to host. "
        f"The Hailstack runner cannot reach that build instance address for {network}. "
        "Set `cluster.floating_ip_pool` or `[packer].floating_ip_pool` to a "
        "reachable external floating IP pool, or run Hailstack from a host "
        "that can route to `cluster.network_name`."
    )


def _extract_packer_ssh_no_route_host(raw_output: str) -> str | None:
    """Return the SSH address Packer could not route to, if debug output names it."""
    for line in raw_output.splitlines():
        no_route_match = _NO_ROUTE_TO_HOST_RE.search(line)
        if no_route_match is not None:
            return no_route_match.group("host")

    for line in raw_output.splitlines():
        ssh_connect_match = _SSH_CONNECT_RE.search(line)
        if ssh_connect_match is not None:
            return ssh_connect_match.group("host")

    return None


def _is_packer_ssh_no_route_line(line: str) -> bool:
    """Return whether a Packer line proves SSH cannot route to the build host."""
    lowered = line.lower()
    return "no route to host" in lowered and ("ssh" in lowered or "dial tcp" in lowered)


def _extract_packer_diagnostics(raw_output: str) -> list[str]:
    """Extract the most useful human-readable messages from Packer output."""
    diagnostics: list[str] = []
    for line in raw_output.splitlines():
        message = _parse_packer_machine_readable_message(line)
        if message is None:
            continue
        if not _is_packer_diagnostic_message(message):
            continue
        if message not in diagnostics:
            diagnostics.append(message)
        if len(diagnostics) == _MAX_PACKER_DIAGNOSTIC_LINES:
            break

    return diagnostics


def _parse_packer_machine_readable_message(line: str) -> str | None:
    """Parse one machine-readable Packer line into display text when possible."""
    parts = line.split(",", maxsplit=4)
    if len(parts) < 4:
        return None

    target = parts[1].strip()
    record_type = parts[2].strip()
    if record_type == "ui" and len(parts) == 5:
        return _clean_packer_message(parts[4])
    if record_type == "error":
        message = _clean_packer_message(parts[3])
        if target and not message.startswith(f"{target}:"):
            return f"{target}: {message}"
        return message

    return None


def _clean_packer_message(message: str) -> str:
    """Normalize Packer message text without hiding its original meaning."""
    return message.replace("\\n", "\n").strip()


def _is_packer_diagnostic_message(message: str) -> bool:
    """Return whether a Packer message belongs in the failure summary."""
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "error",
            "errored",
            "failed",
            "timeout",
            "timed out",
            "no artifacts were created",
        )
    )


def _required_packer_asset_paths(template_path: Path) -> tuple[Path, ...]:
    """Return the asset paths required by the checked-in packer template."""
    return tuple(
        template_path.parent / relative_path
        for relative_path in REQUIRED_PACKER_ASSET_RELATIVE_PATHS
    )


def _validate_packer_assets(template_path: Path) -> None:
    """Ensure the template and its provisioner scripts exist and are executable."""
    missing_paths: list[str] = []
    non_executable_paths: list[str] = []

    if not template_path.is_file():
        missing_paths.append(str(template_path))

    for asset_path in _required_packer_asset_paths(template_path):
        if not asset_path.is_file():
            missing_paths.append(str(asset_path))
            continue
        if asset_path.suffix == ".sh" and not os.access(asset_path, os.X_OK):
            non_executable_paths.append(str(asset_path))

    problems: list[str] = []
    if missing_paths:
        problems.append(f"missing: {', '.join(missing_paths)}")
    if non_executable_paths:
        problems.append(f"not executable: {', '.join(non_executable_paths)}")
    if problems:
        raise PackerError(f"Missing required Packer assets: {'; '.join(problems)}")


@contextmanager
def _normalized_relative_packer_log_path() -> Iterator[None]:
    """Resolve relative PACKER_LOG_PATH values before Packer changes cwd."""
    log_path = os.environ.get("PACKER_LOG_PATH")
    if not log_path or Path(log_path).is_absolute():
        yield
        return

    os.environ["PACKER_LOG_PATH"] = str(Path.cwd() / log_path)
    try:
        yield
    finally:
        os.environ["PACKER_LOG_PATH"] = log_path


@contextmanager
def _temporary_packer_networking(
    *,
    floating_ip_pool: str,
    network_id: str,
    security_group_manager: BuildSecurityGroupManager,
    port_manager: BuildPortManager,
    logger: logging.Logger,
) -> Iterator[tuple[str, ...]]:
    """Create temporary explicit ports only for floating-IP Packer builds."""
    if not floating_ip_pool:
        yield ()
        return

    security_group_name = security_group_manager.create()
    logger.info("Packer SSH security group: %s", security_group_name)
    port_ids: list[str] = []
    try:
        _create_temporary_packer_ports(
            network_id=network_id,
            security_group_name=security_group_name,
            port_manager=port_manager,
            logger=logger,
            port_ids=port_ids,
        )
    except Exception:
        _cleanup_temporary_packer_ports(port_ids, port_manager, logger)
        _cleanup_temporary_packer_security_group(
            security_group_name,
            security_group_manager,
            logger,
        )
        raise

    try:
        yield tuple(port_ids)
    finally:
        _cleanup_temporary_packer_ports(port_ids, port_manager, logger)
        _cleanup_temporary_packer_security_group(
            security_group_name,
            security_group_manager,
            logger,
        )


def _create_temporary_packer_ports(
    *,
    network_id: str,
    security_group_name: str,
    port_manager: BuildPortManager,
    logger: logging.Logger,
    port_ids: list[str],
) -> None:
    """Create the management port for Packer port input."""
    management_port_id = port_manager.create_management_port(
        network_id=network_id,
        security_group_name=security_group_name,
    )
    logger.info("Packer management port: %s", management_port_id)
    port_ids.append(management_port_id)


def _cleanup_temporary_packer_ports(
    port_ids: list[str],
    port_manager: BuildPortManager,
    logger: logging.Logger,
) -> None:
    """Best-effort delete temporary ports before deleting their security group."""
    for port_id in reversed(port_ids):
        try:
            port_manager.cleanup(port_id)
        except Exception as error:
            logger.warning(
                "Could not delete temporary Packer port %s: %s",
                port_id,
                error,
            )


def _cleanup_temporary_packer_security_group(
    security_group_name: str,
    security_group_manager: BuildSecurityGroupManager,
    logger: logging.Logger,
) -> None:
    """Best-effort delete temporary SSH security-group access."""
    try:
        security_group_manager.cleanup(security_group_name)
    except Exception as error:
        logger.warning(
            "Could not delete temporary Packer SSH security group %s: %s",
            security_group_name,
            error,
        )


_DEFAULT_BUILD_SECURITY_GROUP_MANAGER = _OpenStackBuildSecurityGroupManager()
_DEFAULT_BUILD_PORT_MANAGER = _OpenStackBuildPortManager()


def build_image(
    config: ClusterConfig,
    bundle: Bundle,
    *,
    runner: PackerRunner = _run_packer,
    template_path: Path = PACKER_TEMPLATE_PATH,
    logger: logging.Logger | None = None,
    network_resolver: NetworkResolver = _resolve_openstack_network_id,
    security_group_manager: BuildSecurityGroupManager = (
        _DEFAULT_BUILD_SECURITY_GROUP_MANAGER
    ),
    port_manager: BuildPortManager = _DEFAULT_BUILD_PORT_MANAGER,
) -> str:
    """Run packer build using config.packer settings and return the image ID."""
    active_logger = logger or logging.getLogger(__name__)
    resolved_template_path = template_path.resolve()
    _validate_packer_assets(resolved_template_path)
    network_id = _resolve_packer_network_id(
        config.cluster.network_name,
        network_resolver,
    )
    lustre_network_id = _resolve_optional_packer_network_id(
        config.cluster.lustre_network,
        network_resolver,
    )
    _log_packer_networking(active_logger, config, network_id, lustre_network_id)
    packer_config = config.validate_for_command("build-image").packer
    assert packer_config is not None
    floating_ip_pool, _ = _packer_floating_ip_pool(config, packer_config)

    with _temporary_packer_networking(
        floating_ip_pool=floating_ip_pool,
        network_id=network_id,
        security_group_manager=security_group_manager,
        port_manager=port_manager,
        logger=active_logger,
    ) as port_ids:
        active_logger.info("Packer starting")
        with _normalized_relative_packer_log_path():
            result = runner(
                _packer_command(
                    resolved_template_path,
                    _packer_vars(
                        config,
                        bundle,
                        network_id=network_id,
                        lustre_network_id=lustre_network_id,
                        port_ids=port_ids,
                    ),
                ),
                cwd=resolved_template_path.parent,
            )
    if result.returncode != 0:
        raise PackerError(
            _packer_failure_detail(
                result,
                network_name=config.cluster.network_name,
            )
        )

    image_id = _extract_image_id(result.stdout)
    active_logger.info("image uploaded")

    return image_id


__all__ = [
    "PACKER_ROOT_PATH",
    "PACKER_SCRIPTS_PATH",
    "PACKER_TEMPLATE_PATH",
    "REQUIRED_PACKER_ASSET_PATHS",
    "REQUIRED_PACKER_SCRIPT_PATHS",
    "build_image",
]
