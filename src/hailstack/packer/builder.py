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
import signal
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Protocol
from uuid import UUID

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
REQUIRED_PACKER_SCRIPT_PATHS = tuple(
    PACKER_ROOT_PATH / relative_path
    for relative_path in REQUIRED_PACKER_SCRIPT_RELATIVE_PATHS
)
_MAX_PACKER_DIAGNOSTIC_LINES = 8
_PACKER_MONITOR_POLL_SECONDS = 0.05
_PACKER_INTERRUPT_GRACE_SECONDS = 5.0
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


@dataclass(frozen=True)
class _PackerOutputEvent:
    """Represent one line captured from a live Packer output stream."""

    stream_name: str
    line: str


class _OpenStackNetworkShow(BaseModel):
    """Represent the fields Hailstack needs from OpenStack network JSON."""

    model_config = ConfigDict(extra="allow")

    id: str = Field(validation_alias=AliasChoices("id", "ID"))


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
    """Collect live Packer output and stop early for known unreachable SSH routes."""
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
        no_route_line = _drain_packer_output_events(
            events,
            stdout_lines,
            stderr_lines,
        )
        if no_route_line is not None:
            break

        no_route_line, log_position = _read_packer_log_for_no_route(
            monitored_log_path,
            log_position,
        )
        if no_route_line is not None:
            break
        time.sleep(_PACKER_MONITOR_POLL_SECONDS)

    returncode = (
        _interrupt_packer_process(process)
        if no_route_line is not None
        else process.wait()
    )
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


def _interrupt_packer_process(process: subprocess.Popen[str]) -> int:
    """Ask Packer to stop, then escalate if it does not exit promptly."""
    _send_packer_signal(process, signal.SIGINT)
    try:
        return process.wait(timeout=_PACKER_INTERRUPT_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        _send_packer_signal(process, signal.SIGTERM)

    try:
        return process.wait(timeout=_PACKER_INTERRUPT_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=_PACKER_INTERRUPT_GRACE_SECONDS)


def _send_packer_signal(
    process: subprocess.Popen[str],
    requested_signal: signal.Signals,
) -> None:
    """Send a signal to the Packer process or process group."""
    try:
        if os.name == "posix":
            os.killpg(process.pid, requested_signal)
            return
        if requested_signal == signal.SIGINT:
            process.terminate()
            return
        if requested_signal == signal.SIGTERM:
            process.terminate()
            return
        process.kill()
    except ProcessLookupError:
        return


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
        "base_image": packer_config.base_image,
        "ssh_username": config.cluster.ssh_username,
        "flavor": packer_config.flavour,
        "network": network_id,
        "floating_ip_pool": floating_ip_pool,
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
) -> None:
    """Log Packer networking choices without exposing credentials."""
    packer_config = config.validate_for_command("build-image").packer
    assert packer_config is not None
    floating_ip_pool, source = _packer_floating_ip_pool(config, packer_config)

    logger.info("Packer OpenStack network: %s", config.cluster.network_name)
    logger.info("Packer OpenStack network UUID: %s", network_id)
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
        f"temporary build instance fixed IP `{host}`"
        if host is not None
        else "temporary build instance fixed IP"
    )
    network = (
        f"`cluster.network_name` (`{network_name}`)"
        if network_name is not None
        else "`cluster.network_name`"
    )
    return (
        f"Packer could not SSH to the {target}: no route to host. "
        f"The Hailstack runner cannot reach the build instance on {network}. "
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


def _required_packer_script_paths(template_path: Path) -> tuple[Path, ...]:
    """Return the script paths required by the checked-in packer template."""
    return tuple(
        template_path.parent / relative_path
        for relative_path in REQUIRED_PACKER_SCRIPT_RELATIVE_PATHS
    )


def _validate_packer_assets(template_path: Path) -> None:
    """Ensure the template and its provisioner scripts exist and are executable."""
    missing_paths: list[str] = []
    non_executable_paths: list[str] = []

    if not template_path.is_file():
        missing_paths.append(str(template_path))

    for script_path in _required_packer_script_paths(template_path):
        if not script_path.is_file():
            missing_paths.append(str(script_path))
            continue
        if not os.access(script_path, os.X_OK):
            non_executable_paths.append(str(script_path))

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


def build_image(
    config: ClusterConfig,
    bundle: Bundle,
    *,
    runner: PackerRunner = _run_packer,
    template_path: Path = PACKER_TEMPLATE_PATH,
    logger: logging.Logger | None = None,
    network_resolver: NetworkResolver = _resolve_openstack_network_id,
) -> str:
    """Run packer build using config.packer settings and return the image ID."""
    active_logger = logger or logging.getLogger(__name__)
    resolved_template_path = template_path.resolve()
    _validate_packer_assets(resolved_template_path)
    network_id = _resolve_packer_network_id(
        config.cluster.network_name,
        network_resolver,
    )
    _log_packer_networking(active_logger, config, network_id)
    active_logger.info("Packer starting")

    with _normalized_relative_packer_log_path():
        result = runner(
            _packer_command(
                resolved_template_path,
                _packer_vars(config, bundle, network_id=network_id),
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
    "REQUIRED_PACKER_SCRIPT_PATHS",
    "build_image",
]
