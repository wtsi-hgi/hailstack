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

"""Install packages on an existing cluster."""

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import sleep
from typing import Annotated, Protocol, cast

import typer

from hailstack.ansible.runner import (
    NodeResult as PlaybookNodeResult,
)
from hailstack.ansible.runner import (
    run_install_playbook,
)
from hailstack.config.parser import load_config
from hailstack.config.schema import ClusterConfig
from hailstack.errors import ConfigError, PulumiError
from hailstack.pulumi.stack import REPOSITORY_ROOT, AutomationStackRunner
from hailstack.storage.rollout import (
    NodeResult as StoredNodeResult,
)
from hailstack.storage.rollout import (
    RolloutManifest as StoredRolloutManifest,
)
from hailstack.storage.rollout import (
    upload_rollout,
)

type PackageFileValue = str | list[str] | dict[str, "PackageFileValue"]

_PACKAGE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+")
_RETRY_BACKOFF_SECONDS = (1.0, 2.0, 4.0)
_PACKAGE_IMPORT_NAME_ALIASES = {
    "pyyaml": "yaml",
    "python-dateutil": "dateutil",
    "scikit-image": "skimage",
    "scikit-learn": "sklearn",
}


@dataclass(frozen=True)
class InstallNode:
    """Represent a single install target resolved from Pulumi outputs."""

    name: str
    host: str
    role: str
    jump_host: str | None = None


@dataclass(frozen=True)
class InstallNodeResult:
    """Represent the command-level rollout result for a single node."""

    node_name: str
    host: str
    success: bool
    system_packages: list[str]
    python_packages: list[str]
    smoke_test: str | None
    verification: dict[str, object]
    error: str = ""
    changed: bool = False
    attempts: int = 1


@dataclass(frozen=True)
class RolloutManifest:
    """Represent the install rollout metadata recorded for the command."""

    cluster_name: str
    timestamp: str
    system_packages: list[str]
    python_packages: list[str]
    smoke_test: str | None
    ssh_key_path: str | None
    node_results: list[InstallNodeResult]
    success_count: int
    failure_count: int
    sha256: str


class InstallNodeResultLike(Protocol):
    """Describe the per-node result shape accepted from install executors."""

    node_name: str
    host: str
    success: bool
    system_packages: Sequence[str]
    python_packages: Sequence[str]
    smoke_test: str | None
    verification: Mapping[str, object]
    error: str
    changed: bool
    attempts: int


class InstallStackRunner(Protocol):
    """Define the Pulumi stack lookup used by the install command."""

    def get_install_outputs(self, config: ClusterConfig) -> Mapping[str, object]:
        """Return the outputs required to build an install inventory."""
        ...


class InstallExecutor(Protocol):
    """Define the install-execution seam used by the command."""

    def run_install(
        self,
        *,
        inventory: Sequence[InstallNode],
        system_packages: Sequence[str],
        python_packages: Sequence[str],
        ssh_username: str,
        ssh_key_path: Path | None,
        smoke_test: str | None,
    ) -> Sequence[InstallNodeResultLike]:
        """Execute the install workflow for the requested nodes."""
        ...


class RolloutRecorder(Protocol):
    """Define how rollout metadata is persisted after installation."""

    def record_rollout(
        self,
        *,
        manifest: RolloutManifest,
        config: ClusterConfig,
    ) -> str:
        """Persist the manifest and return the manifest location."""
        ...


@dataclass(frozen=True)
class _ExecutorNodeResult:
    """Represent one mapped ansible node result for command processing."""

    node_name: str
    host: str
    success: bool
    system_packages: list[str]
    python_packages: list[str]
    smoke_test: str | None
    verification: dict[str, object]
    error: str
    changed: bool
    attempts: int = 1


class PulumiInstallStackRunner:
    """Resolve cluster node inventory from the Pulumi stack outputs."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initialise the runner with a shared logger."""
        self._logger = logger
        self._automation_runner = AutomationStackRunner(logger)

    def get_install_outputs(self, config: ClusterConfig) -> Mapping[str, object]:
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
            raise PulumiError(f"Unable to read Pulumi stack outputs: {detail}")

        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise PulumiError(
                "Pulumi stack output was not valid JSON") from error

        if not isinstance(payload, dict):
            raise PulumiError("Pulumi stack output must be a JSON object")

        return cast(Mapping[str, object], payload)


class AnsibleInstallExecutor:
    """Execute installs through the real ansible runner."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initialise the executor with a shared logger."""
        self._logger = logger

    def run_install(
        self,
        *,
        inventory: Sequence[InstallNode],
        system_packages: Sequence[str],
        python_packages: Sequence[str],
        ssh_username: str,
        ssh_key_path: Path | None,
        smoke_test: str | None,
    ) -> Sequence[InstallNodeResultLike]:
        """Run the install playbook and map results back to command node names."""
        inventory_by_role = _inventory_groups(inventory)
        playbook_results = run_install_playbook(
            inventory=inventory_by_role,
            system_packages=list(system_packages),
            python_packages=list(python_packages),
            ssh_username=ssh_username,
            ssh_key_path=ssh_key_path,
            worker_jump_host=_worker_jump_host(inventory),
            smoke_test=smoke_test,
        )
        playbook_by_host = {
            result.hostname: result for result in playbook_results}

        mapped_results: list[_ExecutorNodeResult] = []
        for node in inventory:
            playbook_result = playbook_by_host.get(node.host)
            if playbook_result is None:
                mapped_results.append(
                    _ExecutorNodeResult(
                        node_name=node.name,
                        host=node.host,
                        success=False,
                        system_packages=list(system_packages),
                        python_packages=list(python_packages),
                        smoke_test=smoke_test,
                        verification={"software_state_updated": False},
                        error="install runner did not return a result for the node",
                        changed=False,
                    )
                )
                continue
            mapped_results.append(
                _executor_result_from_playbook(
                    node=node,
                    result=playbook_result,
                    smoke_test=smoke_test,
                )
            )
        return cast(Sequence[InstallNodeResultLike], mapped_results)


class S3RolloutRecorder:
    """Persist rollout metadata through the Ceph S3 storage module."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initialise the recorder with a shared logger."""
        self._logger = logger

    def record_rollout(
        self,
        *,
        manifest: RolloutManifest,
        config: ClusterConfig,
    ) -> str:
        """Upload rollout artifacts to Ceph S3 and return the manifest path."""
        self._logger.debug("Uploading rollout manifest %s", manifest.sha256)
        return upload_rollout(
            manifest=_to_stored_manifest(manifest),
            node_results=_to_stored_node_results(manifest.node_results),
            ceph_s3_config=config.ceph_s3,
            cluster_name=config.cluster.name,
        )


def get_install_logger() -> logging.Logger:
    """Return a dedicated stderr logger for install progress messages."""
    logger = logging.getLogger("hailstack.install")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def create_install_stack_runner(logger: logging.Logger) -> InstallStackRunner:
    """Create the default Pulumi output resolver for installs."""
    return PulumiInstallStackRunner(logger)


def create_install_executor(logger: logging.Logger) -> InstallExecutor:
    """Create the default install executor for the command."""
    return AnsibleInstallExecutor(logger)


def create_rollout_recorder(logger: logging.Logger) -> RolloutRecorder:
    """Create the default rollout recorder for the command."""
    return S3RolloutRecorder(logger)


def _pulumi_env(config: ClusterConfig) -> dict[str, str]:
    """Build the environment required for Pulumi backend access."""
    env = dict(os.environ)
    env["AWS_ACCESS_KEY_ID"] = config.ceph_s3.access_key
    env["AWS_SECRET_ACCESS_KEY"] = config.ceph_s3.secret_key
    return env


def _ensure_ceph_s3_credentials(config: ClusterConfig) -> None:
    """Require Ceph S3 credentials before resolving outputs or recording rollout."""
    if not config.ceph_s3.has_required_credentials():
        raise ConfigError("Ceph S3 credentials required for rollout storage")


def _read_package_file(path: Path) -> tuple[list[str], list[str]]:
    """Load optional system and Python packages from a TOML file."""
    try:
        with path.open("rb") as handle:
            payload = cast(dict[str, PackageFileValue], tomllib.load(handle))
    except FileNotFoundError as error:
        raise ConfigError(f"Package file not found: {path}") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(
            f"Invalid package TOML in {path}: {error}") from error
    return (
        _package_list_from_section(payload, "system", path),
        _package_list_from_section(payload, "python", path),
    )


def _package_list_from_section(
    payload: dict[str, PackageFileValue],
    section_name: str,
    path: Path,
) -> list[str]:
    """Extract a string package list from one package-file section."""
    section = payload.get(section_name)
    if section is None:
        return []
    if not isinstance(section, dict):
        raise ConfigError(
            f"Section [{section_name}] in {path} must be a table")

    packages = section.get("packages")
    if packages is None:
        return []
    return _coerce_string_list(
        packages,
        f"Section [{section_name}] packages in {path} must be a list of strings",
    )


def _merge_packages(*groups: Sequence[str]) -> list[str]:
    """Merge package sequences while preserving order and removing duplicates."""
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for item in group:
            stripped = item.strip()
            if stripped and stripped not in seen:
                merged.append(stripped)
                seen.add(stripped)
    return merged


def _coerce_string_list(value: object, error_message: str) -> list[str]:
    """Validate and normalise a list of non-empty strings."""
    if not isinstance(value, list):
        raise ConfigError(error_message)

    items = cast(list[object], value)
    strings: list[str] = []
    for item in items:
        if not isinstance(item, str):
            raise ConfigError(error_message)
        stripped = item.strip()
        if stripped:
            strings.append(stripped)

    return strings


def _resolve_inventory(
    outputs: Mapping[str, object],
    cluster_name: str,
) -> list[InstallNode]:
    """Build install targets from the required Pulumi output values."""
    resolved_cluster_name = _require_output_str(
        outputs,
        "cluster_name",
        default=cluster_name,
    )
    master_public_host = _optional_output_str(outputs, "master_public_ip")
    master_host = master_public_host or _require_output_str(
        outputs,
        "master_private_ip",
    )
    worker_names = _require_output_str_list(outputs, "worker_names")
    worker_hosts = _require_output_str_list(outputs, "worker_private_ips")

    if len(worker_names) != len(worker_hosts):
        raise PulumiError(
            "Pulumi stack outputs contain mismatched worker names and IPs"
        )

    inventory = [
        InstallNode(
            name=f"{resolved_cluster_name}-master",
            host=master_host,
            role="master",
        )
    ]
    inventory.extend(
        InstallNode(name=name, host=host, role="worker",
                    jump_host=master_public_host)
        for name, host in zip(worker_names, worker_hosts, strict=True)
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
    return value


def _require_output_str_list(outputs: Mapping[str, object], key: str) -> list[str]:
    """Extract a list of non-empty strings from stack outputs."""
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


def _package_import_name(package: str) -> str:
    """Return the base import name for a Python package specifier."""
    match = _PACKAGE_NAME_PATTERN.match(package.strip())
    if match is None:
        return package.strip()
    distribution_name = match.group(0)
    alias = _PACKAGE_IMPORT_NAME_ALIASES.get(distribution_name.lower())
    if alias is not None:
        return alias
    return distribution_name.replace("-", "_").replace(".", "_")


def _required_verification_status(
    requested_packages: Sequence[str],
    reported_status: Mapping[str, bool],
) -> dict[str, bool]:
    """Require explicit verification status for every requested package."""
    return {
        package: reported_status.get(package, False) for package in requested_packages
    }


def _required_import_status(
    requested_packages: Sequence[str],
    reported_status: Mapping[str, bool],
) -> dict[str, bool]:
    """Require explicit import verification for every requested Python package."""
    return {
        _package_import_name(package): reported_status.get(
            _package_import_name(package),
            False,
        )
        for package in requested_packages
    }


def _all_checks_pass(status: Mapping[str, bool]) -> bool:
    """Return true when all reported verification checks passed."""
    return all(status.values())


def _verification_failure_message(
    *,
    system_status: Mapping[str, bool],
    python_status: Mapping[str, bool],
    import_status: Mapping[str, bool],
    version_status: Mapping[str, bool],
    smoke_test_requested: bool,
    smoke_test_success: bool,
    software_state_updated: bool,
) -> str:
    """Summarise the first missing or failed verification check."""
    for package, succeeded in system_status.items():
        if not succeeded:
            return f"system package verification failed: {package}"
    for package, succeeded in python_status.items():
        if not succeeded:
            return f"python package verification failed: {package}"
    for package, succeeded in version_status.items():
        if not succeeded:
            return f"python package version verification failed: {package}"
    for package, succeeded in import_status.items():
        if not succeeded:
            return f"python import verification failed: {package}"
    if smoke_test_requested and not smoke_test_success:
        return "smoke test failed"
    if not software_state_updated:
        return "software state update verification failed"
    return "install verification failed"


def _coerce_node_result(
    result: InstallNodeResultLike,
    *,
    system_packages: Sequence[str],
    python_packages: Sequence[str],
    smoke_test: str | None,
    attempts: int,
) -> InstallNodeResult:
    """Normalise executor results into the command-level rollout schema."""
    verification: dict[str, object] = dict(result.verification)
    system_status = _required_verification_status(
        system_packages,
        _coerce_bool_mapping(verification.get("system")),
    )
    python_status = _required_verification_status(
        python_packages,
        _coerce_bool_mapping(verification.get("python")),
    )
    import_status = _required_import_status(
        python_packages,
        _coerce_bool_mapping(verification.get("imports")),
    )
    version_status = _required_verification_status(
        python_packages,
        _coerce_bool_mapping(verification.get("versions")),
    )

    verification["system"] = system_status
    verification["python"] = python_status
    verification["imports"] = import_status
    verification["versions"] = version_status
    smoke_test_success = True
    if smoke_test is not None:
        smoke_test_success = _coerce_bool(
            verification.get("smoke_test"),
            default=False,
        )
        verification["smoke_test"] = smoke_test_success
    software_state_updated = _coerce_bool(
        verification.get("software_state_updated"),
        default=False,
    )
    verification["software_state_updated"] = software_state_updated
    success = (
        result.success
        and _all_checks_pass(system_status)
        and _all_checks_pass(python_status)
        and _all_checks_pass(import_status)
        and _all_checks_pass(version_status)
        and (smoke_test is None or smoke_test_success)
        and software_state_updated
    )
    error = result.error
    if not success and not error:
        error = _verification_failure_message(
            system_status=system_status,
            python_status=python_status,
            import_status=import_status,
            version_status=version_status,
            smoke_test_requested=smoke_test is not None,
            smoke_test_success=smoke_test_success,
            software_state_updated=software_state_updated,
        )

    return InstallNodeResult(
        node_name=result.node_name,
        host=result.host,
        success=success,
        system_packages=list(result.system_packages),
        python_packages=list(result.python_packages),
        smoke_test=result.smoke_test,
        verification=verification,
        error=error,
        changed=result.changed,
        attempts=max(result.attempts, attempts),
    )


def _coerce_bool_mapping(value: object) -> dict[str, bool]:
    """Return a string-to-bool mapping from arbitrary result metadata."""
    if not isinstance(value, Mapping):
        return {}

    mapping: dict[str, bool] = {}
    mapping_value = cast(Mapping[object, object], value)
    for key, item in mapping_value.items():
        if isinstance(key, str) and isinstance(item, bool):
            mapping[key] = item
    return mapping


def _coerce_bool(value: object, *, default: bool) -> bool:
    """Return a boolean metadata value or the provided default."""
    if isinstance(value, bool):
        return value
    return default


def _inventory_groups(inventory: Sequence[InstallNode]) -> dict[str, list[str]]:
    """Group inventory hosts into the ansible inventory structure."""
    groups: dict[str, list[str]] = {"master": [], "worker": []}
    for node in inventory:
        groups[node.role].append(node.host)
    return groups


def _worker_jump_host(inventory: Sequence[InstallNode]) -> str | None:
    """Return the shared worker jump host when one is configured."""
    for node in inventory:
        if node.role == "worker" and node.jump_host:
            return node.jump_host
    return None


def _executor_result_from_playbook(
    *,
    node: InstallNode,
    result: PlaybookNodeResult,
    smoke_test: str | None,
) -> _ExecutorNodeResult:
    """Map one playbook result into the command result shape."""
    error = "; ".join(result.errors)
    return _ExecutorNodeResult(
        node_name=node.name,
        host=node.host,
        success=result.success,
        system_packages=list(result.system_installed),
        python_packages=list(result.python_installed),
        smoke_test=smoke_test,
        verification=dict(result.verification),
        error=error,
        changed=result.changed,
    )


def _run_install_with_retries(
    executor: InstallExecutor,
    *,
    inventory: Sequence[InstallNode],
    system_packages: Sequence[str],
    python_packages: Sequence[str],
    ssh_username: str,
    ssh_key_path: Path | None,
    smoke_test: str | None,
    logger: logging.Logger,
) -> list[InstallNodeResult]:
    """Execute installs, retrying only the failed nodes with backoff."""
    node_by_name = {node.name: node for node in inventory}
    pending_inventory = list(inventory)
    final_results: dict[str, InstallNodeResult] = {}
    attempt_number = 1

    while pending_inventory:
        raw_results = executor.run_install(
            inventory=pending_inventory,
            system_packages=system_packages,
            python_packages=python_packages,
            ssh_username=ssh_username,
            ssh_key_path=ssh_key_path,
            smoke_test=smoke_test,
        )
        results = [
            _coerce_node_result(
                result,
                system_packages=system_packages,
                python_packages=python_packages,
                smoke_test=smoke_test,
                attempts=attempt_number,
            )
            for result in raw_results
        ]
        for result in results:
            final_results[result.node_name] = result

        failed_names = [
            result.node_name for result in results if not result.success]
        if not failed_names:
            break

        if attempt_number > len(_RETRY_BACKOFF_SECONDS):
            break

        delay = _RETRY_BACKOFF_SECONDS[attempt_number - 1]
        logger.info("retrying failed nodes in %.0fs", delay)
        sleep(delay)
        pending_inventory = [node_by_name[name] for name in failed_names]
        attempt_number += 1

    return [
        final_results[node.name] for node in inventory if node.name in final_results
    ]


def _build_rollout_manifest(
    *,
    cluster_name: str,
    system_packages: Sequence[str],
    python_packages: Sequence[str],
    smoke_test: str | None,
    ssh_key_path: Path | None,
    node_results: Sequence[InstallNodeResult],
) -> RolloutManifest:
    """Create a rollout manifest and populate its SHA-256 digest."""
    timestamp = (
        datetime.now(UTC).replace(
            microsecond=0).isoformat().replace("+00:00", "Z")
    )
    success_count = sum(1 for result in node_results if result.success)
    failure_count = len(node_results) - success_count

    payload = {
        "cluster_name": cluster_name,
        "timestamp": timestamp,
        "system_packages": list(system_packages),
        "python_packages": list(python_packages),
        "smoke_test": smoke_test,
        "ssh_key_path": str(ssh_key_path) if ssh_key_path is not None else None,
        "node_results": [asdict(result) for result in node_results],
        "success_count": success_count,
        "failure_count": failure_count,
    }
    encoded_payload = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    sha256 = hashlib.sha256(encoded_payload).hexdigest()

    return RolloutManifest(
        cluster_name=cluster_name,
        timestamp=timestamp,
        system_packages=list(system_packages),
        python_packages=list(python_packages),
        smoke_test=smoke_test,
        ssh_key_path=str(ssh_key_path) if ssh_key_path is not None else None,
        node_results=list(node_results),
        success_count=success_count,
        failure_count=failure_count,
        sha256=sha256,
    )


def _to_stored_manifest(manifest: RolloutManifest) -> StoredRolloutManifest:
    """Convert the command manifest into the storage manifest model."""
    return StoredRolloutManifest(
        cluster_name=manifest.cluster_name,
        timestamp=manifest.timestamp,
        system_packages=list(manifest.system_packages),
        python_packages=list(manifest.python_packages),
        node_count=len(manifest.node_results),
        success_count=manifest.success_count,
        failure_count=manifest.failure_count,
        sha256=manifest.sha256,
    )


def _to_stored_node_results(
    node_results: Sequence[InstallNodeResult],
) -> list[StoredNodeResult]:
    """Convert command-level node results into storage node result models."""
    return [
        StoredNodeResult(
            hostname=node_result.node_name,
            success=node_result.success,
            system_installed=list(node_result.system_packages),
            python_installed=list(node_result.python_packages),
            errors=[node_result.error] if node_result.error else [],
        )
        for node_result in node_results
    ]


def _require_requested_packages(
    system_packages: Sequence[str],
    python_packages: Sequence[str],
) -> None:
    """Reject installs that do not request any packages."""
    if not system_packages and not python_packages:
        raise typer.BadParameter(
            "At least one package must be provided via --system, --python, or --file"
        )


def install(
    config: Annotated[
        Path,
        typer.Option(
            "--config", help="Path to cluster configuration TOML file."),
    ] = Path("./hailstack.toml"),
    system: Annotated[
        list[str] | None,
        typer.Option("--system", help="System packages to install with apt."),
    ] = None,
    python: Annotated[
        list[str] | None,
        typer.Option("--python", help="Python packages to install with uv."),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option("--file", help="Path to a package list TOML file."),
    ] = None,
    smoke_test: Annotated[
        str | None,
        typer.Option(
            "--smoke-test",
            help="Command to run on every node after install.",
        ),
    ] = None,
    ssh_key: Annotated[
        Path | None,
        typer.Option(
            "--ssh-key", help="SSH private key path (default: agent)."),
    ] = None,
    dotenv: Annotated[
        Path | None,
        typer.Option(
            "--dotenv",
            help="Load environment variables from a .env file before parsing config.",
        ),
    ] = None,
) -> None:
    """Install packages on all cluster nodes."""
    logger = get_install_logger()
    loaded_config = load_config(config, dotenv)
    logger.info("config loaded")

    _ensure_ceph_s3_credentials(loaded_config)

    file_system_packages: list[str] = []
    file_python_packages: list[str] = []
    if file is not None:
        file_system_packages, file_python_packages = _read_package_file(file)

    system_packages = _merge_packages(system or [], file_system_packages)
    python_packages = _merge_packages(python or [], file_python_packages)
    _require_requested_packages(system_packages, python_packages)

    logger.info("resolving nodes")
    stack_runner = create_install_stack_runner(logger)
    inventory = _resolve_inventory(
        stack_runner.get_install_outputs(loaded_config),
        loaded_config.cluster.name,
    )

    logger.info("running ansible")
    executor = create_install_executor(logger)
    node_results = _run_install_with_retries(
        executor,
        inventory=inventory,
        system_packages=system_packages,
        python_packages=python_packages,
        ssh_username=loaded_config.cluster.ssh_username,
        ssh_key_path=ssh_key,
        smoke_test=smoke_test,
        logger=logger,
    )

    logger.info("verifying packages")
    manifest = _build_rollout_manifest(
        cluster_name=loaded_config.cluster.name,
        system_packages=system_packages,
        python_packages=python_packages,
        smoke_test=smoke_test,
        ssh_key_path=ssh_key,
        node_results=node_results,
    )

    logger.info("uploading rollout manifest")
    recorder = create_rollout_recorder(logger)
    recorder.record_rollout(manifest=manifest, config=loaded_config)

    if manifest.failure_count > 0:
        raise typer.Exit(code=1)


install_command = install

__all__ = [
    "AnsibleInstallExecutor",
    "InstallExecutor",
    "InstallNode",
    "InstallNodeResult",
    "InstallNodeResultLike",
    "InstallStackRunner",
    "PulumiInstallStackRunner",
    "RolloutManifest",
    "RolloutRecorder",
    "S3RolloutRecorder",
    "create_install_executor",
    "create_install_stack_runner",
    "create_rollout_recorder",
    "get_install_logger",
    "install",
    "install_command",
]
