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

"""Create command for the hailstack CLI."""

import json
import logging
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Annotated, Protocol, cast

import typer

from hailstack.cli.commands._bundle_validation import (
    validate_command_config_bundle as _validate_command_config_bundle,
)
from hailstack.config.compatibility import Bundle, CompatibilityMatrix
from hailstack.config.parser import load_config
from hailstack.config.schema import ClusterConfig
from hailstack.config.validator import validate_bundle
from hailstack.errors import (
    ConfigError,
    ImageNotFoundError,
    NetworkError,
    PulumiError,
    QuotaExceededError,
    ResourceNotFoundError,
)
from hailstack.pulumi.stack import AutomationStackRunner
from hailstack.runtime_paths import BUNDLES_TOML_PATH

DEFAULT_COMPATIBILITY_MATRIX_PATH = BUNDLES_TOML_PATH
_RETRY_BACKOFF_SECONDS = (1.0, 2.0, 4.0)
_TRANSIENT_OPENSTACK_ERROR_MARKERS = (
    "timed out",
    "timeout",
    "temporarily unavailable",
    "temporary failure",
    "connection reset",
    "connection refused",
    "connection aborted",
    "connection closed",
    "no route to host",
    "service unavailable",
    "bad gateway",
    "gateway timeout",
    "internal server error",
    "endpoint not found",
    "endpoint for",
    "unable to establish connection",
)


@dataclass(frozen=True)
class FlavorDetails:
    """Represent the OpenStack flavour data needed for quota checks."""

    vcpus: int
    ram_mb: int


@dataclass(frozen=True)
class ComputeQuota:
    """Represent available compute quota for the current project."""

    instances_available: int
    cores_available: int
    ram_mb_available: int


@dataclass(frozen=True)
class VolumeQuota:
    """Represent available volume quota for the current project."""

    gigabytes_available: int


class OpenStackPreflightClient(Protocol):
    """Define the OpenStack lookups required for create pre-flight checks."""

    def get_image(self, name: str) -> object | None:
        """Return a truthy record when the named image exists."""
        ...

    def get_flavour(self, name: str) -> FlavorDetails | None:
        """Return flavour details when the named flavour exists."""
        ...

    def get_network(self, name: str) -> object | None:
        """Return a truthy record when the named network exists."""
        ...

    def floating_ip_is_available(self, address: str) -> bool:
        """Report whether a configured floating IP exists and is unassociated."""
        ...

    def volume_exists(self, volume_id: str) -> bool:
        """Report whether a referenced Cinder volume exists."""
        ...

    def volume_is_available(self, volume_id: str) -> bool:
        """Report whether a referenced Cinder volume can be attached now."""
        ...

    def volume_is_attached_to_server(self, volume_id: str, server_name: str) -> bool:
        """Report whether a referenced Cinder volume is attached to one server."""
        ...

    def attached_volume_size_gb(
        self,
        server_name: str,
        *,
        volume_name: str,
    ) -> int | None:
        """Return the size of a named volume attached to the given server."""
        ...

    def get_compute_quota(self) -> ComputeQuota:
        """Return currently available compute quota."""
        ...

    def get_volume_quota(self) -> VolumeQuota:
        """Return currently available block-storage quota."""
        ...


class PulumiCreateRunner(Protocol):
    """Define the Pulumi interactions used by the create command."""

    def check_backend_access(self, config: ClusterConfig) -> None:
        """Validate backend access before preview or apply."""
        ...

    def stack_exists(self, config: ClusterConfig) -> bool:
        """Return whether the target stack already exists in the backend."""
        ...

    def current_master_public_ip(self, config: ClusterConfig) -> str | None:
        """Return the current stack master public IP when the stack exists."""
        ...

    def current_stack_outputs(self, config: ClusterConfig) -> Mapping[str, object]:
        """Return the current resolved stack outputs when the stack exists."""
        ...

    def preview(
        self,
        config: ClusterConfig,
        bundle: Bundle,
        *,
        stack_exists: bool | None = None,
    ) -> str:
        """Return rendered preview output."""
        ...

    def up(self, config: ClusterConfig, bundle: Bundle) -> object:
        """Apply infrastructure and return an object exposing master_public_ip."""
        ...

    def cleanup_failed_create(self, config: ClusterConfig, bundle: Bundle) -> None:
        """Destroy infrastructure created by a failed first-time create."""
        ...


class OpenStackCLIClient:
    """Query OpenStack resources through the CLI in a mockable wrapper."""

    def get_image(self, name: str) -> object | None:
        """Return a truthy record when the image exists."""
        return self._run_optional_show(
            ["openstack", "image", "show", name, "-f", "json"]
        )

    def get_flavour(self, name: str) -> FlavorDetails | None:
        """Return flavour details when the flavour exists."""
        payload = self._run_optional_show(
            ["openstack", "flavor", "show", name, "-f", "json"]
        )
        if payload is None:
            return None

        return FlavorDetails(
            vcpus=_require_int(payload, "vcpus"),
            ram_mb=_require_int(payload, "ram"),
        )

    def get_network(self, name: str) -> object | None:
        """Return a truthy record when the network exists."""
        return self._run_optional_show(
            ["openstack", "network", "show", name, "-f", "json"]
        )

    def floating_ip_is_available(self, address: str) -> bool:
        """Report whether the floating IP exists and is not bound to a port."""
        payload = self._run_optional_show(
            ["openstack", "floating", "ip", "show", address, "-f", "json"]
        )
        if payload is None:
            return False

        port_id = payload.get("Port") or payload.get("port_id")
        return not isinstance(port_id, str) or not port_id.strip()

    def volume_exists(self, volume_id: str) -> bool:
        """Report whether the named Cinder volume exists."""
        return (
            self._run_optional_show(
                ["openstack", "volume", "show", volume_id, "-f", "json"]
            )
            is not None
        )

    def volume_is_available(self, volume_id: str) -> bool:
        """Report whether the named Cinder volume is unattached and available."""
        payload = self._run_optional_show(
            ["openstack", "volume", "show", volume_id, "-f", "json"]
        )
        if payload is None:
            return False

        status = payload.get("status")
        attachments = payload.get("attachments")
        return (
            isinstance(status, str)
            and status.strip().lower() == "available"
            and not _volume_has_attachments(attachments)
        )

    def volume_is_attached_to_server(self, volume_id: str, server_name: str) -> bool:
        """Report whether the named Cinder volume is attached to one server."""
        payload = self._run_optional_show(
            ["openstack", "volume", "show", volume_id, "-f", "json"]
        )
        if payload is None:
            return False

        server_id = self._server_id(server_name)
        if server_id is None:
            return False

        return _attachments_include_server_id(payload.get("attachments"), server_id)

    def attached_volume_size_gb(
        self,
        server_name: str,
        *,
        volume_name: str,
    ) -> int | None:
        """Return the size of one named volume currently attached to a server."""
        server_payload = self._run_optional_show(
            ["openstack", "server", "show", server_name, "-f", "json"]
        )
        if server_payload is None:
            return None

        for volume_id in _attached_volume_ids(
            server_payload.get("volumes_attached")
            or server_payload.get("volumes attached")
        ):
            volume_payload = self._run_optional_show(
                ["openstack", "volume", "show", volume_id, "-f", "json"]
            )
            if volume_payload is None:
                continue
            current_name = volume_payload.get(
                "name") or volume_payload.get("Name")
            if not isinstance(current_name, str) or current_name.strip() != volume_name:
                continue
            return _require_int(volume_payload, "size")

        return None

    def get_compute_quota(self) -> ComputeQuota:
        """Return the currently available project compute quota."""
        payload = self._run_required_show(
            ["openstack", "limits", "show", "--absolute", "-f", "json"]
        )
        return ComputeQuota(
            instances_available=_available_limit(
                payload,
                max_key="maxTotalInstances",
                used_key="totalInstancesUsed",
            ),
            cores_available=_available_limit(
                payload,
                max_key="maxTotalCores",
                used_key="totalCoresUsed",
            ),
            ram_mb_available=_available_limit(
                payload,
                max_key="maxTotalRAMSize",
                used_key="totalRAMUsed",
            ),
        )

    def get_volume_quota(self) -> VolumeQuota:
        """Return the currently available project volume quota."""
        payload = self._run_required_show(
            ["openstack", "limits", "show", "--absolute", "-f", "json"]
        )
        return VolumeQuota(
            gigabytes_available=_available_limit(
                payload,
                max_key="maxTotalVolumeGigabytes",
                used_key="totalGigabytesUsed",
            )
        )

    def _run_optional_show(self, command: list[str]) -> dict[str, object] | None:
        """Run an OpenStack show command and return JSON on success only."""
        return self._run_show(command, allow_not_found=True)

    def _run_required_show(self, command: list[str]) -> dict[str, object]:
        """Run an OpenStack command and require a successful JSON response."""
        payload = self._run_show(command, allow_not_found=False)
        assert payload is not None
        return payload

    def _run_show(
        self,
        command: list[str],
        *,
        allow_not_found: bool,
    ) -> dict[str, object] | None:
        """Run an OpenStack command with retries for transient control-plane errors."""
        for attempt, backoff_seconds in enumerate((0.0, *_RETRY_BACKOFF_SECONDS)):
            if attempt > 0:
                sleep(backoff_seconds)
            try:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    check=False,
                    text=True,
                )
            except FileNotFoundError as error:
                raise NetworkError("OpenStack CLI not found") from error

            if result.returncode == 0:
                return _parse_json_mapping(result.stdout, "OpenStack CLI")

            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            if _looks_like_transient_openstack_error(detail) and attempt < len(
                _RETRY_BACKOFF_SECONDS
            ):
                continue
            if allow_not_found and _openstack_show_failed_not_found(detail):
                return None
            raise NetworkError(f"OpenStack command failed: {detail}")

        raise AssertionError("OpenStack retry loop exhausted unexpectedly")

    def _server_id(self, server_name: str) -> str | None:
        """Return the UUID for a server name when it exists."""
        payload = self._run_optional_show(
            ["openstack", "server", "show", server_name, "-f", "json"]
        )
        if payload is None:
            return None

        server_id = payload.get("id") or payload.get("ID")
        if not isinstance(server_id, str) or not server_id.strip():
            return None
        return server_id.strip()


def _openstack_show_failed_not_found(detail: str) -> bool:
    """Return true when an OpenStack show failure represents a missing resource."""
    lowered = detail.lower()
    not_found_markers = (
        "no image with a name or id",
        "no flavor with a name or id",
        "no network with a name or id",
        "no floating ip found",
        "no volume with a name or id",
        "could not find resource",
        "not found",
    )
    return any(marker in lowered for marker in not_found_markers)


def _looks_like_transient_openstack_error(detail: str) -> bool:
    """Return whether an OpenStack CLI failure is worth retrying."""
    lowered = detail.lower()
    return any(marker in lowered for marker in _TRANSIENT_OPENSTACK_ERROR_MARKERS)


def get_create_logger() -> logging.Logger:
    """Return a dedicated stderr logger for create progress messages."""
    logger = logging.getLogger("hailstack.create")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger


def create_openstack_preflight_client() -> OpenStackPreflightClient:
    """Create the default OpenStack pre-flight client."""
    return OpenStackCLIClient()


def create_pulumi_stack_runner(logger: logging.Logger) -> PulumiCreateRunner:
    """Create the default Pulumi stack runner."""
    return AutomationStackRunner(logger)


def validate_command_config_bundle(
    command: str,
    config_path: Path,
    dotenv_file: Path | None = None,
) -> Bundle | None:
    """Preserve the phase-1 helper contract for existing tests and callers."""
    return _validate_command_config_bundle(command, config_path, dotenv_file)


def _resolve_bundle(matrix: CompatibilityMatrix, config: ClusterConfig) -> Bundle:
    """Resolve the create bundle from config or the matrix default."""
    return validate_bundle(config, matrix)


def _run_preflight_validation(
    config: ClusterConfig,
    bundle: Bundle,
    client: OpenStackPreflightClient,
    *,
    expected_attached_floating_ip: str | None = None,
    current_stack_outputs: Mapping[str, object] | None = None,
    skip_backend_dependent_checks: bool = False,
) -> list[str]:
    """Validate required resources and quotas before running Pulumi."""
    image_name = f"hailstack-{bundle.id}"
    missing_resources: list[str] = []
    quota_breaches: list[str] = []
    warnings: list[str] = []

    if client.get_image(image_name) is None:
        missing_resources.append(f"image '{image_name}'")

    flavour_specs = _resolved_flavour_specs(config, client, missing_resources)

    if client.get_network(config.cluster.network_name) is None:
        missing_resources.append(f"network '{config.cluster.network_name}'")
    lustre_network = config.cluster.lustre_network.strip()
    if lustre_network and client.get_network(lustre_network) is None:
        missing_resources.append(f"network '{lustre_network}'")

    floating_ip = config.cluster.floating_ip.strip()
    master_server_name = f"{config.cluster.name}-master"
    if (
        floating_ip
        and floating_ip != (expected_attached_floating_ip or "")
        and not client.floating_ip_is_available(floating_ip)
    ):
        message = f"floating IP '{floating_ip}' is not currently available"
        if skip_backend_dependent_checks:
            warnings.append(message)
        else:
            missing_resources.append(f"floating IP '{floating_ip}'")

    existing_volume_id = config.volumes.existing_volume_id.strip()
    current_attached_volume_id = None
    if current_stack_outputs is not None:
        if "attached_volume_id" in current_stack_outputs:
            current_attached_volume_id = _optional_output_str(
                current_stack_outputs,
                "attached_volume_id",
            )
        elif existing_volume_id and client.volume_is_attached_to_server(
            existing_volume_id,
            master_server_name,
        ):
            current_attached_volume_id = existing_volume_id
    if existing_volume_id:
        if not client.volume_exists(existing_volume_id):
            missing_resources.append(f"volume '{existing_volume_id}'")
        elif existing_volume_id != (
            current_attached_volume_id or ""
        ) and not client.volume_is_available(existing_volume_id):
            message = f"volume '{existing_volume_id}' is not available for attachment"
            if skip_backend_dependent_checks:
                warnings.append(message)
            else:
                missing_resources.append(message)

    compute_quota = client.get_compute_quota()
    required_instances = config.cluster.num_workers + 1
    required_cores = 0
    required_ram_mb = 0
    if (master_flavour := flavour_specs.get(config.cluster.master_flavour)) is not None:
        required_cores += master_flavour.vcpus
        required_ram_mb += master_flavour.ram_mb
    if (worker_flavour := flavour_specs.get(config.cluster.worker_flavour)) is not None:
        required_cores += worker_flavour.vcpus * config.cluster.num_workers
        required_ram_mb += worker_flavour.ram_mb * config.cluster.num_workers

    if required_instances > compute_quota.instances_available:
        quota_message = (
            "instances: need "
            f"{required_instances}, available {compute_quota.instances_available}"
        )
        if skip_backend_dependent_checks:
            warnings.append(quota_message)
        else:
            quota_breaches.append(quota_message)

    if required_cores > compute_quota.cores_available:
        quota_message = (
            f"cores: need {required_cores}, available {compute_quota.cores_available}"
        )
        if skip_backend_dependent_checks:
            warnings.append(quota_message)
        else:
            quota_breaches.append(quota_message)
    if required_ram_mb > compute_quota.ram_mb_available:
        quota_message = (
            "ram_mb: need "
            f"{required_ram_mb}, available {compute_quota.ram_mb_available}"
        )
        if skip_backend_dependent_checks:
            warnings.append(quota_message)
        else:
            quota_breaches.append(quota_message)

    if config.volumes.create:
        volume_quota = client.get_volume_quota()
        required_gigabytes = config.volumes.size_gb
        managed_volume_name = (
            config.volumes.name.strip() or f"{config.cluster.name}-vol"
        )
        if current_stack_outputs is not None:
            current_managed_volume_size_gb = (
                _optional_output_int(
                    current_stack_outputs,
                    "managed_volume_size_gb",
                )
                if "managed_volume_size_gb" in current_stack_outputs
                else client.attached_volume_size_gb(
                    master_server_name,
                    volume_name=managed_volume_name,
                )
            )
            if current_managed_volume_size_gb is not None:
                if required_gigabytes < current_managed_volume_size_gb:
                    raise ConfigError(
                        "Managed volumes cannot be shrunk: requested "
                        f"{required_gigabytes} GiB but deployed volume is "
                        f"{current_managed_volume_size_gb} GiB"
                    )
                required_gigabytes = max(
                    required_gigabytes - current_managed_volume_size_gb,
                    0,
                )
        if required_gigabytes > volume_quota.gigabytes_available:
            quota_message = (
                "gigabytes: need "
                f"{required_gigabytes}, available "
                f"{volume_quota.gigabytes_available}"
            )
            if skip_backend_dependent_checks:
                warnings.append(quota_message)
            else:
                quota_breaches.append(quota_message)

    if not missing_resources and not quota_breaches:
        return warnings

    if missing_resources == [f"image '{image_name}'"] and not quota_breaches:
        raise ImageNotFoundError(
            f"Image '{image_name}' not found. Run: hailstack build-image"
        )

    details: list[str] = []
    if missing_resources:
        details.append(
            f"Unavailable resources: {', '.join(missing_resources)}")
    if quota_breaches:
        details.append(f"Quota exceeded: {', '.join(quota_breaches)}")
    message = "; ".join(details)
    if missing_resources:
        raise ResourceNotFoundError(message)
    raise QuotaExceededError(message)


def _resolved_flavour_specs(
    config: ClusterConfig,
    client: OpenStackPreflightClient,
    missing_resources: list[str],
) -> dict[str, FlavorDetails]:
    """Return the subset of configured flavour specs that could be resolved."""
    flavour_specs: dict[str, FlavorDetails] = {}
    for flavour_name in {config.cluster.master_flavour, config.cluster.worker_flavour}:
        flavour = client.get_flavour(flavour_name)
        if flavour is None:
            missing_resources.append(f"flavour '{flavour_name}'")
            continue
        flavour_specs[flavour_name] = flavour
    return flavour_specs


def _optional_output_str(outputs: Mapping[str, object], key: str) -> str | None:
    """Return a string stack output when present and non-empty."""
    value = outputs.get(key)
    if isinstance(value, str) and value:
        return value
    return None


def _optional_output_int(outputs: Mapping[str, object], key: str) -> int | None:
    """Return an integer stack output when present."""
    value = outputs.get(key)
    if isinstance(value, int):
        return value
    return None


def _parse_json_mapping(raw_json: str, source: str) -> dict[str, object]:
    """Parse a JSON object response into a typed mapping."""
    try:
        payload = cast(object, json.loads(raw_json))
    except json.JSONDecodeError as error:
        raise NetworkError(f"{source} returned invalid JSON") from error

    if not isinstance(payload, dict):
        raise NetworkError(f"{source} returned a non-object JSON payload")

    raw_payload = cast(dict[object, object], payload)
    return {str(key): value for key, value in raw_payload.items()}


def _require_int(payload: Mapping[str, object], key: str) -> int:
    """Extract an integer field from a JSON payload."""
    value = payload.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as error:
            raise NetworkError(
                f"OpenStack CLI response had invalid integer field '{key}'"
            ) from error
    raise NetworkError(f"OpenStack CLI response missing integer field '{key}'")


def _volume_has_attachments(value: object) -> bool:
    """Return whether a Cinder volume-show attachments field is populated."""
    if value is None:
        return False
    if isinstance(value, list):
        return bool(cast(list[object], value))
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _attachments_include_server_id(value: object, server_id: str) -> bool:
    """Return whether a volume attachments field contains one server UUID."""
    if not isinstance(value, list):
        return False

    for item in cast(list[object], value):
        if not isinstance(item, dict):
            continue
        attachment = cast(dict[object, object], item)
        attachment_server_id = attachment.get(
            "server_id") or attachment.get("serverId")
        if (
            isinstance(attachment_server_id, str)
            and attachment_server_id.strip() == server_id
        ):
            return True
    return False


def _attached_volume_ids(value: object) -> list[str]:
    """Extract attached volume IDs from an OpenStack server payload field."""
    if not isinstance(value, list):
        return []

    volume_ids: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, dict):
            continue
        attachment = cast(dict[object, object], item)
        volume_id = attachment.get("id") or attachment.get("ID")
        if isinstance(volume_id, str) and volume_id.strip():
            volume_ids.append(volume_id.strip())
    return volume_ids


def _available_limit(
    payload: Mapping[str, object],
    *,
    max_key: str,
    used_key: str,
) -> int:
    """Calculate available quota from OpenStack absolute limit fields."""
    maximum = _require_int(payload, max_key)
    if maximum < 0:
        return sys.maxsize
    used = _require_int(payload, used_key)
    return max(maximum - used, 0)


def create_command(
    config: Annotated[
        Path,
        typer.Option(
            "--config", help="Path to cluster configuration TOML file."),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Validate configuration without creating resources.",
        ),
    ] = False,
    dotenv: Annotated[
        Path | None,
        typer.Option(
            "--dotenv",
            help="Load environment variables from a .env file before parsing config.",
        ),
    ] = None,
) -> None:
    """Create a new cluster from a TOML configuration file."""
    logger = get_create_logger()
    loaded_config = load_config(config, dotenv)
    logger.info("config loaded")

    matrix = CompatibilityMatrix(DEFAULT_COMPATIBILITY_MATRIX_PATH)
    resolved_bundle = _resolve_bundle(matrix, loaded_config)
    loaded_config = loaded_config.validate_for_command("create")
    logger.info("bundle resolved")

    pulumi_runner = create_pulumi_stack_runner(logger)
    pulumi_runner.check_backend_access(loaded_config)
    stack_already_exists = pulumi_runner.stack_exists(loaded_config)
    current_outputs = (
        pulumi_runner.current_stack_outputs(loaded_config)
        if stack_already_exists
        else None
    )

    _run_preflight_validation(
        loaded_config,
        resolved_bundle,
        create_openstack_preflight_client(),
        expected_attached_floating_ip=(
            _optional_output_str(current_outputs, "master_public_ip")
            if current_outputs is not None
            else None
        ),
        current_stack_outputs=current_outputs,
    )
    logger.info("pre-flight passed")

    if dry_run:
        preview_output = pulumi_runner.preview(
            loaded_config,
            resolved_bundle,
            stack_exists=stack_already_exists,
        )
        typer.echo(preview_output, nl=not preview_output.endswith("\n"))
        return

    logger.info("creating infrastructure")
    try:
        result = pulumi_runner.up(loaded_config, resolved_bundle)
    except PulumiError as error:
        if not stack_already_exists:
            try:
                pulumi_runner.cleanup_failed_create(
                    loaded_config, resolved_bundle)
            except PulumiError as cleanup_error:
                raise PulumiError(
                    f"{error}; cleanup after failed create also failed: {cleanup_error}"
                ) from error
        raise

    master_public_ip = getattr(result, "master_public_ip", None)
    if not isinstance(master_public_ip, str) or not master_public_ip:
        raise PulumiError(
            "Create result did not contain a master_public_ip value")

    logger.info("cluster ready")
    typer.echo(
        f"Cluster '{loaded_config.cluster.name}' created. Master IP: {master_public_ip}"
    )


__all__ = ["create_command", "validate_command_config_bundle"]
