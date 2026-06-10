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

"""Pulumi automation helpers for cluster lifecycle commands."""

import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pulumi import automation as auto
from semver import VersionInfo

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import PulumiError, S3Error
from hailstack.pulumi.resources import create_cluster_resources
from hailstack.runtime_paths import RUNTIME_WORK_DIR, runtime_work_dir
from hailstack.tool_versions import (
    KNOWN_GOOD_PULUMI_CLI_VERSION,
    MINIMUM_PULUMI_AUTOMATION_CLI_VERSION,
)

REPOSITORY_ROOT = RUNTIME_WORK_DIR
S3_CHECKSUM_MISMATCH_ERROR = "XAmzContentSHA256Mismatch"
S3_SIGNATURE_MISMATCH_ERROR = "SignatureDoesNotMatch"
S3_CHECKSUM_COMPATIBILITY_VALUE = "when_required"
S3_REQUEST_CHECKSUM_ENV = "AWS_REQUEST_CHECKSUM_CALCULATION"
S3_RESPONSE_CHECKSUM_ENV = "AWS_RESPONSE_CHECKSUM_VALIDATION"


@dataclass(frozen=True)
class CreateResult:
    """Represent the subset of Pulumi outputs needed by the create command."""

    master_public_ip: str
    stdout: str = ""


@dataclass(frozen=True)
class PulumiCli:
    """Describe the Pulumi CLI executable selected for this runner."""

    path: Path
    version: str

    @property
    def command(self) -> str:
        """Return the executable command passed to subprocesses."""
        return str(self.path)

    @property
    def display_version(self) -> str:
        """Return a human-readable Pulumi version."""
        if self.version == "unknown":
            return "unknown"
        return f"v{self.version}"

    @property
    def parsed_version(self) -> VersionInfo | None:
        """Return the parsed Pulumi version when the CLI reported one."""
        return _parse_pulumi_cli_version(self.version)

    @property
    def is_known_good(self) -> bool:
        """Return whether this CLI matches Hailstack's known-good version."""
        return self.version == KNOWN_GOOD_PULUMI_CLI_VERSION


class _ResolvedPulumiCommand(auto.PulumiCommand):
    """Run Pulumi automation with an already resolved executable path."""

    def __init__(self, cli: PulumiCli) -> None:
        """Initialise without asking PulumiCommand to search PATH again."""
        self.command = cli.command
        self.version = _ensure_pulumi_automation_cli_compatible(cli)


class AutomationStackRunner:
    """Wrap Pulumi automation operations in a command-friendly API."""

    def __init__(
        self,
        logger: logging.Logger | None = None,
        *,
        work_dir: Path = REPOSITORY_ROOT,
    ) -> None:
        """Initialise the runner with a repository work directory."""
        self._logger = logger or logging.getLogger(__name__)
        self._work_dir = (
            runtime_work_dir() if work_dir == RUNTIME_WORK_DIR else work_dir
        )
        self._resolved_pulumi_cli: PulumiCli | None = None

    def check_backend_access(self, config: ClusterConfig) -> None:
        """Validate that the configured Ceph S3 backend accepts authentication."""
        env = self._pulumi_env(config)
        backend_url = self._backend_url(config)
        pulumi_cli = self._pulumi_cli()
        try:
            result = subprocess.run(
                [pulumi_cli.command, "login", "--non-interactive", backend_url],
                capture_output=True,
                check=False,
                cwd=self._work_dir,
                env=env,
                text=True,
            )
        except FileNotFoundError as error:
            raise PulumiError(_pulumi_not_found_message()) from error

        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            detail = _add_pulumi_backend_hint(detail, pulumi_cli, env)
            endpoint = config.ceph_s3.endpoint.removeprefix("https://").removeprefix(
                "http://"
            )
            raise S3Error(f"Unable to access Ceph S3 backend at {endpoint}: {detail}")
        _ensure_pulumi_automation_cli_compatible(pulumi_cli)

    def cli_env(self, config: ClusterConfig) -> dict[str, str]:
        """Return the environment for Pulumi CLI commands run outside automation."""
        return self._pulumi_env(config)

    def preview(
        self,
        config: ClusterConfig,
        bundle: Bundle,
        *,
        image_id: str | None = None,
        stack_exists: bool | None = None,
    ) -> str:
        """Run a Pulumi preview and return the rendered plan output."""
        if stack_exists is None:
            stack_exists = self.stack_exists(config)

        if stack_exists:
            stack = self._get_stack(
                config,
                bundle,
                create_if_missing=False,
                image_id=image_id,
            )
            output_lines: list[str] = []
            try:
                result = stack.preview(on_output=output_lines.append)
            except Exception as error:
                raise PulumiError(f"Pulumi preview failed: {error}") from error

            return result.stdout or "".join(output_lines)

        return self._preview_new_stack(config, bundle, image_id=image_id)

    def stack_exists(self, config: ClusterConfig) -> bool:
        """Return whether the configured Pulumi stack already exists."""
        try:
            self._get_stack(config, None, create_if_missing=False)
        except PulumiError as error:
            if f"hailstack-{config.cluster.name} does not exist" in str(error):
                return False
            raise
        return True

    def _preview_new_stack(
        self,
        config: ClusterConfig,
        bundle: Bundle,
        *,
        image_id: str | None = None,
    ) -> str:
        """Preview a first-time create against an ephemeral local backend."""

        def pulumi_program() -> None:
            create_cluster_resources(
                config,
                bundle,
                image_id=image_id,
                allow_missing_runtime_secrets=True,
            )

        with tempfile.TemporaryDirectory(prefix="hailstack-preview-") as temp_dir:
            stack_name = f"preview-{config.cluster.name}"
            workspace_options = auto.LocalWorkspaceOptions(
                env_vars=self._pulumi_env(config),
                project_settings=auto.ProjectSettings(
                    name="hailstack",
                    runtime="python",
                    backend=auto.ProjectBackend(url=f"file://{temp_dir}"),
                ),
                work_dir=str(self._work_dir),
                pulumi_command=self._pulumi_command(),
            )
            try:
                stack = auto.create_stack(
                    stack_name=stack_name,
                    project_name="hailstack",
                    program=pulumi_program,
                    opts=workspace_options,
                )
                output_lines: list[str] = []
                result = stack.preview(on_output=output_lines.append)
                return result.stdout or "".join(output_lines)
            except Exception as error:
                raise PulumiError(f"Pulumi preview failed: {error}") from error

    def preview_destroy(self, config: ClusterConfig) -> str:
        """Run a Pulumi preview showing the resources that would be destroyed."""
        stack = self._get_stack(config, None, create_if_missing=False)
        output_lines: list[str] = []
        try:
            result = stack.preview_destroy(on_output=output_lines.append)
        except Exception as error:
            raise PulumiError(f"Pulumi preview failed: {error}") from error

        return result.stdout or "".join(output_lines)

    def current_master_public_ip(self, config: ClusterConfig) -> str | None:
        """Return the current master public IP output for an existing stack."""
        output = self._current_output_value(config, "master_public_ip")
        if output is None:
            return None
        if not isinstance(output, str) or not output:
            return None
        return output

    def current_stack_outputs(self, config: ClusterConfig) -> dict[str, object]:
        """Return the resolved current outputs for an existing stack."""
        stack = self._get_stack(config, None, create_if_missing=False)
        return {name: output.value for name, output in stack.outputs().items()}

    def up(
        self,
        config: ClusterConfig,
        bundle: Bundle,
        *,
        image_id: str | None = None,
    ) -> CreateResult:
        """Apply the Pulumi stack and return the master floating IP output."""
        stack = self._get_stack(
            config,
            bundle,
            create_if_missing=True,
            image_id=image_id,
        )
        output_lines: list[str] = []
        try:
            result = stack.up(on_output=output_lines.append)
        except Exception as error:
            raise PulumiError(f"Pulumi up failed: {error}") from error

        master_public_ip = self._master_public_ip(result.outputs)
        stdout = result.stdout or "".join(output_lines)
        return CreateResult(master_public_ip=master_public_ip, stdout=stdout)

    def destroy(self, config: ClusterConfig, bundle: Bundle | None = None) -> None:
        """Destroy the Pulumi stack for cleanup or an explicit destroy command."""
        destroy_bundle = bundle
        retain_created_volume: bool | None = None
        if _requires_destroy_rehydration(config):
            destroy_bundle = self._bundle_for_destroy(config)
        if config.volumes.preserve_on_destroy:
            retain_created_volume = True
        self._destroy_stack(
            config,
            destroy_bundle,
            retain_created_volume=retain_created_volume,
        )

    def cleanup_failed_create(
        self,
        config: ClusterConfig,
        bundle: Bundle,
        *,
        image_id: str | None = None,
    ) -> None:
        """Destroy a failed first-time create without retaining created volumes."""
        self._destroy_stack(
            config,
            bundle,
            retain_created_volume=False,
            image_id=image_id,
        )

    def _destroy_stack(
        self,
        config: ClusterConfig,
        bundle: Bundle | None,
        *,
        retain_created_volume: bool | None = None,
        image_id: str | None = None,
    ) -> None:
        """Destroy the Pulumi stack with optional cleanup-specific ownership."""
        stack = self._get_stack(
            config,
            bundle,
            create_if_missing=False,
            retain_created_volume=retain_created_volume,
            allow_missing_runtime_secrets=True,
            image_id=image_id,
        )
        try:
            stack.destroy(remove=True)
        except Exception as error:
            raise PulumiError(f"Pulumi destroy failed: {error}") from error

    def _bundle_for_destroy(self, config: ClusterConfig) -> Bundle:
        """Build a minimal bundle for destroy-time program rehydration."""
        return Bundle(
            id=config.cluster.bundle or "unknown-bundle",
            hail="unknown",
            spark="unknown",
            hadoop="unknown",
            java="unknown",
            python="unknown",
            scala="unknown",
            gnomad="unknown",
            status="latest",
        )

    def _current_output_value(self, config: ClusterConfig, key: str) -> object | None:
        """Return one current stack output value when present."""
        return self.current_stack_outputs(config).get(key)

    def _get_stack(
        self,
        config: ClusterConfig,
        bundle: Bundle | None,
        *,
        create_if_missing: bool,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
        image_id: str | None = None,
    ) -> auto.Stack:
        """Select the cluster stack and optionally create it when missing."""

        def pulumi_program() -> None:
            if bundle is not None:
                create_cluster_resources(
                    config,
                    bundle,
                    image_id=image_id,
                    retain_created_volume=retain_created_volume,
                    allow_missing_runtime_secrets=allow_missing_runtime_secrets,
                    allow_missing_ssh_public_keys=allow_missing_runtime_secrets,
                )

        workspace_options = auto.LocalWorkspaceOptions(
            env_vars=self._pulumi_env(config),
            project_settings=auto.ProjectSettings(
                name="hailstack",
                runtime="python",
                backend=auto.ProjectBackend(url=self._backend_url(config)),
            ),
            work_dir=str(self._work_dir),
            pulumi_command=self._pulumi_command(),
        )

        try:
            stack_name = f"hailstack-{config.cluster.name}"
            if create_if_missing:
                return auto.create_or_select_stack(
                    stack_name=stack_name,
                    project_name="hailstack",
                    program=pulumi_program,
                    opts=workspace_options,
                )

            return auto.select_stack(
                stack_name=stack_name,
                project_name="hailstack",
                program=pulumi_program,
                opts=workspace_options,
            )
        except Exception as error:
            if not create_if_missing and _is_missing_stack_error(error):
                raise PulumiError(
                    f"Pulumi stack hailstack-{config.cluster.name} does not exist"
                ) from error
            raise PulumiError(f"Unable to initialise Pulumi stack: {error}") from error

    @staticmethod
    def _backend_url(config: ClusterConfig) -> str:
        """Render the documented Pulumi Ceph backend URL."""
        endpoint = _normalize_ceph_endpoint(config.ceph_s3.endpoint)
        return f"s3://{config.ceph_s3.bucket}?endpoint={endpoint}"

    def _pulumi_env(self, config: ClusterConfig) -> dict[str, str]:
        """Build the process environment required for Pulumi backend access."""
        env = dict(os.environ)
        env["AWS_ACCESS_KEY_ID"] = config.ceph_s3.access_key
        env["AWS_SECRET_ACCESS_KEY"] = config.ceph_s3.secret_key
        _set_default_s3_region(env)
        _set_default_s3_checksum_compatibility(env)
        env.setdefault("PULUMI_HOME", str(self._pulumi_home()))
        if (
            "PULUMI_CONFIG_PASSPHRASE" not in env
            and "PULUMI_CONFIG_PASSPHRASE_FILE" not in env
        ):
            env["PULUMI_CONFIG_PASSPHRASE"] = config.ceph_s3.secret_key
        return env

    def _pulumi_home(self) -> Path:
        """Return a workspace-scoped Pulumi home that avoids global backend state."""
        workspace_hash = hashlib.sha256(str(self._work_dir).encode("utf-8")).hexdigest()
        return Path(tempfile.gettempdir()) / "hailstack-pulumi-home" / workspace_hash

    def _pulumi_cli(self) -> PulumiCli:
        """Return the cached Pulumi CLI selected for this runner."""
        if self._resolved_pulumi_cli is None:
            self._resolved_pulumi_cli = self._resolve_pulumi_cli()
        return self._resolved_pulumi_cli

    def _pulumi_command(self) -> auto.PulumiCommand:
        """Return the Pulumi command object used by Automation API calls."""
        return _ResolvedPulumiCommand(self._pulumi_cli())

    def _resolve_pulumi_cli(self) -> PulumiCli:
        """Select a Pulumi CLI from PATH with a home-directory fallback."""
        for candidate in _pulumi_cli_candidates():
            return PulumiCli(
                path=candidate,
                version=self._pulumi_cli_version(candidate),
            )

        return PulumiCli(path=Path("pulumi"), version="unknown")

    def _pulumi_cli_version(self, path: Path) -> str:
        """Return the normalised version reported by a Pulumi executable."""
        env = dict(os.environ)
        env["PULUMI_SKIP_UPDATE_CHECK"] = "true"
        try:
            result = subprocess.run(
                [str(path), "version"],
                capture_output=True,
                check=False,
                cwd=self._work_dir,
                env=env,
                text=True,
            )
        except OSError:
            return "unknown"
        if result.returncode != 0:
            return "unknown"
        output = result.stdout.strip() or result.stderr.strip()
        if not output:
            return "unknown"
        return output.splitlines()[0].strip().removeprefix("v") or "unknown"

    @staticmethod
    def _master_public_ip(outputs: auto.OutputMap) -> str:
        """Extract the required master_public_ip output from a Pulumi update."""
        output = outputs.get("master_public_ip")
        if output is None or not isinstance(output.value, str) or not output.value:
            raise PulumiError(
                "Pulumi create completed without a master_public_ip output"
            )
        return output.value


def _is_missing_stack_error(error: Exception) -> bool:
    """Return true when the Pulumi automation error indicates no stack exists."""
    message = str(error).lower()
    return "not found" in message or "no stack named" in message


def _normalize_ceph_endpoint(endpoint: str) -> str:
    """Return a Pulumi-compatible Ceph endpoint URL."""
    normalized_endpoint = endpoint.rstrip("/")
    if "://" not in normalized_endpoint:
        return f"https://{normalized_endpoint}"
    return normalized_endpoint


def _set_default_s3_region(env: dict[str, str]) -> None:
    """Populate S3 region variables required by Pulumi's S3 backend."""
    region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or "us-east-1"
    if not env.get("AWS_REGION"):
        env["AWS_REGION"] = region
    if not env.get("AWS_DEFAULT_REGION"):
        env["AWS_DEFAULT_REGION"] = region


def _set_default_s3_checksum_compatibility(env: dict[str, str]) -> None:
    """Default newer AWS SDK S3 checksum behavior to Ceph-compatible settings."""
    env.setdefault(S3_REQUEST_CHECKSUM_ENV, S3_CHECKSUM_COMPATIBILITY_VALUE)
    env.setdefault(S3_RESPONSE_CHECKSUM_ENV, S3_CHECKSUM_COMPATIBILITY_VALUE)


def _parse_pulumi_cli_version(version: str) -> VersionInfo | None:
    """Return Pulumi's parsed semver object when the CLI version is known."""
    if version == "unknown":
        return None
    try:
        return VersionInfo.parse(version)
    except ValueError:
        return None


def _ensure_pulumi_automation_cli_compatible(cli: PulumiCli) -> VersionInfo:
    """Return the parsed CLI version or raise a clear Automation API error."""
    parsed_version = cli.parsed_version
    if parsed_version is None:
        raise PulumiError(
            f"Hailstack selected Pulumi CLI {cli.command} ({cli.display_version}), "
            "but could not determine a parseable Pulumi CLI version from "
            "`pulumi version`. Pulumi Automation API requires Pulumi CLI "
            f"{MINIMUM_PULUMI_AUTOMATION_CLI_VERSION} or newer. Check that this "
            "Pulumi executable runs correctly; "
            f"{KNOWN_GOOD_PULUMI_CLI_VERSION} is the known-good fallback for "
            "Ceph S3 backends."
        )
    minimum_version = VersionInfo.parse(MINIMUM_PULUMI_AUTOMATION_CLI_VERSION)
    if minimum_version.compare(parsed_version) <= 0:
        return parsed_version
    raise PulumiError(
        f"Hailstack selected Pulumi CLI {cli.command} ({cli.display_version}), "
        "but Pulumi Automation API requires Pulumi CLI "
        f"{MINIMUM_PULUMI_AUTOMATION_CLI_VERSION} or newer. Update Pulumi; "
        f"{KNOWN_GOOD_PULUMI_CLI_VERSION} is the known-good fallback for "
        "Ceph S3 backends."
    )


def _pulumi_cli_candidates() -> list[Path]:
    """Return discoverable Pulumi CLI paths in deterministic preference order."""
    candidates: list[Path] = []
    path_pulumi = shutil.which("pulumi")
    if path_pulumi is not None:
        candidates.append(Path(path_pulumi))

    home_pulumi = Path.home() / ".pulumi" / "bin" / "pulumi"
    if home_pulumi.is_file() and os.access(home_pulumi, os.X_OK):
        candidates.append(home_pulumi)

    deduped_candidates: list[Path] = []
    seen_paths: set[Path] = set()
    for candidate in candidates:
        resolved_candidate = candidate.expanduser().resolve(strict=False)
        if resolved_candidate not in seen_paths:
            deduped_candidates.append(resolved_candidate)
            seen_paths.add(resolved_candidate)
    return deduped_candidates


def _add_pulumi_backend_hint(
    detail: str,
    cli: PulumiCli,
    env: dict[str, str],
) -> str:
    """Add checksum-aware guidance for known Ceph backend failures."""
    if not any(
        error_code in detail
        for error_code in (S3_CHECKSUM_MISMATCH_ERROR, S3_SIGNATURE_MISMATCH_ERROR)
    ):
        return detail
    used = (
        f"Hailstack used Pulumi CLI {cli.command} ({cli.display_version}) "
        f"with {_s3_checksum_env_summary(env)}."
    )
    if S3_SIGNATURE_MISMATCH_ERROR in detail:
        hint = (
            f"{used} With the checksum settings shown above, signature mismatches "
            "usually mean the Ceph S3 state-bucket credentials are wrong; check "
            "the Ceph S3 state-bucket credentials loaded from "
            "ceph_s3.access_key and ceph_s3.secret_key, including values "
            "supplied through --dotenv."
        )
        return f"{detail} Hint: {hint}"

    hint = (
        f"{used} Hailstack already set AWS SDK checksum compatibility for "
        "Ceph RGW. If this backend still returns "
        f"{S3_CHECKSUM_MISMATCH_ERROR}, fall back to the known-good Pulumi CLI "
        f"{KNOWN_GOOD_PULUMI_CLI_VERSION}."
    )
    return f"{detail} Hint: {hint}"


def _s3_checksum_env_summary(env: dict[str, str]) -> str:
    """Return the checksum environment values included in Pulumi diagnostics."""
    return (
        f"{S3_REQUEST_CHECKSUM_ENV}="
        f"{env.get(S3_REQUEST_CHECKSUM_ENV, '<unset>')} and "
        f"{S3_RESPONSE_CHECKSUM_ENV}="
        f"{env.get(S3_RESPONSE_CHECKSUM_ENV, '<unset>')}"
    )


def _pulumi_not_found_message() -> str:
    """Return the Pulumi CLI missing error."""
    return (
        "Pulumi CLI not found. Install Pulumi CLI or add it to PATH. Hailstack "
        "also checks ~/.pulumi/bin/pulumi; Pulumi CLI "
        f"{KNOWN_GOOD_PULUMI_CLI_VERSION} is the known-good fallback for Ceph "
        "S3 backends."
    )


def _requires_destroy_rehydration(config: ClusterConfig) -> bool:
    """Return whether destroy must rebuild the Pulumi program."""
    floating_ip = getattr(config.cluster, "floating_ip", "")
    return config.volumes.preserve_on_destroy or bool(
        isinstance(floating_ip, str) and floating_ip.strip()
    )


__all__ = [
    "AutomationStackRunner",
    "CreateResult",
    "KNOWN_GOOD_PULUMI_CLI_VERSION",
    "REPOSITORY_ROOT",
]
