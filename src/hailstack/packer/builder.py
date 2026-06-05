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

import logging
import os
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

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


def _run_packer(
    command: list[str],
    *,
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    """Execute a Packer build command in a mockable wrapper."""
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        cwd=cwd,
    )


def _packer_vars(config: ClusterConfig, bundle: Bundle) -> dict[str, str]:
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
        "network": config.cluster.network_name,
        "floating_ip_pool": floating_ip_pool,
    }


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
) -> None:
    """Log Packer networking choices without exposing credentials."""
    packer_config = config.validate_for_command("build-image").packer
    assert packer_config is not None
    floating_ip_pool, source = _packer_floating_ip_pool(config, packer_config)

    logger.info("Packer OpenStack network: %s", config.cluster.network_name)
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


def _packer_failure_detail(result: subprocess.CompletedProcess[str]) -> str:
    """Render readable Packer diagnostics while preserving raw command output."""
    raw_output = _raw_packer_output(result)
    if not raw_output:
        return "Packer build failed: unknown error"

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


def build_image(
    config: ClusterConfig,
    bundle: Bundle,
    *,
    runner: PackerRunner = _run_packer,
    template_path: Path = PACKER_TEMPLATE_PATH,
    logger: logging.Logger | None = None,
) -> str:
    """Run packer build using config.packer settings and return the image ID."""
    active_logger = logger or logging.getLogger(__name__)
    resolved_template_path = template_path.resolve()
    _validate_packer_assets(resolved_template_path)
    _log_packer_networking(active_logger, config)
    active_logger.info("Packer starting")

    result = runner(
        _packer_command(resolved_template_path, _packer_vars(config, bundle)),
        cwd=resolved_template_path.parent,
    )
    if result.returncode != 0:
        raise PackerError(_packer_failure_detail(result))

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
