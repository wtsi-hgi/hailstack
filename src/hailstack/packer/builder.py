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
from hailstack.config.schema import ClusterConfig
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


class PackerRunner(Protocol):
    """Define the callable shape used to execute the Packer CLI."""

    def __call__(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        """Run a prepared Packer command and return its completed process."""
        ...


def _run_packer(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Execute a Packer build command in a mockable wrapper."""
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _packer_vars(config: ClusterConfig, bundle: Bundle) -> dict[str, str]:
    """Build the documented Packer variable mapping for a bundle."""
    packer_config = config.validate_for_command("build-image").packer
    assert packer_config is not None

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
        "floating_ip_pool": packer_config.floating_ip_pool,
    }


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
    _validate_packer_assets(template_path)
    active_logger.info("Packer starting")

    result = runner(_packer_command(template_path, _packer_vars(config, bundle)))
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
        raise PackerError(f"Packer build failed: {detail}")

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
