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

"""Build-image command for the hailstack CLI."""

import logging
import sys
from pathlib import Path
from typing import Annotated

import typer

from hailstack.cli.commands._bundle_validation import (
    validate_command_config_bundle as _validate_command_config_bundle,
)
from hailstack.config.compatibility import Bundle, CompatibilityMatrix
from hailstack.config.parser import load_config
from hailstack.config.schema import ClusterConfig
from hailstack.config.validator import validate_bundle
from hailstack.packer.builder import build_image

DEFAULT_COMPATIBILITY_MATRIX_PATH = Path(
    __file__).resolve().parents[4] / "bundles.toml"


def get_build_logger() -> logging.Logger:
    """Return a dedicated stderr logger for build-image progress messages."""
    logger = logging.getLogger("hailstack.build-image")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger


def _resolve_bundle(
    matrix: CompatibilityMatrix,
    config: ClusterConfig,
    override_bundle: str | None,
) -> Bundle:
    """Resolve the bundle override first, then fall back to config or default."""
    if override_bundle is not None and override_bundle.strip():
        return matrix.get_bundle(override_bundle.strip())

    return validate_bundle(config, matrix)


def validate_command_config_bundle(
    command: str,
    config_path: Path,
    dotenv_file: Path | None = None,
) -> Bundle | None:
    """Preserve the phase-1 helper contract for existing tests and callers."""
    return _validate_command_config_bundle(command, config_path, dotenv_file)


def build_image_cmd(
    config: Annotated[
        Path,
        typer.Option(
            "--config", help="Path to cluster configuration TOML file."),
    ] = Path("./hailstack.toml"),
    bundle: Annotated[
        str | None,
        typer.Option("--bundle", help="Bundle ID (default: from config)."),
    ] = None,
    dotenv: Annotated[
        Path | None,
        typer.Option(
            "--dotenv",
            help="Load environment variables from a .env file before parsing config.",
        ),
    ] = None,
) -> None:
    """Build a cluster image for a selected bundle."""
    logger = get_build_logger()
    loaded_config = load_config(config, dotenv)
    logger.info("config loaded")

    matrix = CompatibilityMatrix(DEFAULT_COMPATIBILITY_MATRIX_PATH)
    resolved_bundle = _resolve_bundle(matrix, loaded_config, bundle)
    logger.info("bundle resolved")

    loaded_config = loaded_config.validate_for_command("build-image")

    image_id = build_image(loaded_config, resolved_bundle, logger=logger)
    typer.echo(image_id)


build_image_command = build_image_cmd


__all__ = [
    "DEFAULT_COMPATIBILITY_MATRIX_PATH",
    "build_image_cmd",
    "build_image_command",
    "get_build_logger",
    "validate_command_config_bundle",
]
