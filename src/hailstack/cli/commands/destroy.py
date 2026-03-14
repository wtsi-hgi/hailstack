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

"""Destroy command for the hailstack CLI."""

import logging
import sys
from pathlib import Path
from typing import Annotated, Protocol

import typer

from hailstack.config.parser import load_config
from hailstack.config.schema import ClusterConfig
from hailstack.errors import ConfigError
from hailstack.pulumi.stack import AutomationStackRunner


class PulumiDestroyRunner(Protocol):
    """Define the Pulumi interactions used by the destroy command."""

    def check_backend_access(self, config: ClusterConfig) -> None:
        """Validate backend access before preview or destroy."""
        ...

    def preview_destroy(self, config: ClusterConfig) -> str:
        """Return rendered destroy preview output."""
        ...

    def destroy(self, config: ClusterConfig) -> None:
        """Destroy infrastructure for the configured cluster."""
        ...


def get_destroy_logger() -> logging.Logger:
    """Return a dedicated stderr logger for destroy progress messages."""
    logger = logging.getLogger("hailstack.destroy")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger


def create_pulumi_destroy_runner(logger: logging.Logger) -> PulumiDestroyRunner:
    """Create the default Pulumi stack runner for destroy operations."""
    return AutomationStackRunner(logger)


def _ensure_ceph_s3_credentials(config: ClusterConfig) -> None:
    """Require Ceph S3 credentials before touching Pulumi state."""
    if not config.ceph_s3.has_required_credentials():
        raise ConfigError("Ceph S3 credentials required for Pulumi state backend")


def _confirm_destroy(cluster_name: str) -> bool:
    """Require an exact cluster-name confirmation before destroying."""
    prompt = (
        f"Do you want to destroy cluster '{cluster_name}'? "
        "Type the cluster name to confirm"
    )
    confirmation = typer.prompt(
        prompt,
        prompt_suffix=": ",
    )
    return confirmation == cluster_name


def destroy_command(
    config: Annotated[
        Path,
        typer.Option("--config", help="Path to cluster configuration TOML file."),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show the destroy plan without deleting resources.",
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
    """Destroy an existing cluster from a TOML configuration file."""
    logger = get_destroy_logger()
    loaded_config = load_config(config, dotenv)
    logger.info("config loaded")

    _ensure_ceph_s3_credentials(loaded_config)
    pulumi_runner = create_pulumi_destroy_runner(logger)
    pulumi_runner.check_backend_access(loaded_config)

    logger.info("previewing resources")
    preview_output = pulumi_runner.preview_destroy(loaded_config)
    typer.echo(preview_output, nl=not preview_output.endswith("\n"))

    if dry_run:
        return

    logger.info("awaiting confirmation")
    if not _confirm_destroy(loaded_config.cluster.name):
        typer.echo("Aborted")
        raise typer.Exit(code=1)

    logger.info("destroying infrastructure")
    pulumi_runner.destroy(loaded_config)
    logger.info("cleanup complete")
    typer.echo(f"Cluster '{loaded_config.cluster.name}' destroyed.")


__all__ = [
    "PulumiDestroyRunner",
    "create_pulumi_destroy_runner",
    "destroy_command",
    "get_destroy_logger",
]
