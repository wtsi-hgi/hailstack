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

from pathlib import Path
from typing import Annotated

import typer

from hailstack.cli.commands._bundle_validation import validate_command_config_bundle


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
    validate_command_config_bundle("create", config, dotenv)
    del dry_run


__all__ = ["create_command"]
