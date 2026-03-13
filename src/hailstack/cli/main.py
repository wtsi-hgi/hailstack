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

"""Entry point for the hailstack command-line interface."""

from typing import Annotated

import typer

from hailstack.cli.commands import (
    build_image_command,
    convert_auth_command,
    create_command,
    destroy_command,
    install_command,
    reboot_command,
    status_command,
)
from hailstack.version import __version__

app = typer.Typer(
    name="hailstack",
    help="Provision and manage Spark/Hadoop/Hail clusters on OpenStack.",
)


def _version_callback(value: bool) -> None:
    """Print the version and exit when requested."""
    if value:
        typer.echo(f"hailstack {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            help="Show version and exit.",
            is_eager=True,
        ),
    ] = None,
) -> None:
    """Run the hailstack CLI."""
    del version


app.command(name="create")(create_command)
app.command(name="destroy")(destroy_command)
app.command(name="reboot")(reboot_command)
app.command(name="build-image")(build_image_command)
app.command(name="install")(install_command)
app.command(name="status")(status_command)
app.command(name="convert-auth")(convert_auth_command)


__all__ = ["app", "main"]
