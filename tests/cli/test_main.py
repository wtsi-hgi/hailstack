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

"""Acceptance tests for the hailstack CLI entry point."""

import re

from typer.testing import CliRunner

from hailstack.cli.main import app

runner = CliRunner()


def test_help_lists_all_commands() -> None:
    """Show all supported commands in top-level help."""
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    for command_name in (
        "create",
        "destroy",
        "reboot",
        "build-image",
        "install",
        "status",
        "convert-auth",
    ):
        assert command_name in result.stdout


def test_version_flag_prints_semver() -> None:
    """Print the application version and exit successfully."""
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert re.fullmatch(r"hailstack \d+\.\d+\.\d+\n", result.stdout) is not None


def test_invalid_command_reports_error_on_stderr() -> None:
    """Reject unknown commands with Click's standard exit status."""
    result = runner.invoke(app, ["nonexistent"])

    assert result.exit_code == 2
    assert "No such command" in result.stderr


def test_create_help_shows_required_options() -> None:
    """Describe the expected create command options."""
    result = runner.invoke(app, ["create", "--help"])

    assert result.exit_code == 0
    assert "--config" in result.stdout
    assert "Path to cluster configuration TOML file." in result.stdout
    assert "--dry-run" in result.stdout
    assert "Validate configuration without creating resources." in result.stdout
    assert "--dotenv" in result.stdout
    assert "Load environment variables from a .env file before" in result.stdout
    assert "parsing config." in result.stdout
