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

import importlib
import re
from pathlib import Path

from typer.testing import CliRunner

from hailstack import runtime_paths as runtime_paths_module
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
    assert re.fullmatch(r"hailstack \d+\.\d+\.\d+\n",
                        result.stdout) is not None


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


def test_runtime_paths_import_does_not_create_workspace_on_import(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Keep CLI startup import-safe when the installed runtime home is not writable."""
    fake_home = tmp_path / "readonly-home"
    fake_home.mkdir()

    checkout_git_dir = runtime_paths_module.CHECKOUT_ROOT / ".git"
    original_exists = Path.exists
    original_mkdir = Path.mkdir

    def fake_exists(self: Path) -> bool:
        if self == checkout_git_dir:
            return False
        return original_exists(self)

    def fake_mkdir(self: Path, *args: object, **kwargs: object) -> None:
        if self == fake_home / ".hailstack" / "workspace":
            raise AssertionError(
                "runtime workspace should not be created at import time"
            )
        original_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(runtime_paths_module.Path, "exists", fake_exists)
    monkeypatch.setattr(
        runtime_paths_module.Path,
        "home",
        staticmethod(lambda: fake_home),
    )
    monkeypatch.setattr(runtime_paths_module.Path, "mkdir", fake_mkdir)

    reloaded = importlib.reload(runtime_paths_module)

    assert reloaded.RUNTIME_WORK_DIR == fake_home / ".hailstack" / "workspace"
