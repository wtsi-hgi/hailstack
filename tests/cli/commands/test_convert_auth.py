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

"""Acceptance tests for the convert-auth CLI command."""

from pathlib import Path
from typing import cast

import pytest
import yaml
from typer.testing import CliRunner

from hailstack.cli.commands.convert_auth import convert_auth_command
from hailstack.cli.main import app
from hailstack.errors import ConfigError

runner = CliRunner()


def _load_clouds_yaml(document: str) -> dict[str, object]:
    """Parse convert-auth output with a real YAML parser."""
    loaded = cast(object, yaml.safe_load(document))
    assert isinstance(loaded, dict)
    return cast(dict[str, object], loaded)


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Populate the required OpenStack environment variables."""
    monkeypatch.setenv("OS_AUTH_URL", "https://keystone.example/v3")
    monkeypatch.setenv("OS_PROJECT_NAME", "research-project")
    monkeypatch.setenv("OS_USERNAME", "alice")


def test_convert_auth_prints_valid_yaml_to_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Print the required auth fields in clouds.yaml format."""
    _set_required_env(monkeypatch)

    result = runner.invoke(app, ["convert-auth"])
    parsed = _load_clouds_yaml(result.stdout)

    assert result.exit_code == 0
    assert result.stdout == (
        "clouds:\n"
        "  openstack:\n"
        "    auth:\n"
        "      auth_url: https://keystone.example/v3\n"
        "      project_name: research-project\n"
        "      username: alice\n"
    )
    assert parsed == {
        "clouds": {
            "openstack": {
                "auth": {
                    "auth_url": "https://keystone.example/v3",
                    "project_name": "research-project",
                    "username": "alice",
                }
            }
        }
    }


def test_convert_auth_raises_config_error_when_required_env_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject missing required OpenStack environment variables."""
    monkeypatch.delenv("OS_AUTH_URL", raising=False)
    monkeypatch.setenv("OS_PROJECT_NAME", "research-project")
    monkeypatch.setenv("OS_USERNAME", "alice")

    with pytest.raises(
        ConfigError,
        match="OS_AUTH_URL not set. Source your openrc.sh first.",
    ):
        convert_auth_command()


def test_convert_auth_write_flag_writes_clouds_yaml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Write generated YAML to the standard clouds.yaml path."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("HOME", str(tmp_path))

    result = runner.invoke(app, ["convert-auth", "--write"])

    clouds_path = tmp_path / ".config" / "openstack" / "clouds.yaml"
    assert result.exit_code == 0
    assert clouds_path.read_text(encoding="utf-8") == result.stdout


def test_convert_auth_write_flag_creates_backup_for_existing_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Preserve an existing clouds.yaml file before overwriting it."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("HOME", str(tmp_path))
    clouds_dir = tmp_path / ".config" / "openstack"
    clouds_dir.mkdir(parents=True)
    clouds_path = clouds_dir / "clouds.yaml"
    clouds_path.write_text("old: value\n", encoding="utf-8")

    result = runner.invoke(app, ["convert-auth", "--write"])

    backups = sorted(clouds_dir.glob("clouds.yaml.bak.*"))
    assert result.exit_code == 0
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == "old: value\n"
    assert clouds_path.read_text(encoding="utf-8") == result.stdout


def test_convert_auth_output_contains_all_provided_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Include all supported optional environment variables when present."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("OS_PASSWORD", "true")
    monkeypatch.setenv("OS_REGION_NAME", "null")
    monkeypatch.setenv("OS_PROJECT_DOMAIN_NAME", "team: alpha")
    monkeypatch.setenv("OS_USER_DOMAIN_NAME", "users #1")
    monkeypatch.setenv("OS_IDENTITY_API_VERSION", "3")

    result = runner.invoke(app, ["convert-auth"])
    parsed = _load_clouds_yaml(result.stdout)

    clouds = parsed["clouds"]
    assert isinstance(clouds, dict)
    clouds = cast(dict[str, object], clouds)
    openstack = clouds["openstack"]
    assert isinstance(openstack, dict)
    openstack = cast(dict[str, object], openstack)
    auth = openstack["auth"]
    assert isinstance(auth, dict)
    auth = cast(dict[str, object], auth)

    assert result.exit_code == 0
    assert auth["auth_url"] == "https://keystone.example/v3"
    assert auth["project_name"] == "research-project"
    assert auth["username"] == "alice"
    assert auth["password"] == "true"
    assert auth["user_domain_name"] == "users #1"
    assert auth["project_domain_name"] == "team: alpha"
    assert openstack["region_name"] == "null"
    assert openstack["identity_api_version"] == "3"


def test_convert_auth_omits_password_when_not_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omit optional password fields when they are absent."""
    _set_required_env(monkeypatch)
    monkeypatch.delenv("OS_PASSWORD", raising=False)

    result = runner.invoke(app, ["convert-auth"])

    assert result.exit_code == 0
    assert "password:" not in result.stdout
