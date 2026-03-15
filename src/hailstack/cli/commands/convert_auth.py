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

"""Convert-auth command for the hailstack CLI."""

import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
import yaml

from hailstack.errors import ConfigError


def get_convert_auth_logger() -> logging.Logger:
    """Return a dedicated stderr logger for convert-auth progress messages."""
    logger = logging.getLogger("hailstack.convert-auth")
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def _require_env_var(name: str) -> str:
    """Return a required OpenStack environment variable or raise ConfigError."""
    try:
        return os.environ[name]
    except KeyError as error:
        message = f"{name} not set. Source your openrc.sh first."
        raise ConfigError(message) from error


def _optional_env_var(name: str) -> str | None:
    """Return an optional OpenStack environment variable if available."""
    return os.environ.get(name)


def _build_clouds_config() -> dict[str, object]:
    """Collect OpenStack environment variables into a clouds.yaml structure."""
    auth: dict[str, str] = {
        "auth_url": _require_env_var("OS_AUTH_URL"),
        "project_name": _require_env_var("OS_PROJECT_NAME"),
        "username": _require_env_var("OS_USERNAME"),
    }

    optional_auth_values = (
        ("OS_PASSWORD", "password"),
        ("OS_USER_DOMAIN_NAME", "user_domain_name"),
        ("OS_PROJECT_DOMAIN_NAME", "project_domain_name"),
    )
    for env_name, yaml_name in optional_auth_values:
        value = _optional_env_var(env_name)
        if value is not None:
            auth[yaml_name] = value

    openstack: dict[str, object] = {"auth": auth}
    optional_top_level_values = (
        ("OS_REGION_NAME", "region_name"),
        ("OS_IDENTITY_API_VERSION", "identity_api_version"),
    )
    for env_name, yaml_name in optional_top_level_values:
        value = _optional_env_var(env_name)
        if value is not None:
            openstack[yaml_name] = value

    return {"clouds": {"openstack": openstack}}


def _render_clouds_yaml() -> str:
    """Build a clouds.yaml document from current OpenStack environment variables."""
    return yaml.safe_dump(
        _build_clouds_config(),
        default_flow_style=False,
        sort_keys=False,
    )


def _clouds_yaml_path() -> Path:
    """Return the standard OpenStack clouds.yaml path in the user's home dir."""
    return Path.home() / ".config" / "openstack" / "clouds.yaml"


def _backup_existing_clouds_yaml(clouds_yaml_path: Path) -> None:
    """Create a timestamped backup of an existing clouds.yaml file."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = clouds_yaml_path.with_name(f"clouds.yaml.bak.{timestamp}")
    shutil.copy2(clouds_yaml_path, backup_path)
    backup_path.chmod(0o600)


def _write_clouds_yaml(yaml_output: str) -> None:
    """Write clouds.yaml to the standard OpenStack config location."""
    clouds_yaml_path = _clouds_yaml_path()
    clouds_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    if clouds_yaml_path.exists():
        _backup_existing_clouds_yaml(clouds_yaml_path)
    clouds_yaml_path.write_text(yaml_output, encoding="utf-8")
    clouds_yaml_path.chmod(0o600)


def convert_auth_command(
    write: Annotated[
        bool,
        typer.Option(
            "--write", help="Write to ~/.config/openstack/clouds.yaml."),
    ] = False,
) -> None:
    """Convert openrc.sh env vars to clouds.yaml format."""
    logger = get_convert_auth_logger()
    logger.info("reading OpenStack environment")
    yaml_output = _render_clouds_yaml()
    logger.info("clouds.yaml generated")
    if write:
        _write_clouds_yaml(yaml_output)
        logger.info("clouds.yaml written")
        return
    typer.echo(yaml_output, nl=False)


__all__ = ["convert_auth_command", "get_convert_auth_logger"]
