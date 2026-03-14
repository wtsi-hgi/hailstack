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

"""Parse and validate cluster configuration files."""

import logging
import os
import re
import tomllib
from pathlib import Path
from typing import Never

from dotenv import load_dotenv
from pydantic import ValidationError as PydanticValidationError

from hailstack.config.schema import ClusterConfig
from hailstack.errors import ConfigError, ValidationError

type ConfigValue = str | int | float | bool | list[ConfigValue] | dict[str, ConfigValue]

_ENV_VAR_PATTERN = re.compile(
    r"\$(?P<simple>[A-Za-z_][A-Za-z0-9_]*)|\$\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)\}"
)

logger = logging.getLogger(__name__)


def _format_location(parts: tuple[int | str, ...]) -> str:
    """Convert a Pydantic error location tuple into dot notation."""
    return ".".join(str(part) for part in parts)


def _raise_model_error(error: PydanticValidationError) -> Never:
    """Map Pydantic validation details into project exceptions."""
    missing_fields = [
        _format_location(tuple(detail["loc"]))
        for detail in error.errors(include_url=False)
        if detail["type"] == "missing"
    ]
    if missing_fields:
        missing_list = ", ".join(missing_fields)
        raise ConfigError(f"Missing required configuration fields: {missing_list}")

    first_error = error.errors(include_url=False)[0]
    raise ValidationError(str(first_error["msg"]))


def _substitute_env_vars(obj: ConfigValue) -> ConfigValue:
    """Recursively replace $VAR and ${VAR} references in string values."""
    match obj:
        case str():
            return _ENV_VAR_PATTERN.sub(_replace_env_var, obj)
        case list():
            return [_substitute_env_vars(item) for item in obj]
        case dict():
            return {key: _substitute_env_vars(value) for key, value in obj.items()}
        case _:
            return obj


def _replace_env_var(match: re.Match[str]) -> str:
    """Resolve a matched environment variable reference."""
    variable_name = match.group("simple") or match.group("braced")
    if variable_name is None:
        return ""

    variable_value = os.environ.get(variable_name)
    if variable_value is None:
        logger.warning(
            "Environment variable %s is undefined; substituting empty string",
            variable_name,
        )
        return ""

    return variable_value


def load_config(path: Path, dotenv_file: Path | None = None) -> ClusterConfig:
    """Load TOML config, validate it, and return a ClusterConfig."""
    if dotenv_file is not None:
        load_dotenv(dotenv_path=dotenv_file, override=False)

    try:
        with path.open("rb") as handle:
            raw_config = tomllib.load(handle)
    except FileNotFoundError as error:
        raise ConfigError(f"Configuration file not found: {path}") from error
    except tomllib.TOMLDecodeError as error:
        line_number = getattr(error, "lineno", "unknown")
        column_number = getattr(error, "colno", "unknown")
        message = getattr(error, "msg", str(error))
        raise ConfigError(
            "Invalid TOML syntax in "
            f"{path}: line {line_number}, column {column_number}: {message}"
        ) from error

    substituted_config = _substitute_env_vars(raw_config)

    try:
        return ClusterConfig.model_validate(substituted_config)
    except PydanticValidationError as error:
        _raise_model_error(error)


__all__ = ["load_config"]
