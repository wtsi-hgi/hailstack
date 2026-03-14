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

"""Acceptance tests for configuration loading and validation."""

import logging
from pathlib import Path

import pytest

from hailstack.config.parser import _substitute_env_vars, load_config
from hailstack.config.schema import ClusterConfig
from hailstack.errors import ConfigError, ValidationError


def _write_config(path: Path, content: str) -> Path:
    """Write TOML content to a temporary config path."""
    path.write_text(content, encoding="utf-8")
    return path


def test_load_config_returns_cluster_config_for_valid_toml(tmp_path: Path) -> None:
    """Load a valid TOML file into the ClusterConfig model."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
num_workers = 2
master_flavour = "m2.medium"
monitoring = "netdata"
""".strip(),
    )

    result = load_config(config_path)

    assert isinstance(result, ClusterConfig)
    assert result.cluster.name == "test-cluster"
    assert result.cluster.bundle == "hail-0.2.137-gnomad-3.0.4-r2"
    assert result.cluster.num_workers == 2
    assert result.cluster.master_flavour == "m2.medium"
    assert result.cluster.monitoring == "netdata"


def test_load_config_raises_config_error_when_cluster_name_missing(
    tmp_path: Path,
) -> None:
    """Reject configs that omit the required cluster.name field."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
    master_flavour = "m2.medium"
""".strip(),
    )

    with pytest.raises(ConfigError, match="cluster\\.name"):
        load_config(config_path)


def test_load_config_rejects_cluster_name_with_underscore(tmp_path: Path) -> None:
    """Reject cluster names that contain underscores."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test_cluster"
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
    master_flavour = "m2.medium"
""".strip(),
    )

    with pytest.raises(ValidationError, match="Invalid cluster name"):
        load_config(config_path)


def test_load_config_rejects_cluster_name_that_is_too_short(tmp_path: Path) -> None:
    """Reject cluster names shorter than the minimum allowed length."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "A"
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
    master_flavour = "m2.medium"
""".strip(),
    )

    with pytest.raises(ValidationError, match="Invalid cluster name"):
        load_config(config_path)


def test_load_config_rejects_cluster_name_starting_with_digit(tmp_path: Path) -> None:
    """Reject cluster names that do not start with a lowercase letter."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "9starts-with-digit"
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
    master_flavour = "m2.medium"
""".strip(),
    )

    with pytest.raises(ValidationError, match="Invalid cluster name"):
        load_config(config_path)


def test_load_config_raises_config_error_with_line_number_for_toml_syntax_error(
    tmp_path: Path,
) -> None:
    """Report TOML syntax failures with the source line number."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
bundle = "hail-0.2.137-gnomad-3.0.4-r2
""".strip(),
    )

    with pytest.raises(ConfigError, match=r"line 3"):
        load_config(config_path)


def test_load_config_loads_dotenv_and_substitutes_env_vars(tmp_path: Path) -> None:
    """Load environment variables from a .env file before validation."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "$CLUSTER_NAME"
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
    master_flavour = "m2.medium"

[ceph_s3]
endpoint = "https://ceph.example.invalid"
bucket = "hailstack-state"
access_key = "$API_TOKEN"
secret_key = "static-secret"
""".strip(),
    )
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "CLUSTER_NAME=test-cluster\nAPI_TOKEN=val123\n",
        encoding="utf-8",
    )

    result = load_config(config_path, dotenv_file=dotenv_path)

    assert result.cluster.name == "test-cluster"
    assert result.ceph_s3.access_key == "val123"


def test_load_config_rejects_missing_s3_credential_env_vars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail validation when runtime S3 env vars expand to incomplete credentials."""
    monkeypatch.delenv("S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("S3_SECRET_KEY", raising=False)
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.medium"

[s3]
endpoint = "https://ceph.example.invalid"
access_key = "$S3_ACCESS_KEY"
secret_key = "$S3_SECRET_KEY"
""".strip(),
    )

    with pytest.raises(
        ValidationError,
        match=(
            "s3.endpoint, s3.access_key, and s3.secret_key must all be set together"
        ),
    ):
        load_config(config_path)


def test_load_config_rejects_unknown_top_level_sections(tmp_path: Path) -> None:
    """Reject undeclared top-level config sections instead of preserving them."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
master_flavour = "m2.medium"

[secrets]
token = "val123"
""".strip(),
    )

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        load_config(config_path)


def test_substitute_env_vars_uses_empty_string_for_undefined_vars_and_logs_warning(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replace missing environment variables with empty strings."""
    monkeypatch.delenv("UNDEFINED", raising=False)

    with caplog.at_level(logging.WARNING):
        result = _substitute_env_vars("${UNDEFINED}")

    assert result == ""
    assert "UNDEFINED" in caplog.text


def test_substitute_env_vars_recurses_through_nested_structures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Substitute environment variables through dict and list nesting."""
    monkeypatch.setenv("TOKEN", "val123")
    raw_config = {
        "cluster": {"name": "test-cluster", "bundle": "$TOKEN"},
        "nested": [{"token": "$TOKEN"}, ["${TOKEN}", "prefix-${TOKEN}"]],
    }

    result = _substitute_env_vars(raw_config)

    assert result == {
        "cluster": {"name": "test-cluster", "bundle": "val123"},
        "nested": [{"token": "val123"}, ["val123", "prefix-val123"]],
    }


def test_substitute_env_vars_leaves_non_string_values_unchanged() -> None:
    """Preserve non-string TOML values during substitution."""
    raw_config = {
        "count": 3,
        "enabled": True,
        "ratio": 1.5,
        "items": [1, False, 2.0],
    }

    result = _substitute_env_vars(raw_config)

    assert result == raw_config
