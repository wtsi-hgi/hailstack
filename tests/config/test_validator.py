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

"""Acceptance tests for the C2 bundle validator."""

from pathlib import Path

import pytest

from hailstack.config.compatibility import CompatibilityMatrix
from hailstack.config.parser import load_config
from hailstack.config.validator import validate_bundle, validate_bundle_for_command
from hailstack.errors import BundleNotFoundError


def _write_bundles(path: Path, content: str) -> Path:
    """Write TOML bundle data to a temporary path."""
    path.write_text(content, encoding="utf-8")
    return path


def _write_config(path: Path, content: str) -> Path:
    """Write TOML content to a temporary config path."""
    path.write_text(content, encoding="utf-8")
    return path


def _matrix(tmp_path: Path) -> CompatibilityMatrix:
    """Build a small matrix fixture for bundle validation tests."""
    bundles_path = _write_bundles(
        tmp_path / "bundles.toml",
        """
[default]
bundle = "hail-0.2.137-gnomad-3.0.4-r2"

[bundle."hail-0.2.137-gnomad-3.0.4-r1"]
hail = "0.2.137"
spark = "3.5.4"
hadoop = "3.4.0"
java = "11"
python = "3.12"
scala = "2.12.18"
gnomad = "3.0.4"
status = "supported"

[bundle."hail-0.2.137-gnomad-3.0.4-r2"]
hail = "0.2.137"
spark = "3.5.6"
hadoop = "3.4.1"
java = "11"
python = "3.12"
scala = "2.12.18"
gnomad = "3.0.4"
status = "latest"
""".strip(),
    )
    return CompatibilityMatrix(bundles_path)


def _config(tmp_path: Path, bundle: str) -> Path:
    """Create a minimal valid cluster config with a configurable bundle ID."""
    bundle_line = f'bundle = "{bundle}"\n' if bundle else ""
    return _write_config(
        tmp_path / f"cluster-{bundle or 'default'}.toml",
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            f"{bundle_line}"
            'master_flavour = "m2.medium"\n'
        ),
    )


def test_validate_bundle_returns_matching_bundle(tmp_path: Path) -> None:
    """Return the explicit bundle configured in cluster.bundle."""
    config = load_config(_config(tmp_path, "hail-0.2.137-gnomad-3.0.4-r1"))

    result = validate_bundle(config, _matrix(tmp_path))

    assert result.id == "hail-0.2.137-gnomad-3.0.4-r1"
    assert result.spark == "3.5.4"


def test_validate_bundle_raises_bundle_not_found_with_available_bundles(
    tmp_path: Path,
) -> None:
    """List available bundle IDs when cluster.bundle is unknown."""
    config = load_config(_config(tmp_path, "removed-bundle"))

    with pytest.raises(
        BundleNotFoundError,
        match=(
            r"removed-bundle.*hail-0\.2\.137-gnomad-3\.0\.4-r1.*"
            r"hail-0\.2\.137-gnomad-3\.0\.4-r2"
        ),
    ):
        validate_bundle(config, _matrix(tmp_path))


def test_validate_bundle_uses_matrix_default_when_config_bundle_empty(
    tmp_path: Path,
) -> None:
    """Use the matrix default bundle when cluster.bundle is unset."""
    config = load_config(_config(tmp_path, ""))

    result = validate_bundle(config, _matrix(tmp_path))

    assert result.id == "hail-0.2.137-gnomad-3.0.4-r2"
    assert result.status == "latest"


def test_validate_bundle_for_command_skips_removed_bundles_outside_create_time(
    tmp_path: Path,
) -> None:
    """Skip bundle lookups for commands that may target older clusters."""
    config = load_config(_config(tmp_path, "removed-bundle"))
    matrix = _matrix(tmp_path)

    assert validate_bundle_for_command(config, matrix, "destroy") is None
    assert validate_bundle_for_command(config, matrix, "status") is None
    assert validate_bundle_for_command(config, matrix, "reboot") is None
    assert validate_bundle_for_command(config, matrix, "install") is None

    with pytest.raises(BundleNotFoundError):
        validate_bundle_for_command(config, matrix, "create")

    with pytest.raises(BundleNotFoundError):
        validate_bundle_for_command(config, matrix, "build-image")
