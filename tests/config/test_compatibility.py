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

"""Acceptance tests for the C1 compatibility matrix."""

from pathlib import Path

import pytest

from hailstack.config.compatibility import CompatibilityMatrix
from hailstack.errors import BundleNotFoundError, ConfigError


def _write_bundles(path: Path, content: str) -> Path:
    """Write TOML bundle data to a temporary path."""
    path.write_text(content, encoding="utf-8")
    return path


def _repo_bundles_path() -> Path:
    """Return the checked-in bundles.toml path from the repo root."""
    return Path(__file__).resolve().parents[2] / "bundles.toml"


def test_matrix_initialises_all_three_repo_bundles() -> None:
    """Load the checked-in matrix and expose all documented bundle IDs."""
    matrix = CompatibilityMatrix(_repo_bundles_path())

    bundle_ids = [bundle.id for bundle in matrix.list_bundles()]

    assert bundle_ids == [
        "hail-0.2.136-gnomad-3.0.4-r1",
        "hail-0.2.137-gnomad-3.0.4-r1",
        "hail-0.2.137-gnomad-3.0.4-r2",
    ]


def test_get_default_returns_bundle_from_default_section() -> None:
    """Return the bundle referenced by the default.bundle setting."""
    matrix = CompatibilityMatrix(_repo_bundles_path())

    result = matrix.get_default()

    assert result.id == "hail-0.2.137-gnomad-3.0.4-r2"
    assert result.status == "latest"


def test_get_bundle_returns_expected_component_versions() -> None:
    """Return the requested bundle with all explicit version fields."""
    matrix = CompatibilityMatrix(_repo_bundles_path())

    result = matrix.get_bundle("hail-0.2.137-gnomad-3.0.4-r2")

    assert result.hail == "0.2.137"
    assert result.spark == "3.5.6"
    assert result.hadoop == "3.4.1"
    assert result.gnomad == "3.0.4"


def test_get_bundle_raises_bundle_not_found_with_available_ids() -> None:
    """List valid bundle IDs when an unknown bundle is requested."""
    matrix = CompatibilityMatrix(_repo_bundles_path())

    with pytest.raises(
        BundleNotFoundError,
        match=(
            "nonexistent.*hail-0\\.2\\.136-gnomad-3\\.0\\.4-r1.*"
            "hail-0\\.2\\.137-gnomad-3\\.0\\.4-r1.*"
            "hail-0\\.2\\.137-gnomad-3\\.0\\.4-r2"
        ),
    ):
        matrix.get_bundle("nonexistent")


def test_list_bundles_returns_all_bundles() -> None:
    """Return every parsed bundle as Bundle models."""
    matrix = CompatibilityMatrix(_repo_bundles_path())

    result = matrix.list_bundles()

    assert len(result) == 3
    assert {bundle.status for bundle in result} == {"latest", "supported"}


def test_matrix_raises_config_error_for_empty_bundle_set(tmp_path: Path) -> None:
    """Reject bundle files that do not define any bundle entries."""
    bundles_path = _write_bundles(
        tmp_path / "bundles.toml",
        """
[default]
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
""".strip(),
    )

    with pytest.raises(ConfigError, match="No bundles defined"):
        CompatibilityMatrix(bundles_path)


def test_revisioned_bundles_return_distinct_spark_and_hadoop_versions() -> None:
    """Keep separate revisions distinct for the same Hail and gnomAD pair."""
    matrix = CompatibilityMatrix(_repo_bundles_path())

    revision_one = matrix.get_bundle("hail-0.2.137-gnomad-3.0.4-r1")
    revision_two = matrix.get_bundle("hail-0.2.137-gnomad-3.0.4-r2")

    assert revision_one.spark == "3.5.4"
    assert revision_one.hadoop == "3.4.0"
    assert revision_two.spark == "3.5.6"
    assert revision_two.hadoop == "3.4.1"


def test_matrix_raises_config_error_for_missing_required_bundle_field(
    tmp_path: Path,
) -> None:
    """Reject bundle entries that omit required component fields."""
    bundles_path = _write_bundles(
        tmp_path / "bundles.toml",
        """
[default]
bundle = "hail-0.2.137-gnomad-3.0.4-r1"

[bundle."hail-0.2.137-gnomad-3.0.4-r1"]
hail = "0.2.137"
hadoop = "3.4.0"
java = "11"
python = "3.12"
scala = "2.12.18"
gnomad = "3.0.4"
status = "supported"
""".strip(),
    )

    with pytest.raises(ConfigError, match="spark"):
        CompatibilityMatrix(bundles_path)
