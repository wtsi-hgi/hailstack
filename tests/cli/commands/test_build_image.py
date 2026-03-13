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

"""Acceptance tests for the build-image CLI command."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from hailstack.cli.commands import build_image as build_image_module
from hailstack.cli.main import app
from hailstack.config.compatibility import Bundle
from hailstack.errors import BundleNotFoundError

runner = CliRunner()


def _write_config(path: Path, bundle_id: str = "") -> Path:
    """Write a minimal build-image config file."""
    bundle_line = f'bundle = "{bundle_id}"\n' if bundle_id else ""
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            f"{bundle_line}"
            'master_flavour = "m2.medium"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n\n'
            "[packer]\n"
            'base_image = "ubuntu-22.04"\n'
            'flavour = "m2.large"\n'
            'floating_ip_pool = "public"\n'
        ),
        encoding="utf-8",
    )
    return path


def _write_bundles(path: Path) -> Path:
    """Write a compatibility matrix with two bundles for override tests."""
    path.write_text(
        """
[default]
bundle = "hail-0.2.137-gnomad-3.0.4-r2"

[bundle."hail-0.2.136-gnomad-3.0.4-r1"]
hail = "0.2.136"
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
        encoding="utf-8",
    )
    return path


@pytest.fixture
def command_matrix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point build-image command bundle resolution at a temporary matrix file."""
    matrix_path = _write_bundles(tmp_path / "bundles.toml")
    monkeypatch.setattr(
        build_image_module,
        "DEFAULT_COMPATIBILITY_MATRIX_PATH",
        matrix_path,
    )
    return matrix_path


def test_build_image_cmd_raises_bundle_not_found_before_packer_invoked(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject unknown bundle IDs before delegating to the packer builder."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "build-image.toml", "removed-bundle")

    def fail_build(*_: object, **__: object) -> str:
        raise AssertionError("builder should not be called")

    monkeypatch.setattr(build_image_module, "build_image", fail_build)

    result = runner.invoke(app, ["build-image", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, BundleNotFoundError)


def test_build_image_cmd_bundle_override_takes_precedence(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the CLI override bundle instead of the config bundle."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "build-image.toml",
        "hail-0.2.137-gnomad-3.0.4-r2",
    )
    called_with: list[Bundle] = []

    def fake_build_image(config: object, bundle: Bundle, **_: object) -> str:
        del config
        called_with.append(bundle)
        return "image-123"

    monkeypatch.setattr(build_image_module, "build_image", fake_build_image)

    result = runner.invoke(
        app,
        [
            "build-image",
            "--config",
            str(config_path),
            "--bundle",
            "hail-0.2.136-gnomad-3.0.4-r1",
        ],
    )

    assert result.exit_code == 0
    assert called_with[0].id == "hail-0.2.136-gnomad-3.0.4-r1"


def test_build_image_cmd_uses_config_bundle_when_override_not_set(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve the bundle from config when the CLI override is absent."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "build-image.toml",
        "hail-0.2.137-gnomad-3.0.4-r2",
    )
    called_with: list[Bundle] = []

    def fake_build_image(config: object, bundle: Bundle, **_: object) -> str:
        del config
        called_with.append(bundle)
        return "image-123"

    monkeypatch.setattr(build_image_module, "build_image", fake_build_image)

    result = runner.invoke(app, ["build-image", "--config", str(config_path)])

    assert result.exit_code == 0
    assert called_with[0].id == "hail-0.2.137-gnomad-3.0.4-r2"


def test_build_image_cmd_prints_built_image_id(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Print the built image ID to stdout on success."""
    del command_matrix
    config_path = _write_config(tmp_path / "build-image.toml")

    def fake_build_image(config: object, bundle: Bundle, **_: object) -> str:
        del config, bundle
        return "openstack-image-id"

    monkeypatch.setattr(build_image_module, "build_image", fake_build_image)

    result = runner.invoke(app, ["build-image", "--config", str(config_path)])

    assert result.exit_code == 0
    assert result.stdout == "openstack-image-id\n"


def test_build_image_cmd_logs_each_progress_stage_to_stderr(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Log the documented build-image stages to stderr."""
    del command_matrix
    config_path = _write_config(tmp_path / "build-image.toml")

    def fake_build_image(config: object, bundle: Bundle, **_: object) -> str:
        del config, bundle
        logger = build_image_module.get_build_logger()
        logger.info("Packer starting")
        logger.info("image uploaded")
        return "image-123"

    monkeypatch.setattr(build_image_module, "build_image", fake_build_image)

    result = runner.invoke(app, ["build-image", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "config loaded" in result.stderr
    assert "bundle resolved" in result.stderr
    assert "Packer starting" in result.stderr
    assert "image uploaded" in result.stderr
