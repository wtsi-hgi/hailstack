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

"""CLI tests for phase-1 bundle validation semantics."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from hailstack.cli.commands import _bundle_validation
from hailstack.cli.commands import build_image as build_image_module
from hailstack.cli.commands import create as create_module
from hailstack.cli.commands import destroy as destroy_module
from hailstack.cli.commands import install as install_module
from hailstack.cli.commands import reboot as reboot_module
from hailstack.cli.commands import status as status_module
from hailstack.cli.main import app
from hailstack.errors import BundleNotFoundError

runner = CliRunner()


def _write_bundles(path: Path) -> Path:
    """Write a minimal compatibility matrix for command tests."""
    path.write_text(
        """
[default]
bundle = "hail-0.2.137-gnomad-3.0.4-r2"

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


def _write_config(path: Path, bundle: str) -> Path:
    """Write a minimal valid cluster config for CLI command tests."""
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            f'bundle = "{bundle}"\n'
            'master_flavour = "m2.medium"\n'
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def command_matrix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point CLI bundle validation at a temporary matrix file."""
    bundles_path = _write_bundles(tmp_path / "bundles.toml")
    monkeypatch.setattr(
        _bundle_validation,
        "DEFAULT_COMPATIBILITY_MATRIX_PATH",
        bundles_path,
    )
    return bundles_path


def test_create_validates_bundle_at_command_time(
    command_matrix: Path,
    tmp_path: Path,
) -> None:
    """Fail create before any later-phase provisioning logic when bundle is unknown."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml", "removed-bundle")

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, BundleNotFoundError)


def test_build_image_validates_bundle_at_command_time(
    command_matrix: Path,
    tmp_path: Path,
) -> None:
    """Fail build-image before any Packer work when bundle is unknown."""
    del command_matrix
    config_path = _write_config(tmp_path / "build-image.toml", "removed-bundle")

    result = runner.invoke(app, ["build-image", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, BundleNotFoundError)


def test_non_provisioning_commands_do_not_invoke_bundle_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Leave destroy, status, reboot, and install free of CLI-time bundle checks."""

    class FakeCephS3:
        def has_required_credentials(self) -> bool:
            return True

    fake_config = SimpleNamespace(
        cluster=SimpleNamespace(
            name="test-cluster",
            bundle="removed-bundle",
            ssh_username="ubuntu",
            master_flavour="m2.medium",
            monitoring="netdata",
        ),
        volumes=SimpleNamespace(
            create=False,
            name="",
            size_gb=0,
            existing_volume_id="",
        ),
        ceph_s3=FakeCephS3(),
    )

    def fail_validation(
        command: str,
        config_path: Path,
        dotenv_file: Path | None = None,
    ) -> None:
        del command, config_path, dotenv_file
        raise AssertionError("unexpected bundle validation")

    monkeypatch.setattr(
        create_module,
        "validate_command_config_bundle",
        fail_validation,
    )
    monkeypatch.setattr(
        build_image_module,
        "validate_command_config_bundle",
        fail_validation,
    )

    monkeypatch.setattr(
        destroy_module, "load_config", lambda config, dotenv: fake_config
    )
    monkeypatch.setattr(
        destroy_module,
        "create_pulumi_destroy_runner",
        lambda logger: SimpleNamespace(
            check_backend_access=lambda config: None,
            preview_destroy=lambda config: "preview\n",
            destroy=lambda config: None,
        ),
    )

    monkeypatch.setattr(
        status_module, "load_config", lambda config, dotenv: fake_config
    )
    monkeypatch.setattr(
        status_module,
        "create_status_stack_runner",
        lambda logger: SimpleNamespace(
            get_status_outputs=lambda config: {
                "cluster_name": "test-cluster",
                "bundle_id": "removed-bundle",
                "master_public_ip": "198.51.100.10",
                "master_private_ip": "10.0.0.10",
                "worker_private_ips": [],
                "worker_names": [],
            }
        ),
    )

    monkeypatch.setattr(
        reboot_module, "load_config", lambda config, dotenv_file=None: fake_config
    )
    monkeypatch.setattr(
        reboot_module,
        "create_reboot_stack_runner",
        lambda logger: SimpleNamespace(
            get_reboot_outputs=lambda config: {
                "cluster_name": "test-cluster",
                "worker_private_ips": [],
                "worker_names": [],
            }
        ),
    )
    monkeypatch.setattr(
        reboot_module,
        "create_reboot_executor",
        lambda logger: SimpleNamespace(
            reboot_nodes=lambda inventory, **kwargs: None,
        ),
    )

    monkeypatch.setattr(
        install_module, "load_config", lambda config, dotenv: fake_config
    )
    monkeypatch.setattr(
        install_module,
        "create_install_stack_runner",
        lambda logger: SimpleNamespace(
            get_install_outputs=lambda config: {
                "cluster_name": "test-cluster",
                "master_public_ip": "198.51.100.10",
                "master_private_ip": "10.0.0.10",
                "worker_private_ips": [],
                "worker_names": [],
            }
        ),
    )
    monkeypatch.setattr(
        install_module,
        "create_install_executor",
        lambda logger: SimpleNamespace(
            run_install=lambda **kwargs: [
                SimpleNamespace(
                    node_name="test-cluster-master",
                    host="198.51.100.10",
                    success=True,
                    system_packages=["mc"],
                    python_packages=[],
                    smoke_test=None,
                    verification={
                        "system": {"mc": True},
                        "python": {},
                        "imports": {},
                        "versions": {},
                        "software_state_updated": True,
                    },
                    error="",
                    changed=False,
                    attempts=1,
                )
            ],
        ),
    )
    monkeypatch.setattr(
        install_module,
        "create_rollout_recorder",
        lambda logger: SimpleNamespace(
            record_rollout=lambda **kwargs: "s3://bucket/manifest.json",
        ),
    )

    config_path = tmp_path / "dummy.toml"
    config_path.write_text("", encoding="utf-8")

    command_invocations = (
        ["destroy", "--config", str(config_path), "--dry-run"],
        ["status", "--config", str(config_path)],
        ["reboot", "--config", str(config_path)],
        ["install", "--config", str(config_path), "--system", "mc"],
    )

    for argv in command_invocations:
        result = runner.invoke(app, argv)

        assert result.exit_code == 0
