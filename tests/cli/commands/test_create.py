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

"""Acceptance tests for the D1 create CLI command."""

import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest
from typer.testing import CliRunner

from hailstack.cli.commands import create as create_module
from hailstack.cli.main import app
from hailstack.errors import (
    ConfigError,
    ImageNotFoundError,
    NetworkError,
    PulumiError,
    QuotaExceededError,
    ResourceNotFoundError,
    S3Error,
    ValidationError,
)

runner = CliRunner()


@dataclass
class FlavorDetails:
    """Represent fake flavour details for pre-flight quota calculations."""

    vcpus: int
    ram_mb: int


@dataclass
class ComputeQuota:
    """Represent fake available compute quota."""

    instances_available: int = 10
    cores_available: int = 100
    ram_mb_available: int = 512000


@dataclass
class VolumeQuota:
    """Represent fake available volume quota."""

    gigabytes_available: int = 1000


@dataclass
class FakeCreateResult:
    """Represent a fake successful create result."""

    master_public_ip: str
    stdout: str = ""


class FakeOpenStackClient:
    """Provide deterministic OpenStack pre-flight responses."""

    def __init__(
        self,
        *,
        images: set[str] | None = None,
        flavours: dict[str, FlavorDetails] | None = None,
        networks: set[str] | None = None,
        available_floating_ips: set[str] | None = None,
        existing_volumes: set[str] | None = None,
        compute_quota: ComputeQuota | None = None,
        volume_quota: VolumeQuota | None = None,
    ) -> None:
        """Initialise fake resource lookup state."""
        self.images = (
            images if images is not None else {
                "hailstack-hail-0.2.137-gnomad-3.0.4-r2"}
        )
        self.flavours = (
            flavours
            if flavours is not None
            else {
                "m2.2xlarge": FlavorDetails(vcpus=8, ram_mb=32768),
                "m2.xlarge": FlavorDetails(vcpus=4, ram_mb=16384),
            }
        )
        self.networks = networks if networks is not None else {"private-net"}
        self.available_floating_ips = (
            available_floating_ips if available_floating_ips is not None else set()
        )
        self.existing_volumes = (
            existing_volumes if existing_volumes is not None else set()
        )
        self.compute_quota = compute_quota or ComputeQuota()
        self.volume_quota = volume_quota or VolumeQuota()

    def get_image(self, name: str) -> object | None:
        """Return a truthy image record when the image exists."""
        return {"name": name} if name in self.images else None

    def get_flavour(self, name: str) -> FlavorDetails | None:
        """Return flavour details when the flavour exists."""
        return self.flavours.get(name)

    def get_network(self, name: str) -> object | None:
        """Return a truthy network record when the network exists."""
        return {"name": name} if name in self.networks else None

    def floating_ip_is_available(self, address: str) -> bool:
        """Report whether a configured floating IP is usable."""
        return address in self.available_floating_ips

    def volume_exists(self, volume_id: str) -> bool:
        """Report whether an existing volume is present."""
        return volume_id in self.existing_volumes

    def get_compute_quota(self) -> ComputeQuota:
        """Return available compute quota."""
        return self.compute_quota

    def get_volume_quota(self) -> VolumeQuota:
        """Return available volume quota."""
        return self.volume_quota


class FakePulumiRunner:
    """Capture create-command Pulumi interactions."""

    def __init__(
        self,
        *,
        preview_output: str = "Previewing update\nResources:\n  + 4 to create\n",
        create_result: FakeCreateResult | None = None,
        backend_error: Exception | None = None,
        up_error: Exception | None = None,
    ) -> None:
        """Initialise fake Pulumi responses."""
        self.preview_output = preview_output
        self.create_result = create_result or FakeCreateResult("203.0.113.10")
        self.backend_error = backend_error
        self.up_error = up_error
        self.checked_backend = 0
        self.preview_calls = 0
        self.up_calls = 0
        self.destroy_calls = 0

    def check_backend_access(self, config: object) -> None:
        """Record backend validation and optionally fail."""
        del config
        self.checked_backend += 1
        if self.backend_error is not None:
            raise self.backend_error

    def preview(self, config: object, bundle: object) -> str:
        """Return a preview-only Pulumi output string."""
        del config, bundle
        self.preview_calls += 1
        return self.preview_output

    def up(self, config: object, bundle: object) -> FakeCreateResult:
        """Return a fake create result or fail."""
        del config, bundle
        self.up_calls += 1
        if self.up_error is not None:
            raise self.up_error
        return self.create_result

    def destroy(self, config: object, bundle: object) -> None:
        """Record automatic cleanup calls."""
        del config, bundle
        self.destroy_calls += 1


def test_openstack_optional_show_returns_none_for_missing_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat OpenStack not-found lookups as absent resources, not network errors."""

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        return subprocess.CompletedProcess(
            [],
            1,
            stdout="",
            stderr="No network with a name or ID of 'missing-net' exists.",
        )

    monkeypatch.setattr(create_module.subprocess, "run", fake_run)

    assert create_module.OpenStackCLIClient().get_network("missing-net") is None


def test_openstack_optional_show_raises_network_error_for_auth_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface OpenStack auth/API failures instead of misreporting missing resources."""

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        return subprocess.CompletedProcess(
            [],
            1,
            stdout="",
            stderr="Authentication failed: invalid token",
        )

    monkeypatch.setattr(create_module.subprocess, "run", fake_run)

    with pytest.raises(NetworkError, match="Authentication failed"):
        create_module.OpenStackCLIClient().get_network("private-net")


def _write_bundles(path: Path) -> Path:
    """Write a temporary compatibility matrix for create tests."""
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


def _write_config(
    path: Path,
    *,
    cluster_name: str = "test-cluster",
    cluster_extra: str = "",
    ceph_s3_block: str | None = None,
    volumes_block: str = "",
) -> Path:
    """Write a minimal valid create configuration file."""
    ceph_s3_content = ceph_s3_block or (
        "[ceph_s3]\n"
        'endpoint = "https://ceph.example.invalid"\n'
        'bucket = "hailstack-state"\n'
        'access_key = "state-access"\n'
        'secret_key = "state-secret"\n'
    )
    path.write_text(
        (
            "[cluster]\n"
            f'name = "{cluster_name}"\n'
            'bundle = "hail-0.2.137-gnomad-3.0.4-r2"\n'
            "num_workers = 2\n"
            'master_flavour = "m2.2xlarge"\n'
            'worker_flavour = "m2.xlarge"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n\n'
            f"{cluster_extra}"
            f"{ceph_s3_content}\n"
            "[ssh_keys]\n"
            'public_keys = ["ssh-ed25519 AAAA primary@test"]\n'
            f"\n{volumes_block}"
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def command_matrix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point create bundle resolution at a temporary matrix file."""
    matrix_path = _write_bundles(tmp_path / "bundles.toml")
    monkeypatch.setattr(
        create_module, "DEFAULT_COMPATIBILITY_MATRIX_PATH", matrix_path)
    return matrix_path


def _install_fakes(
    monkeypatch: pytest.MonkeyPatch,
    client: FakeOpenStackClient,
    pulumi_runner: FakePulumiRunner,
) -> None:
    """Install fake OpenStack and Pulumi factories into the create module."""
    monkeypatch.setattr(
        create_module,
        "create_openstack_preflight_client",
        lambda: client,
    )
    monkeypatch.setattr(
        create_module,
        "create_pulumi_stack_runner",
        lambda logger: pulumi_runner,
    )


def test_create_dry_run_shows_preview_output(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Show the Pulumi preview output without creating resources."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(preview_output="Plan: create 4 resources\n")
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(
        app, ["create", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert result.stdout == "Plan: create 4 resources\n"
    assert fake_runner.preview_calls == 1
    assert fake_runner.up_calls == 0


def test_create_apply_outputs_master_floating_ip(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Print the created cluster master IP on successful apply."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(
        create_result=FakeCreateResult(master_public_ip="198.51.100.20")
    )
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    assert result.stdout.splitlines()[-1] == (
        "Cluster 'test-cluster' created. Master IP: 198.51.100.20"
    )
    assert fake_runner.up_calls == 1


def test_create_raises_image_not_found_with_build_image_hint(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise a specific image error when only the bundle image is missing."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(images=set()),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ImageNotFoundError)
    assert "Run: hailstack build-image" in str(result.exception)
    assert fake_runner.preview_calls == 0
    assert fake_runner.up_calls == 0


def test_create_invalid_config_stops_before_pulumi_calls(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject invalid config before creating a Pulumi runner or stack."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "invalid-create.toml",
        cluster_name="test_cluster",
    )

    monkeypatch.setattr(
        create_module,
        "create_openstack_preflight_client",
        lambda: (_ for _ in ()).throw(
            AssertionError("pre-flight should not run")),
    )
    monkeypatch.setattr(
        create_module,
        "create_pulumi_stack_runner",
        lambda logger: (_ for _ in ()).throw(
            AssertionError("Pulumi should not run")),
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ValidationError)


def test_create_dry_run_does_not_write_pulumi_state(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid state-mutating Pulumi operations during a dry run."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(
        app, ["create", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert fake_runner.checked_backend == 1
    assert fake_runner.preview_calls == 1
    assert fake_runner.up_calls == 0
    assert fake_runner.destroy_calls == 0


def test_create_requires_ceph_s3_credentials_before_pulumi(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject missing Ceph S3 state-backend settings before Pulumi calls."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        ceph_s3_block=(
            "[ceph_s3]\n"
            'endpoint = ""\n'
            'bucket = "hailstack-state"\n'
            'access_key = ""\n'
            'secret_key = ""\n'
        ),
    )
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ConfigError)
    assert str(result.exception) == (
        "Ceph S3 credentials required for Pulumi state backend"
    )
    assert fake_runner.checked_backend == 0


def test_create_invalid_ceph_s3_credentials_raise_s3_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface a clear backend-auth failure before preview or apply."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(
        backend_error=S3Error(
            "Unable to access Ceph S3 backend at ceph.example.invalid"
        )
    )
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, S3Error)
    assert "ceph.example.invalid" in str(result.exception)
    assert fake_runner.preview_calls == 0
    assert fake_runner.up_calls == 0


def test_create_missing_master_flavour_raises_resource_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the missing master flavour in the pre-flight error."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            flavours={"m2.xlarge": FlavorDetails(vcpus=4, ram_mb=16384)}
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "m2.2xlarge" in str(result.exception)
    assert fake_runner.checked_backend == 0


def test_create_missing_network_raises_resource_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the missing network in the aggregated pre-flight error."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(
        networks=set()), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "private-net" in str(result.exception)


def test_create_aggregates_missing_image_and_flavour(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report multiple missing resources in a single error."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            images=set(),
            flavours={"m2.2xlarge": FlavorDetails(vcpus=8, ram_mb=32768)},
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "hailstack-hail-0.2.137-gnomad-3.0.4-r2" in str(result.exception)
    assert "m2.xlarge" in str(result.exception)


def test_create_runs_cleanup_after_failed_apply(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Destroy partial resources automatically when apply fails."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(up_error=PulumiError("apply failed"))
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, PulumiError)
    assert fake_runner.up_calls == 1
    assert fake_runner.destroy_calls == 1


def test_create_success_prints_exact_final_status_line(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End successful create output with the documented final line."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(
        create_result=FakeCreateResult(master_public_ip="192.0.2.45")
    )
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    assert result.stdout.splitlines()[-1] == (
        "Cluster 'test-cluster' created. Master IP: 192.0.2.45"
    )


def test_create_logs_progress_stages_to_stderr(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Log the documented create stages to stderr."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "config loaded" in result.stderr
    assert "bundle resolved" in result.stderr
    assert "pre-flight passed" in result.stderr
    assert "creating infrastructure" in result.stderr
    assert "cluster ready" in result.stderr


def test_create_compute_quota_error_names_exceeded_quota(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report the exceeded compute quota with required and available counts."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(compute_quota=ComputeQuota(instances_available=2)),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, QuotaExceededError)
    assert "instances: need 3, available 2" in str(result.exception)


def test_create_unusable_floating_ip_raises_resource_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject configured floating IPs that are missing or already associated."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        cluster_extra='floating_ip = "1.2.3.4"\n',
    )
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "1.2.3.4" in str(result.exception)


def test_create_missing_existing_volume_raises_resource_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject missing existing volumes during pre-flight validation."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=('[volumes]\nexisting_volume_id = "vol-123"\n'),
    )
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "vol-123" in str(result.exception)


def test_create_aggregates_missing_flavour_and_quota_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report missing resources and quota failures together in one error."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            flavours={"m2.2xlarge": FlavorDetails(vcpus=8, ram_mb=32768)},
            compute_quota=ComputeQuota(instances_available=2),
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert "m2.xlarge" in str(result.exception)
    assert "instances: need 3, available 2" in str(result.exception)
