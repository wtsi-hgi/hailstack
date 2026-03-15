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
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Never

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
        unavailable_volumes: set[str] | None = None,
        attached_volumes_by_server: dict[str, set[str]] | None = None,
        volume_names: dict[str, str] | None = None,
        volume_sizes_gb: dict[str, int] | None = None,
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
            available_floating_ips if available_floating_ips is not None else set[str](
            )
        )
        self.existing_volumes = (
            existing_volumes if existing_volumes is not None else set[str]()
        )
        self.unavailable_volumes = (
            unavailable_volumes if unavailable_volumes is not None else set[str](
            )
        )
        self.attached_volumes_by_server = (
            attached_volumes_by_server if attached_volumes_by_server is not None else {}
        )
        self.volume_names = volume_names if volume_names is not None else {}
        self.volume_sizes_gb = volume_sizes_gb if volume_sizes_gb is not None else {}
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

    def volume_is_available(self, volume_id: str) -> bool:
        """Report whether an existing volume can be attached."""
        return (
            volume_id in self.existing_volumes
            and volume_id not in self.unavailable_volumes
        )

    def volume_is_attached_to_server(self, volume_id: str, server_name: str) -> bool:
        """Report whether an existing volume is attached to one server."""
        return volume_id in self.attached_volumes_by_server.get(server_name, set())

    def attached_volume_size_gb(
        self,
        server_name: str,
        *,
        volume_name: str,
    ) -> int | None:
        """Return the size of a named volume attached to one server."""
        for volume_id in self.attached_volumes_by_server.get(server_name, set()):
            if self.volume_names.get(volume_id) == volume_name:
                return self.volume_sizes_gb.get(volume_id)
        return None

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
        cleanup_error: Exception | None = None,
    ) -> None:
        """Initialise fake Pulumi responses."""
        self.preview_output = preview_output
        self.create_result = create_result or FakeCreateResult("203.0.113.10")
        self.backend_error = backend_error
        self.up_error = up_error
        self.cleanup_error = cleanup_error
        self.stack_exists_value = False
        self.current_master_public_ip_value: str | None = None
        self.current_stack_outputs_value: dict[str, object] = {}
        self.checked_backend = 0
        self.stack_exists_calls = 0
        self.preview_calls = 0
        self.up_calls = 0
        self.destroy_calls = 0
        self.cleanup_failed_create_calls = 0

    def check_backend_access(self, config: object) -> None:
        """Record backend validation and optionally fail."""
        del config
        self.checked_backend += 1
        if self.backend_error is not None:
            raise self.backend_error

    def stack_exists(self, config: object) -> bool:
        """Return whether the fake backend already has the target stack."""
        del config
        self.stack_exists_calls += 1
        return self.stack_exists_value

    def preview(
        self,
        config: object,
        bundle: object,
        *,
        stack_exists: bool | None = None,
    ) -> str:
        """Return a preview-only Pulumi output string."""
        del config, bundle, stack_exists
        self.preview_calls += 1
        return self.preview_output

    def current_master_public_ip(self, config: object) -> str | None:
        """Return the current stack master public IP for update preflight."""
        del config
        return self.current_master_public_ip_value

    def current_stack_outputs(self, config: object) -> Mapping[str, object]:
        """Return the current stack outputs for update preflight."""
        del config
        return dict(self.current_stack_outputs_value)

    def up(self, config: object, bundle: object) -> FakeCreateResult:
        """Return a fake create result or fail."""
        del config, bundle
        self.up_calls += 1
        if self.up_error is not None:
            raise self.up_error
        return self.create_result

    def cleanup_failed_create(self, config: object, bundle: object) -> None:
        """Record automatic cleanup calls after failed first-time creates."""
        del config, bundle
        self.cleanup_failed_create_calls += 1
        if self.cleanup_error is not None:
            raise self.cleanup_error


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


def test_openstack_required_show_retries_transient_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry transient OpenStack control-plane failures before succeeding."""
    attempts = {"count": 0}
    sleep_calls: list[float] = []

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        attempts["count"] += 1
        if attempts["count"] < 3:
            return subprocess.CompletedProcess(
                [],
                1,
                stdout="",
                stderr="HTTP 503 Service Unavailable",
            )
        return subprocess.CompletedProcess(
            [],
            0,
            stdout='{"id": "private-net-id"}',
            stderr="",
        )

    monkeypatch.setattr(create_module.subprocess, "run", fake_run)
    monkeypatch.setattr(create_module, "sleep", sleep_calls.append)

    result = create_module.OpenStackCLIClient().get_network("private-net")

    assert result == {"id": "private-net-id"}
    assert attempts["count"] == 3
    assert sleep_calls == [1.0, 2.0]


def test_openstack_required_show_raises_network_error_for_invalid_integer_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Map malformed numeric fields in OpenStack JSON to NetworkError."""

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        return subprocess.CompletedProcess(
            [],
            0,
            stdout='{"vcpus": "many", "ram": 8192}',
            stderr="",
        )

    monkeypatch.setattr(create_module.subprocess, "run", fake_run)

    with pytest.raises(NetworkError, match="invalid integer field 'vcpus'"):
        create_module.OpenStackCLIClient().get_flavour("m2.xlarge")


def test_openstack_optional_show_retries_transient_endpoint_lookup_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry transient endpoint-discovery failures.

    Do not treat them as missing resources.
    """
    attempts = {"count": 0}
    sleep_calls: list[float] = []

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        attempts["count"] += 1
        if attempts["count"] < 3:
            return subprocess.CompletedProcess(
                [],
                1,
                stdout="",
                stderr="public endpoint for network service in RegionOne not found",
            )
        return subprocess.CompletedProcess(
            [],
            0,
            stdout='{"id": "private-net-id"}',
            stderr="",
        )

    monkeypatch.setattr(create_module.subprocess, "run", fake_run)
    monkeypatch.setattr(create_module, "sleep", sleep_calls.append)

    result = create_module.OpenStackCLIClient().get_network("private-net")

    assert result == {"id": "private-net-id"}
    assert attempts["count"] == 3
    assert sleep_calls == [1.0, 2.0]


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
    ssh_keys_block: str | None = None,
    volumes_block: str = "",
) -> Path:
    """Write a minimal valid create configuration file."""
    ceph_s3_content = (
        ceph_s3_block
        if ceph_s3_block is not None
        else (
            "[ceph_s3]\n"
            'endpoint = "https://ceph.example.invalid"\n'
            'bucket = "hailstack-state"\n'
            'access_key = "state-access"\n'
            'secret_key = "state-secret"\n'
        )
    )
    ssh_keys_content = (
        ssh_keys_block
        if ssh_keys_block is not None
        else ('[ssh_keys]\npublic_keys = ["ssh-ed25519 AAAA primary@test"]\n')
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
            f"{ssh_keys_content}"
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

    def fake_create_openstack_preflight_client() -> FakeOpenStackClient:
        return client

    def fake_create_pulumi_stack_runner(logger: object) -> FakePulumiRunner:
        del logger
        return pulumi_runner

    monkeypatch.setattr(
        create_module,
        "create_openstack_preflight_client",
        fake_create_openstack_preflight_client,
    )
    monkeypatch.setattr(
        create_module,
        "create_pulumi_stack_runner",
        fake_create_pulumi_stack_runner,
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
    assert fake_runner.stack_exists_calls == 1
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

    def fail_openstack_client() -> Never:
        raise AssertionError("pre-flight should not run")

    def fail_pulumi_runner(logger: object) -> Never:
        del logger
        raise AssertionError("Pulumi should not run")

    monkeypatch.setattr(
        create_module,
        "create_openstack_preflight_client",
        fail_openstack_client,
    )
    monkeypatch.setattr(
        create_module,
        "create_pulumi_stack_runner",
        fail_pulumi_runner,
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
    assert fake_runner.stack_exists_calls == 1
    assert fake_runner.preview_calls == 1
    assert fake_runner.up_calls == 0
    assert fake_runner.destroy_calls == 0


def test_create_dry_run_invalid_ceph_s3_credentials_raise_s3_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface backend-auth failures for dry-run before preview can start."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(
        backend_error=S3Error(
            "Unable to access Ceph S3 backend at ceph.example.invalid"
        )
    )
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(
        app, ["create", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 1
    assert isinstance(result.exception, S3Error)
    assert "ceph.example.invalid" in str(result.exception)
    assert fake_runner.checked_backend == 1
    assert fake_runner.stack_exists_calls == 0
    assert fake_runner.preview_calls == 0
    assert fake_runner.up_calls == 0


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


def test_create_dry_run_requires_ceph_s3_credentials_before_pulumi(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject missing Ceph S3 state-backend settings for dry-run as well."""
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

    result = runner.invoke(
        app, ["create", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 1
    assert isinstance(result.exception, ConfigError)
    assert str(result.exception) == (
        "Ceph S3 credentials required for Pulumi state backend"
    )
    assert fake_runner.checked_backend == 0
    assert fake_runner.stack_exists_calls == 0
    assert fake_runner.preview_calls == 0


def test_create_dry_run_missing_ceph_credentials_fail_before_preflight(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop before OpenStack preflight when dry-run lacks backend credentials."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        cluster_extra='floating_ip = "1.2.3.4"\n',
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

    result = runner.invoke(
        app, ["create", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 1
    assert isinstance(result.exception, ConfigError)
    assert str(result.exception) == (
        "Ceph S3 credentials required for Pulumi state backend"
    )
    assert fake_runner.checked_backend == 0


def test_create_requires_ssh_public_keys_before_pulumi(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject missing SSH public keys before creating a Pulumi runner."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        ssh_keys_block="",
    )
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ConfigError)
    assert str(result.exception) == "ssh_keys.public_keys required"
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
    assert fake_runner.checked_backend == 1


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


def test_create_missing_lustre_network_raises_resource_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a configured Lustre network before entering Pulumi."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        cluster_extra='lustre_network = "missing-lustre"\n',
    )
    fake_runner = FakePulumiRunner()
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "missing-lustre" in str(result.exception)


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
    assert fake_runner.cleanup_failed_create_calls == 1


def test_create_does_not_destroy_existing_stack_after_failed_apply(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface update failures without tearing down an existing cluster stack."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(up_error=PulumiError("apply failed"))
    fake_runner.stack_exists_value = True
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, PulumiError)
    assert fake_runner.up_calls == 1
    assert fake_runner.cleanup_failed_create_calls == 0


def test_create_existing_stack_allows_attached_floating_ip_preflight(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow updates to proceed when a configured floating IP is already attached."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        cluster_extra='floating_ip = "1.2.3.4"\n',
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    fake_runner.current_master_public_ip_value = "1.2.3.4"
    fake_runner.current_stack_outputs_value = {
        "master_public_ip": "1.2.3.4",
        "worker_names": [
            "test-cluster-worker-01",
            "test-cluster-worker-02",
            "test-cluster-worker-03",
        ],
        "master_flavour": "m2.2xlarge",
        "worker_flavour": "m2.xlarge",
    }
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    assert fake_runner.up_calls == 1


def test_create_existing_stack_validates_changed_floating_ip_value(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject unavailable replacement floating IPs during update preflight."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        cluster_extra='floating_ip = "1.2.3.4"\n',
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    fake_runner.current_master_public_ip_value = "5.6.7.8"
    fake_runner.current_stack_outputs_value = {
        "master_public_ip": "5.6.7.8",
        "worker_names": [
            "test-cluster-worker-01",
            "test-cluster-worker-02",
            "test-cluster-worker-03",
        ],
        "master_flavour": "m2.2xlarge",
        "worker_flavour": "m2.xlarge",
    }
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "1.2.3.4" in str(result.exception)


def test_create_existing_stack_requires_full_compute_quota_for_updates(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Require full compute quota because updates may replace live instances."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    fake_runner.current_stack_outputs_value = {
        "master_public_ip": "203.0.113.10",
        "worker_names": ["test-cluster-worker-01", "test-cluster-worker-02"],
        "master_flavour": "m2.2xlarge",
        "worker_flavour": "m2.xlarge",
    }
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            compute_quota=ComputeQuota(
                instances_available=0,
                cores_available=0,
                ram_mb_available=0,
            ),
            volume_quota=VolumeQuota(gigabytes_available=0),
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, QuotaExceededError)
    assert "instances: need 3, available 0" in str(result.exception)


def test_create_existing_stack_uses_full_compute_quota_for_scale_up_preflight(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use conservative full-capacity checks for updates that may replace nodes."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    fake_runner.current_stack_outputs_value = {
        "master_public_ip": "203.0.113.10",
        "worker_names": ["test-cluster-worker-01"],
        "master_flavour": "m2.2xlarge",
        "worker_flavour": "m2.xlarge",
    }
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            compute_quota=ComputeQuota(
                instances_available=0,
                cores_available=0,
                ram_mb_available=0,
            )
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, QuotaExceededError)
    assert "instances: need 3, available 0" in str(result.exception)


def test_create_preserves_original_up_error_when_cleanup_also_fails(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface the original apply failure even when cleanup also errors."""
    del command_matrix
    config_path = _write_config(tmp_path / "create.toml")
    fake_runner = FakePulumiRunner(
        up_error=PulumiError("apply failed"),
        cleanup_error=PulumiError("cleanup failed"),
    )
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, PulumiError)
    assert str(result.exception) == (
        "apply failed; cleanup after failed create also failed: cleanup failed"
    )
    assert fake_runner.cleanup_failed_create_calls == 1


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


def test_create_unavailable_existing_volume_raises_resource_error(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject existing volumes that are already attached or otherwise unusable."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=('[volumes]\nexisting_volume_id = "vol-123"\n'),
    )
    fake_runner = FakePulumiRunner()
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            existing_volumes={"vol-123"},
            unavailable_volumes={"vol-123"},
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "vol-123" in str(result.exception)
    assert "not available for attachment" in str(result.exception)


def test_create_existing_stack_allows_current_attached_existing_volume(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow reruns to keep using the existing volume already on the stack."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=('[volumes]\nexisting_volume_id = "vol-123"\n'),
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    fake_runner.current_stack_outputs_value = {"attached_volume_id": "vol-123"}
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            existing_volumes={"vol-123"},
            unavailable_volumes={"vol-123"},
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    assert fake_runner.up_calls == 1


def test_create_existing_stack_allows_legacy_existing_volume_attachment(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow reruns for legacy stacks that predate attached volume exports."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=('[volumes]\nexisting_volume_id = "vol-123"\n'),
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            existing_volumes={"vol-123"},
            unavailable_volumes={"vol-123"},
            attached_volumes_by_server={"test-cluster-master": {"vol-123"}},
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    assert fake_runner.up_calls == 1


def test_create_existing_stack_skips_legacy_managed_volume_quota_recheck(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid recharging quota for legacy stacks that predate volume exports."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=(
            '[volumes]\ncreate = true\nname = "legacy-data"\nsize_gb = 100\n'
        ),
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            attached_volumes_by_server={
                "test-cluster-master": {"vol-managed"}},
            volume_names={"vol-managed": "legacy-data"},
            volume_sizes_gb={"vol-managed": 100},
            volume_quota=VolumeQuota(gigabytes_available=0),
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    assert fake_runner.up_calls == 1


def test_create_existing_stack_rejects_repointed_legacy_existing_volume(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject repointing a legacy stack to an unavailable different existing volume."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=('[volumes]\nexisting_volume_id = "vol-new"\n'),
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            existing_volumes={"vol-old", "vol-new"},
            unavailable_volumes={"vol-new"},
            attached_volumes_by_server={"test-cluster-master": {"vol-old"}},
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ResourceNotFoundError)
    assert "vol-new" in str(result.exception)


def test_create_existing_stack_checks_incremental_legacy_managed_volume_quota(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Charge quota only for the additional capacity on legacy managed volumes."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=(
            '[volumes]\ncreate = true\nname = "legacy-data"\nsize_gb = 100\n'
        ),
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    _install_fakes(
        monkeypatch,
        FakeOpenStackClient(
            attached_volumes_by_server={
                "test-cluster-master": {"vol-managed"}},
            volume_names={"vol-managed": "legacy-data"},
            volume_sizes_gb={"vol-managed": 40},
            volume_quota=VolumeQuota(gigabytes_available=50),
        ),
        fake_runner,
    )

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, QuotaExceededError)
    assert "gigabytes: need 60, available 50" in str(result.exception)


def test_create_existing_stack_rejects_managed_volume_shrink(
    command_matrix: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject shrinking an existing managed volume during create updates."""
    del command_matrix
    config_path = _write_config(
        tmp_path / "create.toml",
        volumes_block=(
            '[volumes]\ncreate = true\nname = "legacy-data"\nsize_gb = 40\n'
        ),
    )
    fake_runner = FakePulumiRunner()
    fake_runner.stack_exists_value = True
    fake_runner.current_stack_outputs_value = {"managed_volume_size_gb": 100}
    _install_fakes(monkeypatch, FakeOpenStackClient(), fake_runner)

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ConfigError)
    assert "Managed volumes cannot be shrunk" in str(result.exception)
    assert fake_runner.up_calls == 0


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
