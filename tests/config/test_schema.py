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

"""Acceptance tests for the B1 cluster configuration schema."""

from pathlib import Path

import pytest

from hailstack.config.parser import load_config
from hailstack.errors import ConfigError, ValidationError


def _write_config(path: Path, content: str) -> Path:
    """Write TOML content to a temporary config path."""
    path.write_text(content, encoding="utf-8")
    return path


def test_minimal_toml_populates_schema_defaults(tmp_path: Path) -> None:
    """Populate optional sections and fields from schema defaults."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
""".strip(),
    )

    result = load_config(config_path)

    assert result.cluster.bundle == ""
    assert result.cluster.num_workers == 2
    assert result.cluster.worker_flavour == "m2.2xlarge"
    assert result.cluster.network_name == "cloudforms_network"
    assert result.cluster.monitoring == "netdata"
    assert result.volumes.create is False
    assert result.ceph_s3.bucket == "hailstack-state"
    assert result.ssh_keys.public_keys == []


def test_worker_flavour_defaults_to_master_flavour(tmp_path: Path) -> None:
    """Use the master flavour for workers when omitted."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
""".strip(),
    )

    result = load_config(config_path)

    assert result.cluster.worker_flavour == result.cluster.master_flavour


def test_monitoring_accepts_none(tmp_path: Path) -> None:
    """Allow monitoring to be disabled explicitly."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
monitoring = "none"
""".strip(),
    )

    result = load_config(config_path)

    assert result.cluster.monitoring == "none"


def test_security_group_defaults_apply_when_master_section_omitted(
    tmp_path: Path,
) -> None:
    """Populate the documented default master security group flags."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
""".strip(),
    )

    result = load_config(config_path)

    assert result.security_groups.master.ssh is True
    assert result.security_groups.master.http is True
    assert result.security_groups.master.https is True
    assert result.security_groups.master.spark_master is True
    assert result.security_groups.master.jupyter is True
    assert result.security_groups.master.hdfs is True
    assert result.security_groups.master.netdata is True


def test_ssh_keys_public_keys_parses_multiple_entries(tmp_path: Path) -> None:
    """Accept multiple SSH public keys."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"

[ssh_keys]
public_keys = [
  "ssh-rsa AAAA user1@host",
  "ssh-rsa BBBB user2@host",
  "ssh-ed25519 CCCC user3@host",
]
""".strip(),
    )

    result = load_config(config_path)

    assert result.ssh_keys.public_keys == [
        "ssh-rsa AAAA user1@host",
        "ssh-rsa BBBB user2@host",
        "ssh-ed25519 CCCC user3@host",
    ]


def test_ssh_keys_explicit_empty_list_is_rejected(tmp_path: Path) -> None:
    """Reject explicitly empty SSH key lists."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"

[ssh_keys]
public_keys = []
""".strip(),
    )

    with pytest.raises(ValidationError, match="At least one SSH public key required"):
        load_config(config_path)


def test_volume_create_and_existing_volume_id_are_mutually_exclusive(
    tmp_path: Path,
) -> None:
    """Reject volume configs that request both creation and reuse."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"

[volumes]
create = true
existing_volume_id = "volume-123"
""".strip(),
    )

    with pytest.raises(
        ValidationError,
        match="Cannot set both volumes.create and volumes.existing_volume_id",
    ):
        load_config(config_path)


def test_monitoring_rejects_unsupported_value(tmp_path: Path) -> None:
    """Reject unsupported monitoring providers."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
monitoring = "prometheus"
""".strip(),
    )

    with pytest.raises(ValidationError, match="must be 'netdata' or 'none'"):
        load_config(config_path)


def test_num_workers_must_be_at_least_one(tmp_path: Path) -> None:
    """Reject zero-worker clusters."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
num_workers = 0
""".strip(),
    )

    with pytest.raises(ValidationError, match="num_workers must be >= 1"):
        load_config(config_path)


def test_build_image_requires_packer_base_image(tmp_path: Path) -> None:
    """Require a base image when validating for build-image."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
""".strip(),
    )

    config = load_config(config_path)

    with pytest.raises(ConfigError, match="packer.base_image required"):
        config.validate_for_command("build-image")


def test_packer_base_image_parses_when_provided(tmp_path: Path) -> None:
    """Parse the packer base image value."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"

[packer]
base_image = "ubuntu-22.04"
""".strip(),
    )

    result = load_config(config_path)

    assert result.packer is not None
    assert result.packer.base_image == "ubuntu-22.04"


def test_extras_system_packages_parse_as_list(tmp_path: Path) -> None:
    """Parse extras.system_packages into its list field."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"

[extras]
system_packages = ["pkg1"]
""".strip(),
    )

    result = load_config(config_path)

    assert result.extras.system_packages == ["pkg1"]


def test_floating_ip_must_be_valid_ipv4_address(tmp_path: Path) -> None:
    """Reject invalid floating IP values."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"
floating_ip = "not-an-ip"
""".strip(),
    )

    with pytest.raises(ValidationError, match="floating_ip must be valid IPv4 address"):
        load_config(config_path)


def test_create_requires_ceph_s3_credentials(tmp_path: Path) -> None:
    """Require Ceph S3 settings for create-time validation."""
    config_path = _write_config(
        tmp_path / "cluster.toml",
        """
[cluster]
name = "test-cluster"
master_flavour = "m2.2xlarge"

[ceph_s3]
access_key = "access"
secret_key = "secret"
""".strip(),
    )

    config = load_config(config_path)

    with pytest.raises(ConfigError, match="ceph_s3 credentials required"):
        config.validate_for_command("create")
