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

"""Pydantic schema models for cluster configuration."""

import re
from ipaddress import IPv4Address
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from hailstack.errors import ConfigError

CLUSTER_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9-]{1,62}$")


class SecurityGroupConfig(BaseModel):
    """Represent a security-group ruleset section."""

    model_config = ConfigDict(extra="forbid", strict=True)

    ssh: bool = True
    http: bool = False
    https: bool = False
    spark_master: bool = False
    spark_worker: bool = False
    jupyter: bool = False
    hdfs: bool = False
    netdata: bool = False
    all_tcp_internal: bool = False


class SecurityGroups(BaseModel):
    """Represent master and worker security-group defaults."""

    model_config = ConfigDict(extra="forbid", strict=True)

    master: SecurityGroupConfig = Field(
        default_factory=lambda: SecurityGroupConfig(
            ssh=True,
            http=True,
            https=True,
            spark_master=True,
            jupyter=True,
            hdfs=True,
            netdata=True,
        )
    )
    worker: SecurityGroupConfig = Field(
        default_factory=lambda: SecurityGroupConfig(
            ssh=False,
            hdfs=True,
            spark_worker=True,
            all_tcp_internal=True,
        )
    )


class VolumeConfig(BaseModel):
    """Represent optional Cinder volume settings."""

    model_config = ConfigDict(extra="forbid", strict=True)

    create: bool = False
    name: str = ""
    size_gb: int = 100
    preserve_on_destroy: bool = False
    existing_volume_id: str = ""

    @model_validator(mode="after")
    def validate_volume_source(self) -> Self:
        """Reject configs that request both a new and existing volume."""
        if self.create and self.existing_volume_id.strip():
            raise ValueError(
                "Cannot set both volumes.create and volumes.existing_volume_id"
            )
        return self


class S3Config(BaseModel):
    """Represent runtime S3A credentials for Hadoop."""

    model_config = ConfigDict(extra="forbid", strict=True)

    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""


class CephS3Config(BaseModel):
    """Represent Ceph S3 settings for state and rollout storage."""

    model_config = ConfigDict(extra="forbid", strict=True)

    endpoint: str = ""
    bucket: str = "hailstack-state"
    access_key: str = ""
    secret_key: str = ""

    def has_required_credentials(self) -> bool:
        """Report whether create-time Ceph S3 settings are populated."""
        return all(
            value.strip()
            for value in (self.endpoint, self.bucket, self.access_key, self.secret_key)
        )


class SSHKeysConfig(BaseModel):
    """Represent SSH public keys to inject onto cluster nodes."""

    model_config = ConfigDict(extra="forbid", strict=True)

    public_keys: list[str] = Field(default_factory=list)

    @field_validator("public_keys")
    @classmethod
    def validate_public_keys(cls, value: list[str]) -> list[str]:
        """Require at least one explicitly configured non-empty SSH key."""
        if not value:
            raise ValueError("At least one SSH public key required")
        if any(not key.strip() for key in value):
            raise ValueError("SSH public keys cannot be empty")
        return value


class DNSConfig(BaseModel):
    """Represent optional DNS search-domain settings."""

    model_config = ConfigDict(extra="forbid", strict=True)

    search_domains: str = ""


class ExtrasConfig(BaseModel):
    """Represent optional extra package installation lists."""

    model_config = ConfigDict(extra="forbid", strict=True)

    system_packages: list[str] = Field(default_factory=list)
    python_packages: list[str] = Field(default_factory=list)


class PackerConfig(BaseModel):
    """Represent image-build settings."""

    model_config = ConfigDict(extra="forbid", strict=True)

    base_image: str
    flavour: str = "m2.medium"
    floating_ip_pool: str = ""

    @field_validator("base_image")
    @classmethod
    def validate_base_image(cls, value: str) -> str:
        """Reject blank base-image values when packer settings are present."""
        if not value.strip():
            raise ValueError("packer.base_image required")
        return value


class ClusterSettings(BaseModel):
    """Represent the cluster-specific configuration section."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    bundle: str = ""
    num_workers: int = 2
    master_flavour: str
    worker_flavour: str = ""
    network_name: str = "cloudforms_network"
    lustre_network: str = ""
    ssh_username: str = "ubuntu"
    monitoring: Literal["netdata", "none"] = "netdata"
    floating_ip: str = ""

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Reject invalid cluster names."""
        if CLUSTER_NAME_PATTERN.fullmatch(value) is None:
            raise ValueError(
                "Invalid cluster name: must match ^[a-z][a-z0-9-]{1,62}$")
        return value

    @field_validator("monitoring", mode="before")
    @classmethod
    def validate_monitoring(cls, value: str) -> str:
        """Restrict monitoring to the supported values."""
        if value not in {"netdata", "none"}:
            raise ValueError("monitoring must be 'netdata' or 'none'")
        return value

    @field_validator("num_workers")
    @classmethod
    def validate_num_workers(cls, value: int) -> int:
        """Require at least one worker node."""
        if value < 1:
            raise ValueError("num_workers must be >= 1")
        return value

    @field_validator("floating_ip")
    @classmethod
    def validate_floating_ip(cls, value: str) -> str:
        """Require floating_ip to be empty or a valid IPv4 address."""
        if not value:
            return value
        try:
            IPv4Address(value)
        except ValueError as error:
            raise ValueError(
                "floating_ip must be valid IPv4 address") from error
        return value

    @model_validator(mode="after")
    def default_worker_flavour(self) -> Self:
        """Default the worker flavour to the master flavour."""
        if not self.worker_flavour:
            self.worker_flavour = self.master_flavour
        return self


class ClusterConfig(BaseModel):
    """Represent the top-level TOML configuration document."""

    model_config = ConfigDict(extra="forbid", strict=True)

    cluster: ClusterSettings
    volumes: VolumeConfig = Field(default_factory=VolumeConfig)
    s3: S3Config = Field(default_factory=S3Config)
    ceph_s3: CephS3Config = Field(default_factory=CephS3Config)
    ssh_keys: SSHKeysConfig = Field(default_factory=SSHKeysConfig)
    security_groups: SecurityGroups = Field(default_factory=SecurityGroups)
    dns: DNSConfig = Field(default_factory=DNSConfig)
    extras: ExtrasConfig = Field(default_factory=ExtrasConfig)
    packer: PackerConfig | None = None

    def validate_for_command(self, command: str) -> Self:
        """Apply command-specific validation rules after parsing."""
        if command == "build-image" and self.packer is None:
            raise ConfigError("packer.base_image required")

        if command == "create" and not self.ceph_s3.has_required_credentials():
            raise ConfigError("ceph_s3 credentials required")

        return self


__all__ = [
    "CLUSTER_NAME_PATTERN",
    "CephS3Config",
    "ClusterConfig",
    "ClusterSettings",
    "DNSConfig",
    "ExtrasConfig",
    "PackerConfig",
    "S3Config",
    "SSHKeysConfig",
    "SecurityGroupConfig",
    "SecurityGroups",
    "VolumeConfig",
]
