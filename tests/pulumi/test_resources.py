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

"""Acceptance tests for D2 Pulumi OpenStack resource creation."""

import asyncio
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Never, Protocol, TypedDict, cast

import pulumi
import pulumi.runtime
import pytest

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import PulumiError
from hailstack.pulumi import resources as resources_module
from hailstack.pulumi.resources import (
    create_cluster_resources,
)


class ResourceRecord(TypedDict):
    """Store a normalized resource registration captured by Pulumi mocks."""

    type: str
    name: str
    inputs: dict[str, object]


class InvokeRecord(TypedDict):
    """Store a normalized invoke call captured by Pulumi mocks."""

    token: str
    args: dict[str, object]


class MockResourceArgsView(Protocol):
    """Provide a typed view of Pulumi's unparameterized mock resource args."""

    inputs: Mapping[str, object]


class MockCallArgsView(Protocol):
    """Provide a typed view of Pulumi's unparameterized mock call args."""

    args: Mapping[str, object]


@dataclass
class FakeNetworkLookup:
    """Represent a typed fake OpenStack network lookup result."""

    id: str
    name: str


class RecordingMocks(pulumi.runtime.Mocks):
    """Record mocked Pulumi resources and synthesize stable outputs."""

    def __init__(self) -> None:
        """Initialise the mock recorder."""
        self.resources: list[ResourceRecord] = []
        self.invokes: list[InvokeRecord] = []
        self._next_floating_ip_octet = 10

    def new_resource(
        self, args: pulumi.runtime.MockResourceArgs
    ) -> tuple[str | None, dict[str, object]]:
        """Capture resource registrations and add deterministic outputs."""
        typed_args = cast(MockResourceArgsView, args)
        raw_inputs = typed_args.inputs
        inputs: dict[str, object] = {
            str(key): value for key, value in raw_inputs.items()
        }
        state = dict(inputs)
        state.setdefault("name", inputs.get("name", args.name))
        state.setdefault("tags", [])

        if args.typ == "openstack:networking/port:Port":
            state.setdefault("all_fixed_ips", [self._ip_for_name(args.name)])

        if args.typ == "openstack:networking/floatingIp:FloatingIp":
            state.setdefault(
                "address", f"203.0.113.{self._next_floating_ip_octet}")
            self._next_floating_ip_octet += 1

        resource_id = f"{args.name}-id"
        self.resources.append(
            {
                "type": args.typ,
                "name": args.name,
                "inputs": _normalize_mapping(inputs),
            }
        )
        return resource_id, state

    def call(
        self, args: pulumi.runtime.MockCallArgs
    ) -> tuple[dict[str, object], list[tuple[str, str]]]:
        """Mock data-source calls used by the resource layer."""
        typed_args = cast(MockCallArgsView, args)
        raw_args = typed_args.args
        invoke_args: dict[str, object] = {
            str(key): value for key, value in raw_args.items()
        }
        self.invokes.append({"token": args.token, "args": invoke_args})
        if args.token == "openstack:networking/getNetwork:getNetwork":
            network_name = str(invoke_args["name"])
            return {"id": f"{network_name}-id", "name": network_name}, []
        return {}, []

    @staticmethod
    def _ip_for_name(name: str) -> str:
        """Return a stable private IP for a mocked port name."""
        if name.endswith("master-port"):
            return "10.0.0.10"
        if "lustre-port" in name:
            suffix = int(name.rsplit("-", 1)[1])
            return f"10.1.0.{suffix + 10}"
        suffix = int(name.rsplit("-", 1)[1])
        return f"10.0.0.{suffix + 10}"


def _bundle() -> Bundle:
    """Return a representative compatibility bundle."""
    return Bundle(
        id="hail-0.2.137-gnomad-3.0.4-r2",
        hail="0.2.137",
        spark="3.5.6",
        hadoop="3.4.1",
        java="11",
        python="3.12",
        scala="2.12.18",
        gnomad="3.0.4",
        status="latest",
    )


def _config(**overrides: object) -> ClusterConfig:
    """Build a minimal cluster config with optional overrides."""
    document: dict[str, object] = {
        "cluster": {
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "ssh_username": "ubuntu",
            "floating_ip": "",
        },
        "ssh_keys": {"public_keys": ["ssh-ed25519 AAAA primary@test"]},
    }
    document.update(overrides)
    return ClusterConfig.model_validate(document)


def _run_stack(
    config: ClusterConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[RecordingMocks, dict[str, object], dict[str, object]]:
    """Run the Pulumi program under mocks and resolve returned outputs."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")
    monkeypatch.setenv("HAILSTACK_VOLUME_PASSWORD", "volume-secret")
    mocks = RecordingMocks()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        pulumi.runtime.set_mocks(mocks, project="hailstack", stack="test")

        exported: dict[str, object] = {}

        def _export(name: str, value: object) -> None:
            exported[name] = value

        monkeypatch.setattr(
            pulumi,
            "export",
            _export,
        )

        outputs = create_cluster_resources(config, _bundle())

        resolved_outputs = {
            name: _resolve_output(loop, output) for name, output in outputs.items()
        }
        resolved_exports = {
            name: value
            if not isinstance(value, pulumi.Output)
            else _resolve_output(loop, cast(pulumi.Output[object], value))
            for name, value in exported.items()
        }
        _drain_resource_registrations(loop, mocks)
        return mocks, resolved_outputs, resolved_exports
    finally:
        loop.run_until_complete(asyncio.sleep(0))
        loop.close()
        asyncio.set_event_loop(None)


def _drain_resource_registrations(
    loop: asyncio.AbstractEventLoop,
    mocks: RecordingMocks,
) -> None:
    """Wait until mock resource registrations stop increasing."""
    stable_iterations = 0
    previous_count = -1

    for _ in range(50):
        loop.run_until_complete(asyncio.sleep(0.01))
        current_count = len(mocks.resources)
        pending_tasks = [task for task in asyncio.all_tasks(
            loop) if not task.done()]
        if current_count == previous_count and not pending_tasks:
            stable_iterations += 1
            if stable_iterations >= 2:
                return
            continue

        previous_count = current_count
        stable_iterations = 0


def _resource_inputs(
    mocks: RecordingMocks,
    resource_type: str,
) -> list[dict[str, object]]:
    """Return recorded inputs for a mocked resource type."""
    expected_suffix = resource_type.rsplit(":", 1)[-1].lower()
    return [
        resource["inputs"]
        for resource in mocks.resources
        if resource["type"] == resource_type
        or re.sub(r"v\d+$", "", str(resource["type"]).rsplit(":", 1)[-1].lower())
        == expected_suffix
    ]


def _resolve_output(
    loop: asyncio.AbstractEventLoop,
    output: pulumi.Output[object],
) -> object:
    """Resolve a Pulumi output during the mocked stack run."""
    resolved = loop.run_until_complete(output.future())
    assert resolved is not None
    return resolved


def _normalize_mapping(value: Mapping[str, object]) -> dict[str, object]:
    """Convert provider input keys to snake_case for stable assertions."""
    normalized: dict[str, object] = {}
    for key, entry in value.items():
        normalized_key = re.sub(r"(?<!^)(?=[A-Z])", "_", str(key)).lower()
        if isinstance(entry, Mapping):
            normalized[normalized_key] = _normalize_mapping(
                cast(Mapping[str, object], entry)
            )
        elif isinstance(entry, list):
            normalized[normalized_key] = [
                _normalize_value(item) for item in cast(list[object], entry)
            ]
        else:
            normalized[normalized_key] = entry
    return normalized


def _normalize_value(value: object) -> object:
    """Normalize nested provider values for stable assertions."""
    if isinstance(value, Mapping):
        return _normalize_mapping(cast(Mapping[str, object], value))
    if isinstance(value, list):
        return [_normalize_value(item) for item in cast(list[object], value)]
    return value


def _extract_netdata_api_key(rendered_cloud_init: str) -> str:
    """Extract the Netdata API key from a rendered cloud-init payload."""
    match = re.search(r"api key = ([0-9a-f-]{36})", rendered_cloud_init)
    assert match is not None
    return match.group(1)


def test_num_workers_creates_master_keypair_ports_and_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create one tagged keypair, four main ports, and four instances."""
    mocks, _, _ = _run_stack(_config(), monkeypatch)

    keypairs = _resource_inputs(mocks, "openstack:compute/keypair:Keypair")
    ports = _resource_inputs(mocks, "openstack:networking/port:Port")
    instances = _resource_inputs(mocks, "openstack:compute/instance:Instance")

    assert len(keypairs) == 1
    assert keypairs[0]["name"] == "test-cluster-keypair"
    assert keypairs[0]["value_specs"] == {
        "cluster_name": "test-cluster",
        "tags": "test-cluster",
    }
    assert len(ports) == 4
    assert len(instances) == 4
    assert {instance["name"] for instance in instances} == {
        "test-cluster-master",
        "test-cluster-worker-01",
        "test-cluster-worker-02",
        "test-cluster-worker-03",
    }


def test_master_ssh_toggle_creates_tcp_22_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    """Create a master ingress rule for SSH when enabled."""
    mocks, _, _ = _run_stack(_config(), monkeypatch)

    rules = _resource_inputs(
        mocks, "openstack:networking/secGroupRule:SecGroupRule")

    assert any(
        rule["port_range_min"] == 22
        and rule["port_range_max"] == 22
        and rule["protocol"] == "tcp"
        and rule["remote_ip_prefix"] == "0.0.0.0/0"
        for rule in rules
    )


def test_master_netdata_toggle_false_skips_port_19999(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omit the Netdata ingress rule when monitoring exposure is disabled."""
    config = _config(
        security_groups={
            "master": {
                "ssh": True,
                "http": True,
                "https": True,
                "spark_master": True,
                "jupyter": True,
                "hdfs": True,
                "netdata": False,
            }
        }
    )

    mocks, _, _ = _run_stack(config, monkeypatch)
    rules = _resource_inputs(
        mocks, "openstack:networking/secGroupRule:SecGroupRule")

    assert not any(rule.get("port_range_min") == 19999 for rule in rules)


def test_master_allows_all_tcp_from_worker_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Always allow all TCP from the worker security group into the master SG."""
    mocks, _, _ = _run_stack(_config(), monkeypatch)

    rules = _resource_inputs(
        mocks, "openstack:networking/secGroupRule:SecGroupRule")

    assert any(
        rule["port_range_min"] == 1
        and rule["port_range_max"] == 65535
        and rule["protocol"] == "tcp"
        and str(rule["remote_group_id"]).endswith("test-cluster-worker-sg-id")
        for rule in rules
    )


def test_volume_creation_attaches_requested_size_to_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create a cluster-tagged Cinder volume and attach it to the master."""
    config = _config(
        volumes={"create": True, "name": "my-data-vol", "size_gb": 500})

    mocks, _, _ = _run_stack(config, monkeypatch)
    volumes = _resource_inputs(mocks, "openstack:blockstorage/volume:Volume")
    attachments = [
        resource["inputs"]
        for resource in mocks.resources
        if "instance_id" in resource["inputs"] and "volume_id" in resource["inputs"]
    ]

    assert len(volumes) == 1
    assert volumes[0]["name"] == "my-data-vol"
    assert volumes[0]["size"] == 500
    assert volumes[0]["metadata"] == {
        "cluster_name": "test-cluster",
        "tags": "test-cluster",
    }
    assert len(attachments) == 1
    assert attachments[0]["instance_id"] == "test-cluster-master-id"
    assert attachments[0]["device"] == "/dev/vdb"


def test_existing_volume_id_attaches_without_creating_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Attach a configured existing volume ID without creating a new volume."""
    recorded: dict[str, object] = {}

    class FakeVolumeAttach:
        def __init__(
            self,
            resource_name: str,
            *,
            instance_id: str,
            volume_id: str,
            device: str,
        ) -> None:
            recorded["attachment_name"] = resource_name
            recorded["instance_id"] = instance_id
            recorded["volume_id"] = volume_id
            recorded["device"] = device
            self.id = f"{resource_name}-id"

    def fail_if_volume_created(*args: object, **kwargs: object) -> Never:
        del args, kwargs
        raise AssertionError(
            "Volume() should not be called for existing_volume_id")

    monkeypatch.setattr(resources_module, "Volume", fail_if_volume_created)
    monkeypatch.setattr(resources_module, "VolumeAttach", FakeVolumeAttach)

    volume, attachment = resources_module.create_or_attach_volume(
        _config(volumes={"existing_volume_id": "volume-123"}),
        "test-cluster",
        "test-cluster-master-id",
        ["test-cluster"],
    )

    assert volume is None
    assert attachment is not None
    assert recorded["attachment_name"] == "test-cluster-vol-attach"
    assert recorded["instance_id"] == "test-cluster-master-id"
    assert recorded["volume_id"] == "volume-123"
    assert recorded["device"] == "/dev/vdb"


def test_whitespace_existing_volume_id_is_treated_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ignore blank existing volume IDs instead of trying a provider attach."""

    def fail_if_volume_created(*args: object, **kwargs: object) -> Never:
        del args, kwargs
        raise AssertionError(
            "Volume() should not be called for blank existing_volume_id"
        )

    def fail_if_volume_attached(*args: object, **kwargs: object) -> Never:
        del args, kwargs
        raise AssertionError(
            "VolumeAttach() should not be called for blank existing_volume_id"
        )

    monkeypatch.setattr(resources_module, "Volume", fail_if_volume_created)
    monkeypatch.setattr(resources_module, "VolumeAttach",
                        fail_if_volume_attached)

    volume, attachment = resources_module.create_or_attach_volume(
        _config(volumes={"existing_volume_id": "   "}),
        "test-cluster",
        "test-cluster-master-id",
        ["test-cluster"],
    )

    assert volume is None
    assert attachment is None


def test_preserve_on_destroy_true_marks_created_volume_for_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mark created volumes for retention so destroy detaches without deleting."""
    recorded: dict[str, object] = {}

    class FakeVolume:
        def __init__(
            self,
            resource_name: str,
            *,
            name: str,
            size: int,
            metadata: dict[str, str],
            opts: pulumi.ResourceOptions | None = None,
        ) -> None:
            recorded["volume_name"] = resource_name
            recorded["name"] = name
            recorded["size"] = size
            recorded["metadata"] = metadata
            recorded["opts"] = opts
            self.id = f"{resource_name}-id"

    class FakeVolumeAttach:
        def __init__(
            self,
            resource_name: str,
            *,
            instance_id: str,
            volume_id: str,
            device: str,
        ) -> None:
            recorded["attachment_name"] = resource_name
            recorded["instance_id"] = instance_id
            recorded["volume_id"] = volume_id
            recorded["device"] = device
            self.id = f"{resource_name}-id"

    monkeypatch.setattr(resources_module, "Volume", FakeVolume)
    monkeypatch.setattr(resources_module, "VolumeAttach", FakeVolumeAttach)

    volume, attachment = resources_module.create_or_attach_volume(
        _config(
            volumes={
                "create": True,
                "name": "my-data-vol",
                "size_gb": 500,
                "preserve_on_destroy": True,
            }
        ),
        "test-cluster",
        "test-cluster-master-id",
        ["test-cluster"],
    )

    assert volume is not None
    assert attachment is not None
    assert recorded["volume_name"] == "test-cluster-vol"
    assert recorded["name"] == "my-data-vol"
    assert recorded["instance_id"] == "test-cluster-master-id"
    assert recorded["volume_id"] == "test-cluster-vol-id"
    assert recorded["device"] == "/dev/vdb"
    opts = recorded["opts"]
    assert isinstance(opts, pulumi.ResourceOptions)
    assert opts.retain_on_delete is True


def test_preserve_on_destroy_false_leaves_created_volume_managed_normally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave created volumes owned by Pulumi so destroy deletes them by default."""
    recorded: dict[str, object] = {}

    class FakeVolume:
        def __init__(
            self,
            resource_name: str,
            *,
            name: str,
            size: int,
            metadata: dict[str, str],
            opts: pulumi.ResourceOptions | None = None,
        ) -> None:
            recorded["opts"] = opts
            self.id = f"{resource_name}-id"

    class FakeVolumeAttach:
        def __init__(
            self,
            resource_name: str,
            *,
            instance_id: str,
            volume_id: str,
            device: str,
        ) -> None:
            recorded["instance_id"] = instance_id
            recorded["volume_id"] = volume_id
            recorded["device"] = device
            self.id = f"{resource_name}-id"

    monkeypatch.setattr(resources_module, "Volume", FakeVolume)
    monkeypatch.setattr(resources_module, "VolumeAttach", FakeVolumeAttach)

    volume, attachment = resources_module.create_or_attach_volume(
        _config(volumes={"create": True, "size_gb": 500,
                "preserve_on_destroy": False}),
        "test-cluster",
        "test-cluster-master-id",
        ["test-cluster"],
    )

    assert volume is not None
    assert attachment is not None
    assert recorded["instance_id"] == "test-cluster-master-id"
    assert recorded["volume_id"] == "test-cluster-vol-id"
    assert recorded["device"] == "/dev/vdb"
    opts = recorded["opts"]
    assert isinstance(opts, pulumi.ResourceOptions)
    assert opts.retain_on_delete is False


def test_failed_create_cleanup_overrides_preserve_on_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disable volume retention when cleaning up a failed first-time create."""
    recorded: dict[str, object] = {}

    class FakeVolume:
        def __init__(
            self,
            resource_name: str,
            *,
            name: str,
            size: int,
            metadata: dict[str, str],
            opts: pulumi.ResourceOptions | None = None,
        ) -> None:
            del resource_name, name, size, metadata
            recorded["opts"] = opts
            self.id = "test-cluster-vol-id"

    class FakeVolumeAttach:
        def __init__(
            self,
            resource_name: str,
            *,
            instance_id: str,
            volume_id: str,
            device: str,
        ) -> None:
            del resource_name, instance_id, volume_id, device
            self.id = "test-cluster-vol-attach-id"

    monkeypatch.setattr(resources_module, "Volume", FakeVolume)
    monkeypatch.setattr(resources_module, "VolumeAttach", FakeVolumeAttach)

    volume, attachment = resources_module.create_or_attach_volume(
        _config(
            volumes={
                "create": True,
                "name": "my-data-vol",
                "size_gb": 500,
                "preserve_on_destroy": True,
            }
        ),
        "test-cluster",
        "test-cluster-master-id",
        ["test-cluster"],
        retain_created_volume=False,
    )

    assert volume is not None
    assert attachment is not None
    opts = recorded["opts"]
    assert isinstance(opts, pulumi.ResourceOptions)
    assert opts.retain_on_delete is False


def test_lustre_ports_use_numbered_name_pattern_for_master_and_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the D2 numbered lustre port naming pattern for every node."""
    config = _config(
        cluster={
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "lustre_network": "lustre-net",
            "ssh_username": "ubuntu",
            "floating_ip": "",
        }
    )

    mocks, _, _ = _run_stack(config, monkeypatch)
    ports = _resource_inputs(mocks, "openstack:networking/port:Port")
    lustre_port_names = sorted(
        str(port["name"]) for port in ports if "lustre-port" in str(port["name"])
    )

    assert lustre_port_names == [
        "test-cluster-lustre-port-00",
        "test-cluster-lustre-port-01",
        "test-cluster-lustre-port-02",
        "test-cluster-lustre-port-03",
    ]


def test_empty_floating_ip_allocates_new_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocate a fresh floating IP when none is supplied."""
    config = _config(
        cluster={
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "ssh_username": "ubuntu",
            "floating_ip": "",
            "floating_ip_pool": "public",
        }
    )

    mocks, resolved_outputs, _ = _run_stack(config, monkeypatch)

    floating_ips = _resource_inputs(
        mocks, "openstack:networking/floatingIp:FloatingIp")
    associations = _resource_inputs(
        mocks, "openstack:networking/floatingIpAssociate:FloatingIpAssociate"
    )

    assert len(floating_ips) == 1
    assert "address" not in floating_ips[0]
    assert floating_ips[0]["pool"] == "public"
    assert floating_ips[0]["port_id"] == "test-cluster-master-port-id"
    assert associations == []
    assert resolved_outputs["master_public_ip"] == "203.0.113.10"


def test_packer_floating_ip_pool_remains_backwards_compatible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Continue using the legacy packer pool when cluster.floating_ip_pool is unset."""
    config = _config(
        packer={
            "base_image": "ubuntu-22.04",
            "floating_ip_pool": "public",
        }
    )

    mocks, _, _ = _run_stack(config, monkeypatch)
    floating_ips = _resource_inputs(
        mocks, "openstack:networking/floatingIp:FloatingIp")

    assert len(floating_ips) == 1
    assert floating_ips[0]["pool"] == "public"


def test_existing_floating_ip_is_associated_to_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reuse the configured floating IP on the stable managed resource name."""
    config = _config(
        cluster={
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "ssh_username": "ubuntu",
            "floating_ip": "1.2.3.4",
        },
        packer={
            "base_image": "ubuntu-22.04",
            "floating_ip_pool": "public",
        },
    )

    mocks, resolved_outputs, _ = _run_stack(config, monkeypatch)
    floating_ips = _resource_inputs(
        mocks, "openstack:networking/floatingIp:FloatingIp")
    associations = _resource_inputs(
        mocks, "openstack:networking/floatingIpAssociate:FloatingIpAssociate"
    )

    assert floating_ips == []
    assert resolved_outputs["master_public_ip"] == "1.2.3.4"
    assert len(associations) == 1
    assert associations[0]["floating_ip"] == "1.2.3.4"
    assert associations[0]["port_id"] == "test-cluster-master-port-id"


def test_destroy_behavior_is_encoded_in_resource_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocate new IPs normally but retain user-supplied addresses on destroy."""
    allocated_mocks, _, _ = _run_stack(
        _config(
            packer={
                "base_image": "ubuntu-22.04",
                "floating_ip_pool": "public",
            }
        ),
        monkeypatch,
    )
    reused_mocks, _, _ = _run_stack(
        _config(
            cluster={
                "name": "test-cluster",
                "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
                "num_workers": 3,
                "master_flavour": "m2.2xlarge",
                "worker_flavour": "m2.xlarge",
                "network_name": "private-net",
                "ssh_username": "ubuntu",
                "floating_ip": "1.2.3.4",
            },
            packer={
                "base_image": "ubuntu-22.04",
                "floating_ip_pool": "public",
            },
        ),
        monkeypatch,
    )

    allocated_floating_ips = _resource_inputs(
        allocated_mocks, "openstack:networking/floatingIp:FloatingIp"
    )
    allocated_associations = _resource_inputs(
        allocated_mocks, "openstack:networking/floatingIpAssociate:FloatingIpAssociate"
    )
    reused_floating_ips = _resource_inputs(
        reused_mocks, "openstack:networking/floatingIp:FloatingIp"
    )
    reused_associations = _resource_inputs(
        reused_mocks, "openstack:networking/floatingIpAssociate:FloatingIpAssociate"
    )

    assert len(allocated_floating_ips) == 1
    assert allocated_floating_ips[0]["port_id"] == "test-cluster-master-port-id"
    assert allocated_associations == []
    assert reused_floating_ips == []
    assert len(reused_associations) == 1
    assert reused_associations[0]["floating_ip"] == "1.2.3.4"
    assert reused_associations[0]["port_id"] == "test-cluster-master-port-id"


def test_existing_floating_ip_is_retained_on_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reuse user-supplied floating IPs via association instead of allocation."""
    recorded: dict[str, object] = {}

    class FakeFloatingIpAssociate:
        def __init__(
            self,
            resource_name: str,
            *,
            floating_ip: str,
            port_id: str,
            opts: pulumi.ResourceOptions | None = None,
        ) -> None:
            recorded["resource_name"] = resource_name
            recorded["floating_ip"] = floating_ip
            recorded["port_id"] = port_id
            recorded["opts"] = opts

    monkeypatch.setattr(
        resources_module,
        "FloatingIpAssociate",
        FakeFloatingIpAssociate,
    )
    monkeypatch.setattr(
        resources_module.pulumi.Output,
        "from_input",
        staticmethod(lambda value: value),
    )

    result = resources_module._create_master_floating_ip(
        _config(
            cluster={
                "name": "test-cluster",
                "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
                "num_workers": 3,
                "master_flavour": "m2.2xlarge",
                "worker_flavour": "m2.xlarge",
                "network_name": "private-net",
                "ssh_username": "ubuntu",
                "floating_ip": "1.2.3.4",
            }
        ),
        "test-cluster",
        ["test-cluster"],
        cast(object, SimpleNamespace(id="test-cluster-master-port-id")),
    )

    assert result == "1.2.3.4"
    assert recorded["resource_name"] == "test-cluster-fip"
    assert recorded["floating_ip"] == "1.2.3.4"
    assert recorded["port_id"] == "test-cluster-master-port-id"
    opts = recorded["opts"]
    assert isinstance(opts, pulumi.ResourceOptions)
    assert opts.aliases is not None


def test_exports_include_public_private_ips_worker_names_cluster_and_bundle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Export the documented D2 output values."""
    _, resolved_outputs, resolved_exports = _run_stack(_config(), monkeypatch)

    assert resolved_outputs == {
        "master_public_ip": "203.0.113.10",
        "master_private_ip": "10.0.0.10",
        "worker_private_ips": ["10.0.0.11", "10.0.0.12", "10.0.0.13"],
        "worker_names": [
            "test-cluster-worker-01",
            "test-cluster-worker-02",
            "test-cluster-worker-03",
        ],
        "cluster_name": "test-cluster",
        "bundle_id": "hail-0.2.137-gnomad-3.0.4-r2",
        "master_flavour": "m2.2xlarge",
        "worker_flavour": "m2.xlarge",
        "num_workers": 3,
        "monitoring_enabled": True,
        "managed_volume_size_gb": 0,
    }
    assert resolved_exports == resolved_outputs


def test_all_ssh_keys_are_present_in_master_and_worker_cloud_init(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render full cloud-init payloads that include every configured SSH key."""
    keys = [
        "ssh-rsa AAAA user1@test",
        "ssh-rsa BBBB user2@test",
        "ssh-ed25519 CCCC user3@test",
    ]
    config = _config(ssh_keys={"public_keys": keys})

    mocks, _, _ = _run_stack(config, monkeypatch)
    instances = _resource_inputs(mocks, "openstack:compute/instance:Instance")

    for instance in instances:
        user_data = str(instance["user_data"])
        assert "#!/usr/bin/env bash" in user_data
        assert "/etc/hadoop/conf/core-site.xml" in user_data
        assert all(key in user_data for key in keys)


def test_monitoring_netdata_create_flow_shares_one_api_key_across_all_nodes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the real master and worker cloud-init renderers in one create flow."""
    mocks, _, _ = _run_stack(_config(), monkeypatch)
    instances = _resource_inputs(mocks, "openstack:compute/instance:Instance")
    user_data_by_name = {
        str(instance["name"]): str(instance["user_data"]) for instance in instances
    }

    master_user_data = user_data_by_name["test-cluster-master"]
    worker_user_data = user_data_by_name["test-cluster-worker-01"]
    worker_api_keys = {
        _extract_netdata_api_key(
            user_data_by_name[f"test-cluster-worker-{index:02d}"])
        for index in range(1, 4)
    }

    assert "/etc/jupyter/jupyter_server_config.py" in master_user_data
    assert "location /netdata/" in master_user_data
    assert "url: http://worker-01:9864/jmx" in master_user_data
    assert "destination = 10.0.0.10:19999" in worker_user_data
    assert "10.0.0.11 worker-01 test-cluster-worker-01" in worker_user_data
    assert worker_api_keys == {_extract_netdata_api_key(master_user_data)}


def test_master_cloud_init_targets_attached_volume_id_in_create_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render master cloud-init with the resolved attached volume ID lookup."""
    config = _config(volumes={"create": True, "size_gb": 500})

    mocks, _, _ = _run_stack(config, monkeypatch)
    instances = _resource_inputs(mocks, "openstack:compute/instance:Instance")
    user_data_by_name = {
        str(instance["name"]): str(instance["user_data"]) for instance in instances
    }

    master_user_data = user_data_by_name["test-cluster-master"]

    assert 'ATTACHED_VOLUME_ID="test-cluster-vol-id"' in master_user_data
    assert "lsblk -ndo PATH,SERIAL,TYPE" in master_user_data
    assert (
        "Unable to detect attached data volume for volume ID $ATTACHED_VOLUME_ID"
        in master_user_data
    )


def test_missing_lustre_network_raises_pulumi_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise a Pulumi-specific error when the optional lustre network is absent."""
    config = _config(
        cluster={
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "lustre_network": "missing-lustre",
            "ssh_username": "ubuntu",
            "floating_ip": "",
        }
    )

    def fake_get_network(*, name: str, opts: object | None = None) -> FakeNetworkLookup:
        del opts
        if name == "missing-lustre":
            raise RuntimeError("not found")
        return FakeNetworkLookup(id=f"{name}-id", name=name)

    monkeypatch.setattr(
        "hailstack.pulumi.resources.get_network", fake_get_network)

    with pytest.raises(PulumiError, match="Network 'missing-lustre' not found"):
        _run_stack(config, monkeypatch)


def test_whitespace_lustre_network_is_treated_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ignore blank optional lustre network values instead of looking them up."""
    looked_up_names: list[str] = []

    def fake_get_network(*, name: str, opts: object | None = None) -> FakeNetworkLookup:
        del opts
        looked_up_names.append(name)
        return FakeNetworkLookup(id=f"{name}-id", name=name)

    config = _config(
        cluster={
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "lustre_network": "   ",
            "ssh_username": "ubuntu",
            "floating_ip": "",
        }
    )

    monkeypatch.setattr(
        "hailstack.pulumi.resources.get_network", fake_get_network)

    _run_stack(config, monkeypatch)

    assert looked_up_names == ["private-net"]
