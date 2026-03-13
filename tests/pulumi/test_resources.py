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
from types import SimpleNamespace
from typing import Protocol, TypedDict, cast

import pulumi
import pulumi.runtime
import pytest

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import PulumiError
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
    previous_count = len(mocks.resources)

    for _ in range(20):
        loop.run_until_complete(asyncio.sleep(0))
        current_count = len(mocks.resources)
        if current_count == previous_count:
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
    config = _config(volumes={"create": True, "size_gb": 500})

    mocks, _, _ = _run_stack(config, monkeypatch)
    volumes = _resource_inputs(mocks, "openstack:blockstorage/volume:Volume")
    attachments = [
        resource["inputs"]
        for resource in mocks.resources
        if "host_name" in resource["inputs"] and "volume_id" in resource["inputs"]
    ]

    assert len(volumes) == 1
    assert volumes[0]["name"] == "test-cluster-vol"
    assert volumes[0]["size"] == 500
    assert volumes[0]["metadata"] == {
        "cluster_name": "test-cluster",
        "tags": "test-cluster",
    }
    assert len(attachments) == 1
    assert attachments[0]["host_name"] == "test-cluster-master"


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
        packer={
            "base_image": "ubuntu-22.04",
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


def test_existing_floating_ip_is_associated_to_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the configured floating IP instead of allocating a new one."""
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
    assert associations[0]["floating_ip"] == "1.2.3.4"
    assert associations[0]["port_id"] == "test-cluster-master-port-id"
    assert resolved_outputs["master_public_ip"] == "1.2.3.4"


def test_destroy_behavior_is_encoded_in_resource_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Own allocated IPs and only associate reused IPs so destroy does the right thing."""
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
    }
    assert resolved_exports == resolved_outputs


def test_all_ssh_keys_are_present_in_master_and_worker_cloud_init(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inject every configured SSH public key into each node user-data payload."""
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
        assert all(key in user_data for key in keys)


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

    def fake_get_network(*, name: str, opts: object | None = None) -> SimpleNamespace:
        del opts
        if name == "missing-lustre":
            raise RuntimeError("not found")
        return SimpleNamespace(id=f"{name}-id", name=name)

    monkeypatch.setattr(
        "hailstack.pulumi.resources.get_network", fake_get_network)

    with pytest.raises(PulumiError, match="Network 'missing-lustre' not found"):
        _run_stack(config, monkeypatch)
