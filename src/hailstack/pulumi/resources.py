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

"""Pulumi OpenStack resource graph creation for Hailstack clusters."""

from collections.abc import Sequence
from uuid import uuid4

import pulumi
from pulumi_openstack.blockstorage.volume import Volume
from pulumi_openstack.compute._inputs import InstanceNetworkArgs
from pulumi_openstack.compute.instance import Instance
from pulumi_openstack.compute.keypair import Keypair
from pulumi_openstack.compute.volume_attach import VolumeAttach
from pulumi_openstack.networking.floating_ip import FloatingIp
from pulumi_openstack.networking.get_network import get_network
from pulumi_openstack.networking.port import Port
from pulumi_openstack.networking.sec_group import SecGroup
from pulumi_openstack.networking.sec_group_rule import SecGroupRule

from hailstack.config import Bundle, ClusterConfig, SecurityGroupConfig
from hailstack.errors import PulumiError
from hailstack.pulumi.cloud_init import (
    generate_master_cloud_init,
    generate_worker_cloud_init,
)

type PortRule = tuple[int, int]

MASTER_RULES: dict[str, PortRule] = {
    "ssh": (22, 22),
    "http": (80, 80),
    "https": (443, 443),
    "spark_master": (7077, 7077),
    "hdfs": (9820, 9820),
    "jupyter": (8888, 8888),
    "netdata": (19999, 19999),
}
WORKER_RULES: dict[str, PortRule] = {
    "hdfs": (9866, 9866),
    "spark_worker": (7078, 7099),
}


def create_cluster_resources(
    config: ClusterConfig,
    bundle: Bundle,
) -> dict[str, pulumi.Output[object]]:
    """Create the OpenStack resource graph for a Hailstack cluster."""
    cluster_name = config.cluster.name
    tags = [cluster_name]
    main_network_id = _lookup_network_id(config.cluster.network_name)
    lustre_network_id = _lookup_optional_network_id(
        config.cluster.lustre_network)

    keypair_name = f"{cluster_name}-keypair"
    keypair = Keypair(
        keypair_name,
        name=keypair_name,
        public_key=config.ssh_keys.public_keys[0],
        value_specs=_cluster_value_specs(cluster_name),
    )

    master_security_group = SecGroup(
        f"{cluster_name}-master-sg",
        name=f"{cluster_name}-master-sg",
        description=f"Master security group for {cluster_name}",
        tags=tags,
    )
    worker_security_group = SecGroup(
        f"{cluster_name}-worker-sg",
        name=f"{cluster_name}-worker-sg",
        description=f"Worker security group for {cluster_name}",
        tags=tags,
    )

    _create_public_rules(
        f"{cluster_name}-master-sg",
        master_security_group.id,
        config.security_groups.master,
        MASTER_RULES,
    )
    _create_public_rules(
        f"{cluster_name}-worker-sg",
        worker_security_group.id,
        config.security_groups.worker,
        WORKER_RULES,
    )
    _create_internal_rule(
        f"{cluster_name}-master-from-workers",
        master_security_group.id,
        worker_security_group.id,
    )
    if config.security_groups.worker.all_tcp_internal:
        _create_internal_rule(
            f"{cluster_name}-worker-from-master",
            worker_security_group.id,
            master_security_group.id,
        )
        _create_internal_rule(
            f"{cluster_name}-worker-from-workers",
            worker_security_group.id,
            worker_security_group.id,
        )

    master_port = _create_port(
        f"{cluster_name}-master-port",
        main_network_id,
        [master_security_group.id],
        tags,
    )
    worker_ports = [
        _create_port(
            f"{cluster_name}-worker-port-{index:02d}",
            main_network_id,
            [worker_security_group.id],
            tags,
        )
        for index in range(1, config.cluster.num_workers + 1)
    ]

    master_networks: list[InstanceNetworkArgs] = [
        InstanceNetworkArgs(port=master_port.id)
    ]
    worker_networks: list[list[InstanceNetworkArgs]] = [
        [InstanceNetworkArgs(port=worker_port.id)] for worker_port in worker_ports
    ]
    if lustre_network_id is not None:
        master_lustre_port = _create_port(
            _lustre_port_name(cluster_name, 0),
            lustre_network_id,
            [master_security_group.id],
            tags,
        )
        master_networks.append(InstanceNetworkArgs(port=master_lustre_port.id))
        for index, worker_network in enumerate(worker_networks, start=1):
            worker_lustre_port = _create_port(
                _lustre_port_name(cluster_name, index),
                lustre_network_id,
                [worker_security_group.id],
                tags,
            )
            worker_network.append(
                InstanceNetworkArgs(port=worker_lustre_port.id))

    master_name = f"{cluster_name}-master"
    worker_names = [
        f"{cluster_name}-worker-{index:02d}"
        for index in range(1, config.cluster.num_workers + 1)
    ]
    master_private_ip = _first_ip(master_port.all_fixed_ips)
    worker_private_ip_outputs = [
        _first_ip(worker_port.all_fixed_ips) for worker_port in worker_ports
    ]
    worker_private_ips = (
        pulumi.Output.all(*worker_private_ip_outputs)
        if worker_private_ip_outputs
        else pulumi.Output.from_input([])
    )
    shared_netdata_api_key = _netdata_api_key(config)

    master_instance = Instance(
        master_name,
        name=master_name,
        flavor_name=config.cluster.master_flavour,
        image_name=f"hailstack-{bundle.id}",
        key_pair=keypair.name,
        networks=master_networks,
        tags=tags,
        metadata=_instance_metadata(cluster_name, bundle.id, "master"),
        user_data=worker_private_ips.apply(
            lambda resolved_worker_ips: _render_master_cloud_init(
                config,
                bundle,
                resolved_worker_ips,
                shared_netdata_api_key,
            )
        ),
    )
    cluster_private_ips = pulumi.Output.all(
        master_private_ip,
        *worker_private_ip_outputs,
    )
    for worker_index, (worker_name, worker_network) in enumerate(
        zip(worker_names, worker_networks, strict=True),
        start=1,
    ):
        Instance(
            worker_name,
            name=worker_name,
            flavor_name=config.cluster.worker_flavour,
            image_name=f"hailstack-{bundle.id}",
            key_pair=keypair.name,
            networks=worker_network,
            tags=tags,
            metadata=_instance_metadata(cluster_name, bundle.id, "worker"),
            user_data=cluster_private_ips.apply(
                lambda resolved_private_ips, worker_index=worker_index: (
                    _render_worker_cloud_init(
                        config,
                        bundle,
                        resolved_private_ips,
                        worker_index,
                        shared_netdata_api_key,
                    )
                )
            ),
        )

    volume, volume_attachment = create_or_attach_volume(
        config,
        cluster_name,
        master_instance.id,
        tags,
    )
    attach_dependency: pulumi.Input[str] | None = None
    if volume is not None and volume_attachment is not None:
        attach_dependency = volume_attachment.id

    master_public_ip = _create_master_floating_ip(
        config,
        cluster_name,
        tags,
        master_port,
    )

    outputs: dict[str, pulumi.Output[object]] = {
        "master_public_ip": master_public_ip,
        "master_private_ip": master_private_ip,
        "worker_private_ips": worker_private_ips,
        "worker_names": pulumi.Output.from_input(worker_names),
        "cluster_name": pulumi.Output.from_input(cluster_name),
        "bundle_id": _with_dependency(bundle.id, attach_dependency),
    }
    for name, value in outputs.items():
        pulumi.export(name, value)

    return outputs


def _lookup_network_id(name: str) -> str:
    """Resolve a required OpenStack network name to its ID."""
    try:
        network = get_network(name=name)
    except Exception as error:  # pragma: no cover - provider failures handled uniformly
        raise PulumiError(f"Network '{name}' not found") from error
    return network.id


def _lookup_optional_network_id(name: str) -> str | None:
    """Resolve an optional network name when present."""
    if not name:
        return None
    return _lookup_network_id(name)


def _create_public_rules(
    resource_prefix: str,
    security_group_id: pulumi.Input[str],
    settings: SecurityGroupConfig,
    rules: dict[str, PortRule],
) -> None:
    """Create ingress rules for all enabled public security-group toggles."""
    for toggle_name, (port_min, port_max) in rules.items():
        if not getattr(settings, toggle_name):
            continue
        SecGroupRule(
            f"{resource_prefix}-{toggle_name}",
            direction="ingress",
            ethertype="IPv4",
            security_group_id=security_group_id,
            protocol="tcp",
            port_range_min=port_min,
            port_range_max=port_max,
            remote_ip_prefix="0.0.0.0/0",
        )


def _create_internal_rule(
    name: str,
    security_group_id: pulumi.Input[str],
    remote_group_id: pulumi.Input[str],
) -> None:
    """Create a full TCP ingress rule from another security group."""
    SecGroupRule(
        name,
        direction="ingress",
        ethertype="IPv4",
        security_group_id=security_group_id,
        protocol="tcp",
        port_range_min=1,
        port_range_max=65535,
        remote_group_id=remote_group_id,
    )


def _create_port(
    name: str,
    network_id: pulumi.Input[str],
    security_group_ids: Sequence[pulumi.Input[str]],
    tags: Sequence[str],
) -> Port:
    """Create an OpenStack Neutron port with the given security groups."""
    return Port(
        name,
        name=name,
        network_id=network_id,
        security_group_ids=list(security_group_ids),
        tags=list(tags),
    )


def _cluster_value_specs(cluster_name: str) -> dict[str, str]:
    """Return value-spec metadata for resources without native tag fields."""
    return {
        "cluster_name": cluster_name,
        "tags": cluster_name,
    }


def _create_master_floating_ip(
    config: ClusterConfig,
    cluster_name: str,
    tags: Sequence[str],
    master_port: Port,
) -> pulumi.Output[str]:
    """Create a managed master floating IP, optionally with a fixed address."""
    pool = None
    if config.packer is not None and config.packer.floating_ip_pool:
        pool = config.packer.floating_ip_pool

    floating_ip = FloatingIp(
        f"{cluster_name}-fip",
        address=config.cluster.floating_ip or None,
        pool=pool,
        port_id=master_port.id,
        tags=list(tags),
    )
    return pulumi.Output.from_input(floating_ip.address)


def _lustre_port_name(cluster_name: str, index: int) -> str:
    """Return the standard D2 lustre port name for a node index."""
    return f"{cluster_name}-lustre-port-{index:02d}"


def _instance_metadata(cluster_name: str, bundle_id: str, role: str) -> dict[str, str]:
    """Return a minimal metadata map shared by all cluster instances."""
    return {
        "cluster_name": cluster_name,
        "bundle_id": bundle_id,
        "role": role,
    }


def _netdata_api_key(config: ClusterConfig) -> str | None:
    """Return a per-create shared Netdata key when monitoring is enabled."""
    if config.cluster.monitoring != "netdata":
        return None
    return str(uuid4())


def _render_master_cloud_init(
    config: ClusterConfig,
    bundle: Bundle,
    worker_ips: Sequence[object],
    netdata_api_key: str | None,
) -> str:
    """Render master user-data from resolved cluster IP addresses."""
    return generate_master_cloud_init(
        config,
        bundle,
        _resolved_ip_list(worker_ips),
        netdata_api_key=netdata_api_key,
    )


def _render_worker_cloud_init(
    config: ClusterConfig,
    bundle: Bundle,
    cluster_private_ips: Sequence[object],
    worker_index: int,
    netdata_api_key: str | None,
) -> str:
    """Render worker user-data from resolved master and worker IP addresses."""
    resolved_private_ips = _resolved_ip_list(cluster_private_ips)
    master_ip = resolved_private_ips[0]
    worker_ips = resolved_private_ips[1:]
    return generate_worker_cloud_init(
        config,
        bundle,
        master_ip,
        worker_index,
        worker_ips=worker_ips,
        netdata_api_key=netdata_api_key,
    )


def _resolved_ip_list(values: Sequence[object]) -> list[str]:
    """Convert resolved Pulumi output values into a validated IP list."""
    resolved_values: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise PulumiError(
                "Expected resolved fixed IP values to be strings")
        resolved_values.append(value)
    return resolved_values


def create_or_attach_volume(
    config: ClusterConfig,
    cluster_name: str,
    master_instance_id: pulumi.Input[str],
    tags: Sequence[str],
) -> tuple[Volume | None, VolumeAttach | None]:
    """Create or attach a volume to the master node when configured."""
    del tags
    volume_id: pulumi.Input[str] | None = None
    if config.volumes.create:
        volume = Volume(
            f"{cluster_name}-vol",
            name=f"{cluster_name}-vol",
            size=config.volumes.size_gb,
            metadata=_cluster_value_specs(cluster_name),
            opts=pulumi.ResourceOptions(
                retain_on_delete=config.volumes.preserve_on_destroy
            ),
        )
        volume_id = volume.id
    elif config.volumes.existing_volume_id:
        volume = None
        volume_id = config.volumes.existing_volume_id
    else:
        return None, None

    attachment = VolumeAttach(
        f"{cluster_name}-vol-attach",
        device="/dev/vdb",
        instance_id=master_instance_id,
        volume_id=volume_id,
    )
    return volume, attachment


def _first_ip(all_fixed_ips: pulumi.Input[Sequence[str]] | None) -> pulumi.Output[str]:
    """Extract the first fixed IP from a port output."""
    return pulumi.Output.from_input(all_fixed_ips).apply(_extract_first_ip)


def _extract_first_ip(ips: Sequence[str] | None) -> str:
    """Return the first IP address from a sequence of fixed IPs."""
    if not ips:
        raise PulumiError("Port did not receive a fixed IP")
    return ips[0]


def _with_dependency(
    value: str,
    dependency: pulumi.Input[str] | None,
) -> pulumi.Output[str]:
    """Return an output that preserves value while depending on another resource."""
    if dependency is None:
        return pulumi.Output.from_input(value)
    return pulumi.Output.from_input(dependency).apply(lambda _dependency: value)


__all__ = ["create_cluster_resources", "create_or_attach_volume"]
