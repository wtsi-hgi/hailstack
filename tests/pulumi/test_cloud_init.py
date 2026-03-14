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

"""Acceptance tests for D3 master cloud-init generation."""

import re
from shlex import quote

import pytest

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import ConfigError
from hailstack.pulumi.cloud_init import (
    generate_master_cloud_init,
    generate_worker_cloud_init,
)


def _extract_netdata_api_key(rendered_cloud_init: str) -> str:
    """Extract the Netdata streaming API key from rendered cloud-init."""
    api_key_match = re.search(
        r"api key = ([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
        r"[89ab][0-9a-f]{3}-[0-9a-f]{12})",
        rendered_cloud_init,
    )
    assert api_key_match is not None
    return api_key_match.group(1)


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
    """Build a representative cluster config for cloud-init tests."""
    document: dict[str, object] = {
        "cluster": {
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "ssh_username": "ubuntu",
            "monitoring": "netdata",
        },
        "ssh_keys": {
            "public_keys": [
                "ssh-rsa AAAA user1@test",
                "ssh-rsa BBBB user2@test",
                "ssh-ed25519 CCCC user3@test",
            ]
        },
    }
    document.update(overrides)
    return ClusterConfig.model_validate(document)


def _worker_ips() -> list[str]:
    """Return stable worker private IPs for master config rendering."""
    return ["10.0.0.11", "10.0.0.12", "10.0.0.13"]


def _non_contiguous_worker_ips() -> list[str]:
    """Return worker IPs that cannot be inferred from the master IP."""
    return ["10.0.0.21", "10.0.1.44", "10.0.3.9"]


def _master_ip() -> str:
    """Return a stable master private IP for worker config rendering."""
    return "10.0.0.10"


def test_monitoring_netdata_enables_netdata_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enable the Netdata service on the master when requested."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "systemctl enable netdata" in result
    assert "systemctl restart netdata || systemctl start netdata" in result


def test_master_cloud_init_enables_hailstack_jupyter_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enable the packaged Jupyter service name expected by the image build."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "systemctl enable jupyter-lab" in result
    assert "systemctl restart jupyter-lab || systemctl start jupyter-lab" in result


def test_l1_master_netdata_enables_service_and_stream_accept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enable Netdata on the master and accept worker streams with a UUID4 key."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "systemctl enable netdata" in result
    assert "systemctl restart netdata || systemctl start netdata" in result
    assert "/etc/netdata/stream.conf" in result
    assert "[stream]" in result
    assert "enabled = yes" in result
    assert _extract_netdata_api_key(result)


def test_l1_worker_netdata_streams_to_master_ip_with_shared_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Point worker streaming at the master IP using the same cluster API key."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")
    shared_api_key = "5e79fb15-0bd0-4cf7-8f3d-0e1f0bb7fbe4"

    master_result = generate_master_cloud_init(
        _config(),
        _bundle(),
        _worker_ips(),
        netdata_api_key=shared_api_key,
    )
    worker_result = generate_worker_cloud_init(
        _config(),
        _bundle(),
        _master_ip(),
        1,
        netdata_api_key=shared_api_key,
    )

    assert "destination = 10.0.0.10:19999" in worker_result
    assert _extract_netdata_api_key(master_result) == shared_api_key
    assert _extract_netdata_api_key(worker_result) == shared_api_key


def test_l1_netdata_separate_renders_generate_fresh_api_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generate a fresh UUID4 key for each separate cloud-init render."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    first_result = generate_master_cloud_init(
        _config(), _bundle(), _worker_ips())
    second_result = generate_master_cloud_init(
        _config(), _bundle(), _worker_ips())

    assert _extract_netdata_api_key(first_result) != _extract_netdata_api_key(
        second_result
    )


def test_l1_nginx_proxies_netdata_with_basic_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose the Netdata dashboard behind the existing nginx basic auth."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "location /netdata/" in result
    assert "proxy_pass http://127.0.0.1:19999/;" in result
    assert "auth_basic 'Hailstack';" in result


def test_monitoring_none_omits_all_netdata_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove Netdata service and proxy references when monitoring is disabled."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(
            cluster={
                "name": "test-cluster",
                "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
                "num_workers": 3,
                "master_flavour": "m2.2xlarge",
                "worker_flavour": "m2.xlarge",
                "network_name": "private-net",
                "ssh_username": "ubuntu",
                "monitoring": "none",
            }
        ),
        _bundle(),
        _worker_ips(),
    )

    assert "netdata" not in result


def test_l1_monitoring_none_omits_netdata_from_master_and_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Suppress all Netdata config on both node types when monitoring is disabled."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")
    config = _config(
        cluster={
            "name": "test-cluster",
            "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
            "num_workers": 3,
            "master_flavour": "m2.2xlarge",
            "worker_flavour": "m2.xlarge",
            "network_name": "private-net",
            "ssh_username": "ubuntu",
            "monitoring": "none",
        }
    )

    master_result = generate_master_cloud_init(
        config, _bundle(), _worker_ips())
    worker_result = generate_worker_cloud_init(
        config, _bundle(), _master_ip(), 1)

    assert "netdata" not in master_result
    assert "netdata" not in worker_result


def test_l1_master_netdata_config_includes_hdfs_and_datanode_jmx_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Monitor the NameNode and each DataNode JMX endpoint from the master."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "http://localhost:9870/jmx" in result
    assert "http://worker-01:9864/jmx" in result
    assert "http://worker-02:9864/jmx" in result
    assert "http://worker-03:9864/jmx" in result


def test_l1_master_netdata_health_alarms_cover_hdfs_capacity_and_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Configure HDFS capacity thresholds plus missing blocks and dead nodes alarms."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "warn: $this > 70" in result
    assert "crit: $this > 80" in result
    assert "missing_blocks" in result
    assert "dead_nodes" in result


def test_hosts_block_contains_master_and_all_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render a deterministic hosts block for the master and each worker."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "127.0.1.1 master test-cluster-master" in result
    assert "10.0.0.11 worker-01 test-cluster-worker-01" in result
    assert "10.0.0.12 worker-02 test-cluster-worker-02" in result
    assert "10.0.0.13 worker-03 test-cluster-worker-03" in result


def test_nginx_config_contains_all_required_proxy_locations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render the documented reverse-proxy locations behind nginx."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    for path in (
        "location /jupyter/",
        "location /spark/",
        "location /sparkhist/",
        "location /yarn/",
        "location /mapreduce/",
        "location /hdfs/",
        "location /nm01/",
        "location /nm02/",
        "location /nm03/",
    ):
        assert path in result


def test_web_password_creates_htpasswd_and_ssl_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create nginx basic-auth and TLS setup when the web password is set."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "htpasswd -bc /etc/nginx/.hailstack-htpasswd hailstack web-secret" in result
    assert "openssl req -x509 -nodes -days 3650" in result
    assert "/etc/nginx/ssl/hailstack.crt" in result
    assert "/etc/nginx/ssl/hailstack.key" in result


def test_web_password_is_shell_quoted_for_htpasswd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shell-quote the htpasswd password so special characters survive cloud-init."""
    password = "pa ss$word'with\"chars"
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", password)

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert (
        f"htpasswd -bc /etc/nginx/.hailstack-htpasswd hailstack {quote(password)}"
        in result
    )


def test_missing_web_password_raises_config_error() -> None:
    """Reject master cloud-init generation without the required web password."""
    with pytest.raises(ConfigError, match="HAILSTACK_WEB_PASSWORD required"):
        generate_master_cloud_init(_config(), _bundle(), _worker_ips())


def test_destroy_rehydration_can_render_without_runtime_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow destroy-time graph rehydration without create-only secret env vars."""
    monkeypatch.delenv("HAILSTACK_WEB_PASSWORD", raising=False)
    monkeypatch.delenv("HAILSTACK_VOLUME_PASSWORD", raising=False)

    result = generate_master_cloud_init(
        _config(volumes={"create": True, "size_gb": 500}),
        _bundle(),
        _worker_ips(),
        allow_missing_runtime_secrets=True,
    )

    assert "destroy-only-web-password" in result
    assert "destroy-only-volume-password" in result


def test_volume_enabled_adds_luks_and_nfs_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Configure encrypted storage and NFS exports when a volume is attached."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")
    monkeypatch.setenv("HAILSTACK_VOLUME_PASSWORD", "volume-secret")

    result = generate_master_cloud_init(
        _config(volumes={"create": True, "size_gb": 500}),
        _bundle(),
        _worker_ips(),
        attached_volume_id="volume-123",
    )

    assert "/etc/hailstack/volume-device" in result
    assert 'ATTACHED_VOLUME_ID="volume-123"' in result
    assert "lsblk -ndo PATH,SERIAL,TYPE" in result
    assert (
        "Unable to detect attached data volume for volume ID $ATTACHED_VOLUME_ID"
        in result
    )
    assert 'cryptsetup isLuks "$VOLUME_DEVICE"' in result
    assert "mkfs.ext4 /dev/mapper/hailstack-data" in result
    assert (
        "mountpoint -q /home/ubuntu/data || mount /dev/mapper/hailstack-data "
        "/home/ubuntu/data" in result
    )
    assert (
        "grep -q '^hailstack-data ' /etc/crypttab || echo "
        '"hailstack-data UUID=$(cat /etc/hailstack/volume-uuid) '
        '/etc/hailstack/volume.key luks" >> /etc/crypttab' in result
    )
    assert "systemctl enable nfs-server" in result
    assert "systemctl restart nfs-server || systemctl start nfs-server" in result
    assert (
        "echo '/home/ubuntu/data *(rw,sync,no_subtree_check,no_root_squash)'"
        " > /etc/exports.d/hailstack.exports" in result
    )


def test_existing_volume_reuses_encrypted_filesystem_without_reformatting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reuse an attached volume by opening and mounting it without reformatting."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")
    monkeypatch.setenv("HAILSTACK_VOLUME_PASSWORD", "volume-secret")

    result = generate_master_cloud_init(
        _config(volumes={"existing_volume_id": "volume-123"}),
        _bundle(),
        _worker_ips(),
        attached_volume_id="volume-123",
    )

    assert (
        'cryptsetup open "$(cat /etc/hailstack/volume-device)" '
        "hailstack-data --key-file /etc/hailstack/volume.key" in result
    )
    assert (
        "mountpoint -q /home/ubuntu/data || mount /dev/mapper/hailstack-data "
        "/home/ubuntu/data" in result
    )
    assert (
        "grep -q '^hailstack-data ' /etc/crypttab || echo "
        '"hailstack-data UUID=$(cat /etc/hailstack/volume-uuid) '
        '/etc/hailstack/volume.key luks" >> /etc/crypttab' in result
    )
    assert "systemctl enable nfs-server" in result
    assert "systemctl restart nfs-server || systemctl start nfs-server" in result
    assert "cryptsetup luksFormat /dev/vdb /etc/hailstack/volume.key" not in result
    assert "mkfs.ext4 /dev/mapper/hailstack-data" not in result


def test_volume_password_is_written_to_the_luks_keyfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write the configured volume password into the LUKS keyfile."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")
    monkeypatch.setenv("HAILSTACK_VOLUME_PASSWORD", "volume-secret")

    result = generate_master_cloud_init(
        _config(volumes={"create": True, "size_gb": 500}),
        _bundle(),
        _worker_ips(),
        attached_volume_id="volume-123",
    )

    assert "cat <<'EOF_VOLUME_KEY' > /etc/hailstack/volume.key" in result
    assert "volume-secret" in result


def test_missing_volume_password_raises_config_error_when_volume_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject encrypted volume provisioning without the LUKS password."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    with pytest.raises(
        ConfigError,
        match="HAILSTACK_VOLUME_PASSWORD required when a data volume is attached",
    ):
        generate_master_cloud_init(
            _config(volumes={"create": True, "size_gb": 500}),
            _bundle(),
            _worker_ips(),
        )


def test_master_cloud_init_creates_hdfs_data_dirs_without_shared_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prepare local HDFS and Spark directories even when no shared volume exists."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(),
        _bundle(),
        _worker_ips(),
    )

    assert "install -d -m 0755 /home/ubuntu/data /home/ubuntu/data/hdfs" in result
    assert "/home/ubuntu/data/hdfs/name" in result
    assert "/home/ubuntu/data/spark-history" in result
    assert "hdfs namenode -format -nonInteractive" in result


def test_worker_cloud_init_creates_hdfs_data_dirs_without_shared_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prepare worker HDFS data directories before starting the DataNode."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_worker_cloud_init(
        _config(),
        _bundle(),
        "10.0.0.10",
        1,
        _worker_ips(),
    )

    assert "install -d -m 0755 /home/ubuntu/data /home/ubuntu/data/hdfs" in result
    assert "/home/ubuntu/data/hdfs/data" in result


def test_dns_search_domain_updates_resolv_conf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist the configured DNS search domain via systemd-resolved."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(dns={"search_domains": "internal.sanger.ac.uk"}),
        _bundle(),
        _worker_ips(),
    )

    assert "/etc/systemd/resolved.conf.d/99-hailstack-search-domains.conf" in result
    assert "Domains=internal.sanger.ac.uk" in result
    assert "systemctl restart systemd-resolved || true" in result


def test_extras_system_packages_are_installed_with_apt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Install configured extra system packages during cloud-init."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(extras={"system_packages": ["libpq-dev"]}),
        _bundle(),
        _worker_ips(),
    )

    assert "apt-get install -y libpq-dev" in result


def test_extras_python_packages_are_installed_into_overlay_venv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Install configured extra Python packages into the overlay venv."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(extras={"python_packages": ["pandas"]}),
        _bundle(),
        _worker_ips(),
    )

    assert (
        "/opt/hailstack/base-venv/bin/python -m venv --system-site-packages "
        "/opt/hailstack/overlay-venv" in result
    )
    assert "hailstack-base-venv.pth" in result
    assert (
        "uv pip install --python /opt/hailstack/overlay-venv/bin/python pandas"
        in result
    )


def test_extras_python_packages_are_shell_quoted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Quote Python requirement arguments so shell metacharacters are preserved."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(
            extras={
                "python_packages": [
                    "pandas>=2.0",
                    "package[extra]",
                    "git+https://example.invalid/pkg.whl",
                ]
            }
        ),
        _bundle(),
        _worker_ips(),
    )

    assert (
        "uv pip install --python /opt/hailstack/overlay-venv/bin/python "
        f"{quote('pandas>=2.0')} {quote('package[extra]')} "
        f"{quote('git+https://example.invalid/pkg.whl')}" in result
    )


def test_all_ssh_public_keys_are_written_to_authorized_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write each configured public key to the login user's authorized_keys file."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "/home/ubuntu/.ssh/authorized_keys" in result
    assert "ssh-rsa AAAA user1@test" in result
    assert "ssh-rsa BBBB user2@test" in result
    assert "ssh-ed25519 CCCC user3@test" in result


def test_s3_settings_inject_s3a_properties_into_core_site(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write the documented S3A properties to core-site.xml when configured."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(
            s3={
                "endpoint": "cog.sanger.ac.uk",
                "access_key": "access-123",
                "secret_key": "secret-123",
            }
        ),
        _bundle(),
        _worker_ips(),
    )

    assert "<name>fs.s3a.endpoint</name>" in result
    assert "<value>cog.sanger.ac.uk</value>" in result
    assert "<name>fs.s3a.access.key</name>" in result
    assert "<value>access-123</value>" in result
    assert "<name>fs.s3a.secret.key</name>" in result
    assert "<value>secret-123</value>" in result
    assert "<name>fs.s3a.connection.maximum</name>" in result
    assert "<value>100</value>" in result


def test_empty_s3_endpoint_omits_s3a_properties(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip S3A properties entirely when no endpoint is configured."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "fs.s3a.endpoint" not in result
    assert "fs.s3a.access.key" not in result
    assert "fs.s3a.secret.key" not in result
    assert "fs.s3a.connection.maximum" not in result


def test_lustre_network_configures_lustre_mount_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prepare a dedicated /lustre mount when the Lustre network is enabled."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(
            cluster={
                "name": "test-cluster",
                "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
                "num_workers": 3,
                "master_flavour": "m2.2xlarge",
                "worker_flavour": "m2.xlarge",
                "network_name": "private-net",
                "lustre_network": "lustre-net",
                "ssh_username": "ubuntu",
                "monitoring": "netdata",
            }
        ),
        _bundle(),
        _worker_ips(),
    )

    assert "install -d -m 0755 /lustre" in result
    assert (
        "grep -q '^10.1.0.1@tcp:/fsx /lustre lustre ' /etc/fstab || echo "
        "'10.1.0.1@tcp:/fsx /lustre lustre defaults,_netdev 0 0' >> /etc/fstab"
        in result
    )
    assert "mountpoint -q /lustre || mount /lustre" in result


def test_lustre_network_uses_configured_mount_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render the configured Lustre target instead of a hardcoded endpoint."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(
        _config(
            cluster={
                "name": "test-cluster",
                "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
                "num_workers": 3,
                "master_flavour": "m2.2xlarge",
                "worker_flavour": "m2.xlarge",
                "network_name": "private-net",
                "lustre_network": "lustre-net",
                "lustre_mount_target": "192.0.2.10@tcp:/custom",
                "ssh_username": "ubuntu",
                "monitoring": "netdata",
            }
        ),
        _bundle(),
        _worker_ips(),
    )

    assert (
        "grep -q '^192.0.2.10@tcp:/custom /lustre lustre ' /etc/fstab || echo "
        "'192.0.2.10@tcp:/custom /lustre lustre defaults,_netdev 0 0' >> /etc/fstab"
        in result
    )
    assert "mountpoint -q /lustre || mount /lustre" in result


def test_nginx_config_is_written_only_to_sites_enabled_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write nginx config to the site file without touching nginx.conf."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "/etc/nginx/sites-enabled/hailstack.conf" in result
    assert "/etc/nginx/nginx.conf" not in result


def test_hadoop_and_spark_config_use_dedicated_conf_directories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write Hadoop and Spark config to dedicated conf directories only."""
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")

    result = generate_master_cloud_init(_config(), _bundle(), _worker_ips())

    assert "/etc/hadoop/conf/core-site.xml" in result
    assert "/etc/hadoop/conf/hdfs-site.xml" in result
    assert "/etc/spark/conf/spark-defaults.conf" in result
    assert "spark.pyspark.python /opt/hailstack/overlay-venv/bin/python" in result
    assert "/etc/hadoop/hadoop-env.sh" not in result
    assert "/etc/default/hadoop" not in result


def test_worker_cloud_init_enables_spark_worker_service() -> None:
    """Enable the Spark worker service on workers."""
    result = generate_worker_cloud_init(_config(), _bundle(), _master_ip(), 2)

    assert "systemctl enable spark-worker" in result
    assert "systemctl restart spark-worker || systemctl start spark-worker" in result


def test_worker_hosts_block_contains_master_and_all_workers() -> None:
    """Render a deterministic hosts block listing the master and each worker."""
    result = generate_worker_cloud_init(
        _config(),
        _bundle(),
        _master_ip(),
        2,
        _non_contiguous_worker_ips(),
    )

    assert "10.0.0.10 master test-cluster-master" in result
    assert "10.0.0.21 worker-01 test-cluster-worker-01" in result
    assert "10.0.1.44 worker-02 test-cluster-worker-02" in result
    assert "10.0.3.9 worker-03 test-cluster-worker-03" in result
    assert "127.0.1.1 worker-02 test-cluster-worker-02" in result


def test_worker_hosts_block_preserves_backwards_compatible_ip_fallback() -> None:
    """Retain the legacy 4-argument call form for existing callers."""
    result = generate_worker_cloud_init(_config(), _bundle(), _master_ip(), 2)

    assert "10.0.0.11 worker-01 test-cluster-worker-01" in result
    assert "10.0.0.12 worker-02 test-cluster-worker-02" in result
    assert "10.0.0.13 worker-03 test-cluster-worker-03" in result


def test_worker_volume_mount_waits_for_master_nfs_and_mounts_data() -> None:
    """Wait for the master NFS service before mounting the shared data path."""
    result = generate_worker_cloud_init(
        _config(volumes={"create": True, "size_gb": 500}),
        _bundle(),
        _master_ip(),
        1,
    )

    assert "until nc -z 10.0.0.10 2049; do sleep 2; done" in result
    assert (
        "mountpoint -q /home/ubuntu/data || mount -t nfs "
        "master:/home/ubuntu/data /home/ubuntu/data" in result
    )
    assert (
        "master:/home/ubuntu/data /home/ubuntu/data nfs "
        "defaults,_netdev,nofail 0 0" in result
    )


def test_worker_monitoring_netdata_enables_streaming_to_master() -> None:
    """Enable Netdata and point worker streaming config at the master."""
    result = generate_worker_cloud_init(_config(), _bundle(), _master_ip(), 1)

    assert "systemctl enable netdata" in result
    assert "systemctl restart netdata || systemctl start netdata" in result
    assert "/etc/netdata/stream.conf" in result
    assert "destination = 10.0.0.10:19999" in result


def test_worker_cloud_init_overrides_baked_spark_worker_master_target() -> None:
    """Override the baked worker unit so it connects to the cluster master."""
    result = generate_worker_cloud_init(_config(), _bundle(), _master_ip(), 1)
    baked_exec_start = (
        "ExecStart=/opt/spark/sbin/start-worker.sh spark://127.0.0.1:7077"
    )

    assert "/etc/systemd/system/spark-worker.service.d/hailstack.conf" in result
    assert "ExecStart=/opt/spark/sbin/start-worker.sh spark://master:7077" in result
    assert baked_exec_start not in result
    assert "systemctl daemon-reload" in result


def test_worker_s3_settings_inject_s3a_properties_into_core_site() -> None:
    """Write the documented S3A properties to worker core-site.xml."""
    result = generate_worker_cloud_init(
        _config(
            s3={
                "endpoint": "cog.sanger.ac.uk",
                "access_key": "access-123",
                "secret_key": "secret-123",
            }
        ),
        _bundle(),
        _master_ip(),
        1,
    )

    assert "<name>fs.s3a.endpoint</name>" in result
    assert "<value>cog.sanger.ac.uk</value>" in result
    assert "<name>fs.s3a.access.key</name>" in result
    assert "<value>access-123</value>" in result
    assert "<name>fs.s3a.secret.key</name>" in result
    assert "<value>secret-123</value>" in result
    assert "<name>fs.s3a.connection.maximum</name>" in result
    assert "<value>100</value>" in result


def test_worker_lustre_network_configures_lustre_mount_point() -> None:
    """Prepare a dedicated /lustre mount on workers when enabled."""
    result = generate_worker_cloud_init(
        _config(
            cluster={
                "name": "test-cluster",
                "bundle": "hail-0.2.137-gnomad-3.0.4-r2",
                "num_workers": 3,
                "master_flavour": "m2.2xlarge",
                "worker_flavour": "m2.xlarge",
                "network_name": "private-net",
                "lustre_network": "lustre-net",
                "ssh_username": "ubuntu",
                "monitoring": "netdata",
            }
        ),
        _bundle(),
        _master_ip(),
        1,
    )

    assert "install -d -m 0755 /lustre" in result
    assert (
        "grep -q '^10.1.0.1@tcp:/fsx /lustre lustre ' /etc/fstab || echo "
        "'10.1.0.1@tcp:/fsx /lustre lustre defaults,_netdev 0 0' >> /etc/fstab"
        in result
    )
    assert "mountpoint -q /lustre || mount /lustre" in result


def test_worker_extras_system_packages_are_installed_with_apt() -> None:
    """Install configured extra system packages during worker cloud-init."""
    result = generate_worker_cloud_init(
        _config(extras={"system_packages": ["libpq-dev"]}),
        _bundle(),
        _master_ip(),
        1,
    )

    assert "apt-get install -y libpq-dev" in result


def test_worker_extras_python_packages_are_installed_into_overlay_venv() -> None:
    """Install configured extra Python packages into the worker overlay venv."""
    result = generate_worker_cloud_init(
        _config(extras={"python_packages": ["pandas"]}),
        _bundle(),
        _master_ip(),
        1,
    )

    assert (
        "/opt/hailstack/base-venv/bin/python -m venv --system-site-packages "
        "/opt/hailstack/overlay-venv" in result
    )
    assert "hailstack-base-venv.pth" in result
    assert (
        "uv pip install --python /opt/hailstack/overlay-venv/bin/python pandas"
        in result
    )


def test_worker_config_uses_dedicated_conf_directories_only() -> None:
    """Write worker config to dedicated conf directories only."""
    result = generate_worker_cloud_init(_config(), _bundle(), _master_ip(), 1)

    assert "/etc/hadoop/conf/core-site.xml" in result
    assert "/etc/hadoop/conf/hdfs-site.xml" in result
    assert "/etc/spark/conf/spark-defaults.conf" in result
    assert "spark.pyspark.python /opt/hailstack/overlay-venv/bin/python" in result
    assert "/etc/netdata/stream.conf" in result
    assert "/etc/hadoop/hadoop-env.sh" not in result
    assert "/etc/default/hadoop" not in result
    assert "/etc/netdata/netdata.conf" not in result
