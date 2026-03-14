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

"""Cloud-init renderers for Hailstack cluster nodes."""

import os
from collections.abc import Sequence
from shlex import quote
from uuid import uuid4
from xml.sax.saxutils import escape

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import ConfigError

HADOOP_CONF_DIR = "/etc/hadoop/conf"
SPARK_CONF_DIR = "/etc/spark/conf"
NGINX_SITE_PATH = "/etc/nginx/sites-enabled/hailstack.conf"
HTPASSWD_PATH = "/etc/nginx/.hailstack-htpasswd"
SSL_CERT_PATH = "/etc/nginx/ssl/hailstack.crt"
SSL_KEY_PATH = "/etc/nginx/ssl/hailstack.key"
VOLUME_KEY_PATH = "/etc/hailstack/volume.key"
VOLUME_DEVICE_PATH = "/etc/hailstack/volume-device"
VOLUME_UUID_PATH = "/etc/hailstack/volume-uuid"
BASE_VENV_PATH = "/opt/hailstack/base-venv"
OVERLAY_VENV_PATH = "/opt/hailstack/overlay-venv"
RUNTIME_PYTHON_PATH = f"{OVERLAY_VENV_PATH}/bin/python"
NETDATA_DIR = "/etc/netdata"
NETDATA_STREAM_PATH = f"{NETDATA_DIR}/stream.conf"
NETDATA_GO_D_PATH = f"{NETDATA_DIR}/go.d"
NETDATA_HEALTH_D_PATH = f"{NETDATA_DIR}/health.d"


def _env_var_or_placeholder(
    name: str,
    message: str,
    *,
    allow_missing: bool,
    placeholder: str,
) -> str:
    """Return an env var or an inert placeholder for destroy-time rehydration."""
    value = os.environ.get(name, "").strip()
    if value:
        return value
    if allow_missing:
        return placeholder
    raise ConfigError(message)


def _volume_enabled(config: ClusterConfig) -> bool:
    """Report whether the cluster attaches a shared data volume."""
    return config.volumes.create or bool(config.volumes.existing_volume_id.strip())


def _new_volume_requested(config: ClusterConfig) -> bool:
    """Report whether Pulumi will create a new shared data volume."""
    return config.volumes.create


def _netdata_enabled(config: ClusterConfig) -> bool:
    """Report whether Netdata monitoring is enabled for the cluster."""
    return config.cluster.monitoring == "netdata"


def _here_doc(target_path: str, marker: str, content: str) -> list[str]:
    """Return a shell heredoc that writes deterministic content to a file."""
    return [f"cat <<'{marker}' > {target_path}", content.rstrip(), marker]


def _hosts_content(config: ClusterConfig, worker_ips: Sequence[str]) -> str:
    """Render the /etc/hosts payload for the master node."""
    host_lines = [
        "127.0.0.1 localhost",
        f"127.0.1.1 master {config.cluster.name}-master",
    ]
    for index, worker_ip in enumerate(worker_ips, start=1):
        host_lines.append(
            f"{worker_ip} worker-{index:02d} {config.cluster.name}-worker-{index:02d}"
        )
    return "\n".join(host_lines) + "\n"


def _worker_hosts_content(
    config: ClusterConfig,
    master_ip: str,
    worker_index: int,
    worker_ips: Sequence[str] | None = None,
) -> str:
    """Render a deterministic /etc/hosts payload for a worker node."""
    host_lines = [
        "127.0.0.1 localhost",
        f"{master_ip} master {config.cluster.name}-master",
    ]
    resolved_worker_ips = worker_ips
    if resolved_worker_ips is None:
        prefix, last_octet = master_ip.rsplit(".", maxsplit=1)
        resolved_worker_ips = [
            f"{prefix}.{int(last_octet) + index}"
            for index in range(1, config.cluster.num_workers + 1)
        ]
    for index, worker_ip in enumerate(resolved_worker_ips, start=1):
        host_lines.append(
            f"{worker_ip} worker-{index:02d} {config.cluster.name}-worker-{index:02d}"
        )
    if 1 <= worker_index <= config.cluster.num_workers:
        self_host = (
            f"worker-{worker_index:02d} {config.cluster.name}-worker-{worker_index:02d}"
        )
        host_lines.append(f"127.0.1.1 {self_host}")
    return "\n".join(host_lines) + "\n"


def _authorized_keys_content(config: ClusterConfig) -> str:
    """Render the login user's authorized_keys file content."""
    return "\n".join(config.ssh_keys.public_keys) + "\n"


def _xml_property(name: str, value: str) -> str:
    """Render a Hadoop XML property entry."""
    return (
        "  <property>\n"
        f"    <name>{escape(name)}</name>\n"
        f"    <value>{escape(value)}</value>\n"
        "  </property>"
    )


def _core_site_content(config: ClusterConfig) -> str:
    """Render the cluster's core-site.xml content."""
    properties = [_xml_property("fs.defaultFS", "hdfs://master:9820")]
    if config.s3.endpoint.strip():
        properties.extend(
            [
                _xml_property("fs.s3a.endpoint", config.s3.endpoint),
                _xml_property("fs.s3a.access.key", config.s3.access_key),
                _xml_property("fs.s3a.secret.key", config.s3.secret_key),
                _xml_property("fs.s3a.connection.maximum", "100"),
            ]
        )
    return "<configuration>\n" + "\n".join(properties) + "\n</configuration>\n"


def _hdfs_site_content(config: ClusterConfig) -> str:
    """Render a minimal hdfs-site.xml for the master node."""
    data_dir = f"/home/{config.cluster.ssh_username}/data/hdfs/name"
    return (
        "<configuration>\n"
        + _xml_property("dfs.namenode.name.dir", data_dir)
        + "\n"
        + _xml_property("dfs.replication", "1")
        + "\n</configuration>\n"
    )


def _worker_hdfs_site_content(config: ClusterConfig) -> str:
    """Render a minimal hdfs-site.xml for a worker node."""
    data_dir = f"/home/{config.cluster.ssh_username}/data/hdfs/data"
    return (
        "<configuration>\n"
        + _xml_property("dfs.datanode.data.dir", data_dir)
        + "\n"
        + _xml_property("dfs.client.use.datanode.hostname", "true")
        + "\n</configuration>\n"
    )


def _spark_defaults_content(config: ClusterConfig, bundle: Bundle) -> str:
    """Render cluster-specific spark-defaults.conf content."""
    return (
        "\n".join(
            [
                "spark.master spark://master:7077",
                f"spark.history.fs.logDirectory file:///home/{config.cluster.ssh_username}/data/spark-history",
                f"spark.pyspark.python {RUNTIME_PYTHON_PATH}",
                f"spark.hadoop.hailstack.bundle {bundle.id}",
            ]
        )
        + "\n"
    )


def _jupyter_config_content(web_password: str) -> str:
    """Render a deterministic Jupyter Server config file."""
    return (
        "\n".join(
            [
                "from jupyter_server.auth import passwd",
                "c.ServerApp.base_url = '/jupyter/'",
                "c.ServerApp.password_required = True",
                f"c.ServerApp.password = passwd({web_password!r})",
            ]
        )
        + "\n"
    )


def _nginx_locations(worker_count: int, monitoring_enabled: bool) -> list[str]:
    """Render the configured nginx location blocks."""
    locations = [
        "  location /jupyter/ {",
        "    proxy_pass http://127.0.0.1:8888/jupyter/;",
        "    proxy_http_version 1.1;",
        "    proxy_set_header Upgrade $http_upgrade;",
        "    proxy_set_header Connection 'upgrade';",
        "    proxy_set_header Host $host;",
        "  }",
        "  location /spark/ { proxy_pass http://127.0.0.1:8080/; }",
        "  location /sparkhist/ { proxy_pass http://127.0.0.1:18080/; }",
        "  location /yarn/ { proxy_pass http://127.0.0.1:8088/; }",
        "  location /mapreduce/ { proxy_pass http://127.0.0.1:19888/; }",
        "  location /hdfs/ { proxy_pass http://127.0.0.1:9870/; }",
    ]
    for index in range(1, worker_count + 1):
        backend = f"http://worker-{index:02d}:8042/"
        locations.append(f"  location /nm{index:02d}/ {{ proxy_pass {backend}; }}")
    if monitoring_enabled:
        locations.append("  location /netdata/ { proxy_pass http://127.0.0.1:19999/; }")
    return locations


def _resolve_netdata_api_key(netdata_api_key: str | None) -> str:
    """Return the Netdata streaming API key for a single render flow."""
    return netdata_api_key or str(uuid4())


def _netdata_master_stream_content(netdata_api_key: str) -> str:
    """Render the master-side Netdata streaming accept configuration."""
    return (
        "[stream]\n"
        "    enabled = yes\n"
        f"    api key = {netdata_api_key}\n"
        "    allow from = *\n"
    )


def _netdata_hdfs_jmx_content(worker_count: int) -> str:
    """Render the master-side Netdata HDFS JMX scrape configuration."""
    lines = [
        "jobs:",
        "  - name: namenode",
        "    url: http://localhost:9870/jmx",
    ]
    for index in range(1, worker_count + 1):
        lines.extend(
            [
                f"  - name: datanode-{index:02d}",
                f"    url: http://worker-{index:02d}:9864/jmx",
            ]
        )
    return "\n".join(lines) + "\n"


def _netdata_hdfs_health_content() -> str:
    """Render HDFS health alarms for Netdata on the master."""
    return (
        "alarm: hdfs_capacity_used\n"
        "on: hdfs.capacity_used\n"
        "calc: $this\n"
        "warn: $this > 70\n"
        "crit: $this > 80\n"
        "\n"
        "alarm: hdfs_missing_blocks\n"
        "on: hdfs.missing_blocks\n"
        "calc: $this\n"
        "warn: $this > 0\n"
        "\n"
        "alarm: hdfs_dead_nodes\n"
        "on: hdfs.dead_nodes\n"
        "calc: $this\n"
        "warn: $this > 0\n"
    )


def _master_netdata_commands(config: ClusterConfig, netdata_api_key: str) -> list[str]:
    """Render master-side Netdata configuration commands."""
    if not _netdata_enabled(config):
        return []
    return [
        *_here_doc(
            NETDATA_STREAM_PATH,
            "EOF_NETDATA_STREAM",
            _netdata_master_stream_content(netdata_api_key),
        ),
        *_here_doc(
            f"{NETDATA_GO_D_PATH}/hdfs.conf",
            "EOF_NETDATA_HDFS_JMX",
            _netdata_hdfs_jmx_content(config.cluster.num_workers),
        ),
        *_here_doc(
            f"{NETDATA_HEALTH_D_PATH}/hdfs.conf",
            "EOF_NETDATA_HDFS_HEALTH",
            _netdata_hdfs_health_content(),
        ),
    ]


def _nginx_config_content(config: ClusterConfig) -> str:
    """Render the dedicated nginx site configuration for the master."""
    locations = _nginx_locations(
        config.cluster.num_workers,
        config.cluster.monitoring == "netdata",
    )
    lines = [
        "server {",
        "  listen 80;",
        "  listen 443 ssl;",
        "  server_name _;",
        f"  ssl_certificate {SSL_CERT_PATH};",
        f"  ssl_certificate_key {SSL_KEY_PATH};",
        "  auth_basic 'Hailstack';",
        f"  auth_basic_user_file {HTPASSWD_PATH};",
    ]
    lines.extend(locations)
    lines.append("}")
    return "\n".join(lines) + "\n"


def _service_commands(config: ClusterConfig) -> list[str]:
    """Render systemd activation commands for master services."""
    services = [
        "hdfs-namenode",
        "yarn-rm",
        "mapred-history",
        "spark-master",
        "spark-history-server",
        "jupyter-lab",
        "nginx",
    ]
    if _volume_enabled(config):
        services.append("nfs-server")
    if _netdata_enabled(config):
        services.append("netdata")
    commands: list[str] = []
    for service in services:
        commands.append(f"systemctl enable {service}")
        commands.append(f"systemctl restart {service} || systemctl start {service}")
    return commands


def _worker_service_commands(config: ClusterConfig) -> list[str]:
    """Render systemd activation commands for worker services."""
    services = ["hdfs-datanode", "yarn-nm", "spark-worker"]
    if _netdata_enabled(config):
        services.append("netdata")
    commands: list[str] = []
    for service in services:
        commands.append(f"systemctl enable {service}")
        commands.append(f"systemctl restart {service} || systemctl start {service}")
    return commands


def _spark_worker_override_content() -> str:
    """Render a systemd drop-in that points workers at the cluster master."""
    return (
        "[Service]\n"
        "ExecStart=\n"
        "ExecStart=/opt/spark/sbin/start-worker.sh spark://master:7077\n"
    )


def _worker_spark_commands() -> list[str]:
    """Render worker-specific Spark unit override commands."""
    return [
        "install -d -m 0755 /etc/systemd/system/spark-worker.service.d",
        *_here_doc(
            "/etc/systemd/system/spark-worker.service.d/hailstack.conf",
            "EOF_SPARK_WORKER_OVERRIDE",
            _spark_worker_override_content(),
        ),
        "systemctl daemon-reload",
    ]


def _extras_commands(config: ClusterConfig) -> list[str]:
    """Render optional package installation commands."""
    commands: list[str] = []
    if config.extras.system_packages:
        system_packages = " ".join(
            quote(package) for package in config.extras.system_packages
        )
        commands.extend(
            [
                "apt-get update",
                "apt-get install -y " + system_packages,
            ]
        )
    if config.extras.python_packages:
        packages = " ".join(quote(package) for package in config.extras.python_packages)
        commands.extend(
            [
                f"{BASE_VENV_PATH}/bin/python -m venv --system-site-packages "
                f"{OVERLAY_VENV_PATH}",
                f'BASE_PURELIB=$({BASE_VENV_PATH}/bin/python -c "import '
                "sysconfig; print(sysconfig.get_path('purelib'))\")",
                f'OVERLAY_PURELIB=$({OVERLAY_VENV_PATH}/bin/python -c "import '
                "sysconfig; print(sysconfig.get_path('purelib'))\")",
                'printf "%s\\n" "$BASE_PURELIB" > '
                '"$OVERLAY_PURELIB/hailstack-base-venv.pth"',
                f"uv pip install --python {OVERLAY_VENV_PATH}/bin/python {packages}",
            ]
        )
    return commands


def _dns_commands(config: ClusterConfig) -> list[str]:
    """Render optional DNS search-domain commands."""
    if not config.dns.search_domains.strip():
        return []
    search_domains = config.dns.search_domains.strip()
    return [
        "install -d -m 0755 /etc/systemd/resolved.conf.d",
        *_here_doc(
            "/etc/systemd/resolved.conf.d/99-hailstack-search-domains.conf",
            "EOF_SEARCH_DOMAINS",
            f"[Resolve]\nDomains={search_domains}\n",
        ),
        "systemctl restart systemd-resolved || true",
    ]


def _volume_commands(
    config: ClusterConfig,
    volume_password: str | None,
    attached_volume_id: str | None,
) -> list[str]:
    """Render shared-volume setup commands when a volume is attached."""
    if not _volume_enabled(config):
        return []
    assert volume_password is not None
    assert attached_volume_id is not None
    data_path = f"/home/{config.cluster.ssh_username}/data"
    normalized_attached_volume_id = attached_volume_id.strip().lower().replace("-", "")
    commands = [
        "install -d -m 0755 /etc/hailstack",
        *_here_doc(VOLUME_KEY_PATH, "EOF_VOLUME_KEY", volume_password + "\n"),
        f"chmod 600 {VOLUME_KEY_PATH}",
        f'ATTACHED_VOLUME_ID="{attached_volume_id}"',
        f'ATTACHED_VOLUME_ID_NORMALIZED="{normalized_attached_volume_id}"',
        "VOLUME_DEVICE=$(lsblk -ndo PATH,SERIAL,TYPE | "
        'awk -v volume_id="$ATTACHED_VOLUME_ID" '
        '-v normalized_volume_id="$ATTACHED_VOLUME_ID_NORMALIZED" '
        '\'$3 == "disk" { serial=tolower($2); gsub(/-/, "", serial); '
        "if ($2 == volume_id || serial == normalized_volume_id) "
        "{ print $1; exit } }'); "
        '[ -n "$VOLUME_DEVICE" ] || { '
        'echo "Unable to detect attached data volume for volume ID '
        '$ATTACHED_VOLUME_ID" >&2; exit 1; }; '
        f'printf "%s\\n" "$VOLUME_DEVICE" > {VOLUME_DEVICE_PATH}',
    ]
    if _new_volume_requested(config):
        commands.append(
            f"VOLUME_DEVICE=$(cat {VOLUME_DEVICE_PATH}) && "
            "("
            'cryptsetup isLuks "$VOLUME_DEVICE" || '
            f'cryptsetup luksFormat "$VOLUME_DEVICE" {VOLUME_KEY_PATH} '
            "--batch-mode)"
        )
    commands.append(
        f'cryptsetup luksUUID "$(cat {VOLUME_DEVICE_PATH})" > {VOLUME_UUID_PATH}'
    )
    commands.append(
        "cryptsetup open "
        f'"$(cat {VOLUME_DEVICE_PATH})" hailstack-data --key-file {VOLUME_KEY_PATH}'
    )
    if _new_volume_requested(config):
        commands.append(
            "blkid /dev/mapper/hailstack-data >/dev/null 2>&1 "
            + "|| mkfs.ext4 /dev/mapper/hailstack-data"
        )
    commands.extend(
        [
            f"install -d -m 0755 {data_path}",
            "grep -q '^hailstack-data ' /etc/crypttab "
            + '|| echo "hailstack-data UUID=$(cat '
            + VOLUME_UUID_PATH
            + ") "
            + f'{VOLUME_KEY_PATH} luks" >> /etc/crypttab',
            "mountpoint -q "
            + data_path
            + " || mount /dev/mapper/hailstack-data "
            + data_path,
            "echo '/dev/mapper/hailstack-data "
            + data_path
            + " ext4 defaults,nofail 0 2' >> /etc/fstab",
            "echo '"
            + data_path
            + " *(rw,sync,no_subtree_check,no_root_squash)'"
            + " > /etc/exports.d/hailstack.exports",
            "exportfs -ra",
        ]
    )
    return commands


def _lustre_commands(config: ClusterConfig) -> list[str]:
    """Render optional Lustre mount commands."""
    if not config.cluster.lustre_network.strip():
        return []
    return [
        "install -d -m 0755 /lustre",
        "grep -q '^"
        + config.cluster.lustre_mount_target
        + " /lustre lustre ' /etc/fstab || echo '"
        + config.cluster.lustre_mount_target
        + " /lustre lustre defaults,_netdev 0 0' >> /etc/fstab",
        "mountpoint -q /lustre || mount /lustre",
    ]


def _worker_volume_commands(config: ClusterConfig, master_ip: str) -> list[str]:
    """Render worker-side shared-volume mount commands."""
    if not _volume_enabled(config):
        return []
    data_path = f"/home/{config.cluster.ssh_username}/data"
    return [
        f"install -d -m 0755 {data_path}",
        f"until nc -z {master_ip} 2049; do sleep 2; done",
        "grep -q '^master:"
        + data_path
        + " "
        + data_path
        + " nfs ' /etc/fstab || echo 'master:"
        + data_path
        + " "
        + data_path
        + " nfs defaults,_netdev,nofail 0 0' >> /etc/fstab",
        f"mountpoint -q {data_path} || mount -t nfs master:{data_path} {data_path}",
    ]


def _master_data_commands(config: ClusterConfig) -> list[str]:
    """Render master-side HDFS and Spark data-directory setup commands."""
    data_root = f"/home/{config.cluster.ssh_username}/data"
    name_dir = f"{data_root}/hdfs/name"
    history_dir = f"{data_root}/spark-history"
    return [
        f"install -d -m 0755 {data_root} {data_root}/hdfs {name_dir} {history_dir}",
        "[ -f "
        f"{name_dir}/current/VERSION ] || "
        "/opt/hadoop/bin/hdfs namenode -format -nonInteractive",
    ]


def _worker_data_commands(config: ClusterConfig) -> list[str]:
    """Render worker-side HDFS data-directory setup commands."""
    data_root = f"/home/{config.cluster.ssh_username}/data"
    data_dir = f"{data_root}/hdfs/data"
    return [f"install -d -m 0755 {data_root} {data_root}/hdfs {data_dir}"]


def _worker_netdata_commands(
    config: ClusterConfig,
    master_ip: str,
    netdata_api_key: str,
) -> list[str]:
    """Render worker-side Netdata streaming config commands."""
    if not _netdata_enabled(config):
        return []
    return _here_doc(
        NETDATA_STREAM_PATH,
        "EOF_NETDATA_STREAM",
        "[stream]\n"
        "    enabled = yes\n"
        f"    destination = {master_ip}:19999\n"
        f"    api key = {netdata_api_key}\n",
    )


def _worker_install_directories(config: ClusterConfig) -> str:
    """Render worker install directories, omitting Netdata paths when disabled."""
    directories = [HADOOP_CONF_DIR, SPARK_CONF_DIR]
    if _netdata_enabled(config):
        directories.insert(0, NETDATA_DIR)
    return "install -d -m 0755 " + " ".join(directories)


def _master_install_directories(config: ClusterConfig) -> str:
    """Render master install directories.

    Include Netdata config paths only when monitoring is enabled.
    """
    directories = [
        "/etc/nginx/sites-enabled",
        "/etc/nginx/ssl",
        HADOOP_CONF_DIR,
        SPARK_CONF_DIR,
        "/etc/jupyter",
        "/etc/exports.d",
    ]
    if _netdata_enabled(config):
        directories.extend([NETDATA_DIR, NETDATA_GO_D_PATH, NETDATA_HEALTH_D_PATH])
    return "install -d -m 0755 " + " ".join(directories)


def generate_master_cloud_init(
    config: ClusterConfig,
    bundle: Bundle,
    worker_ips: list[str],
    netdata_api_key: str | None = None,
    *,
    attached_volume_id: str | None = None,
    allow_missing_runtime_secrets: bool = False,
) -> str:
    """Return cloud-init user-data bash script for the master."""
    web_password = _env_var_or_placeholder(
        "HAILSTACK_WEB_PASSWORD",
        "HAILSTACK_WEB_PASSWORD required",
        allow_missing=allow_missing_runtime_secrets,
        placeholder="destroy-only-web-password",
    )
    volume_password: str | None = None
    if _volume_enabled(config):
        volume_password = _env_var_or_placeholder(
            "HAILSTACK_VOLUME_PASSWORD",
            "HAILSTACK_VOLUME_PASSWORD required when a data volume is attached",
            allow_missing=allow_missing_runtime_secrets,
            placeholder="destroy-only-volume-password",
        )
        if attached_volume_id is None and allow_missing_runtime_secrets:
            attached_volume_id = "destroy-only-volume-id"
    resolved_netdata_api_key = _resolve_netdata_api_key(netdata_api_key)

    username = config.cluster.ssh_username
    commands = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"# Hailstack bundle {bundle.id}",
        _master_install_directories(config),
        f"install -d -m 0700 /home/{username}/.ssh",
        *_here_doc("/etc/hosts", "EOF_HOSTS", _hosts_content(config, worker_ips)),
        *_here_doc(
            f"/home/{username}/.ssh/authorized_keys",
            "EOF_AUTH_KEYS",
            _authorized_keys_content(config),
        ),
        f"chmod 600 /home/{username}/.ssh/authorized_keys",
        f"chown -R {quote(username)}:{quote(username)} /home/{username}/.ssh",
        *_here_doc(
            f"{HADOOP_CONF_DIR}/core-site.xml",
            "EOF_CORE_SITE",
            _core_site_content(config),
        ),
        *_here_doc(
            f"{HADOOP_CONF_DIR}/hdfs-site.xml",
            "EOF_HDFS_SITE",
            _hdfs_site_content(config),
        ),
        *_here_doc(
            f"{SPARK_CONF_DIR}/spark-defaults.conf",
            "EOF_SPARK_DEFAULTS",
            _spark_defaults_content(config, bundle),
        ),
        *_here_doc(
            "/etc/jupyter/jupyter_server_config.py",
            "EOF_JUPYTER_CONFIG",
            _jupyter_config_content(web_password),
        ),
        f"htpasswd -bc {HTPASSWD_PATH} hailstack {quote(web_password)}",
        "openssl req -x509 -nodes -days 3650 -newkey rsa:2048 "
        f"-keyout {SSL_KEY_PATH} -out {SSL_CERT_PATH} -subj '/CN=hailstack'",
        *_here_doc(NGINX_SITE_PATH, "EOF_NGINX_SITE", _nginx_config_content(config)),
        *_master_netdata_commands(config, resolved_netdata_api_key),
        *_volume_commands(config, volume_password, attached_volume_id),
        *_master_data_commands(config),
        *_lustre_commands(config),
        *_dns_commands(config),
        *_extras_commands(config),
        *_service_commands(config),
    ]
    return "\n".join(commands) + "\n"


def generate_worker_cloud_init(
    config: ClusterConfig,
    bundle: Bundle,
    master_ip: str,
    worker_index: int,
    worker_ips: Sequence[str] | None = None,
    netdata_api_key: str | None = None,
) -> str:
    """Return cloud-init user-data bash script for a worker."""
    resolved_netdata_api_key = _resolve_netdata_api_key(netdata_api_key)
    username = config.cluster.ssh_username
    commands = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"# Hailstack bundle {bundle.id}",
        _worker_install_directories(config),
        f"install -d -m 0700 /home/{username}/.ssh",
        *_here_doc(
            "/etc/hosts",
            "EOF_HOSTS",
            _worker_hosts_content(config, master_ip, worker_index, worker_ips),
        ),
        *_here_doc(
            f"/home/{username}/.ssh/authorized_keys",
            "EOF_AUTH_KEYS",
            _authorized_keys_content(config),
        ),
        f"chmod 600 /home/{username}/.ssh/authorized_keys",
        f"chown -R {quote(username)}:{quote(username)} /home/{username}/.ssh",
        *_here_doc(
            f"{HADOOP_CONF_DIR}/core-site.xml",
            "EOF_CORE_SITE",
            _core_site_content(config),
        ),
        *_here_doc(
            f"{HADOOP_CONF_DIR}/hdfs-site.xml",
            "EOF_HDFS_SITE",
            _worker_hdfs_site_content(config),
        ),
        *_here_doc(
            f"{SPARK_CONF_DIR}/spark-defaults.conf",
            "EOF_SPARK_DEFAULTS",
            _spark_defaults_content(config, bundle),
        ),
        *_worker_spark_commands(),
        *_worker_netdata_commands(config, master_ip, resolved_netdata_api_key),
        *_worker_volume_commands(config, master_ip),
        *_worker_data_commands(config),
        *_lustre_commands(config),
        *_dns_commands(config),
        *_extras_commands(config),
        *_worker_service_commands(config),
    ]
    return "\n".join(commands) + "\n"


__all__ = ["generate_master_cloud_init", "generate_worker_cloud_init"]
