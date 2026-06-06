#!/usr/bin/env bash
set -euo pipefail

archive_path="/tmp/hadoop-${HADOOP_VERSION}.tar.gz"
install_dir="/opt/hadoop-${HADOOP_VERSION}"

hailstack_resolve_java_home() {
	local candidate="${JAVA_HOME:-}"
	local java_bin
	local resolved_java_bin

	if [[ -n "${candidate}" && -x "${candidate}/bin/java" ]]; then
		printf '%s\n' "${candidate}"
		return 0
	fi

	if java_bin="$(command -v java 2>/dev/null)"; then
		:
	elif java_bin="$(command -v javac 2>/dev/null)"; then
		:
	else
		printf '[hailstack] unable to find java or javac on PATH while configuring Hadoop\n' >&2
		return 1
	fi

	if ! resolved_java_bin="$(readlink -f "${java_bin}")"; then
		printf '[hailstack] unable to resolve Java binary path: %s\n' "${java_bin}" >&2
		return 1
	fi

	candidate="$(dirname "$(dirname "${resolved_java_bin}")")"
	if [[ ! -x "${candidate}/bin/java" ]]; then
		printf '[hailstack] discovered JAVA_HOME does not contain bin/java: %s\n' "${candidate}" >&2
		return 1
	fi

	printf '%s\n' "${candidate}"
}

hailstack_write_hadoop_java_home() {
	local hadoop_env="$1"

	if [[ ! -f "${hadoop_env}" ]]; then
		printf '[hailstack] Hadoop env file not found: %s\n' "${hadoop_env}" >&2
		return 1
	fi

	printf '\nexport JAVA_HOME=%q\n' "${JAVA_HOME}" >>"${hadoop_env}"
}

curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o "$archive_path"
tar -xzf "$archive_path" -C /opt
ln -sfn "$install_dir" /opt/hadoop

JAVA_HOME="$(hailstack_resolve_java_home)"
export JAVA_HOME
hailstack_write_hadoop_java_home "${install_dir}/etc/hadoop/hadoop-env.sh"

cat >/etc/systemd/system/hdfs-namenode.service <<'EOF'
[Unit]
Description=HDFS NameNode
After=network.target

[Service]
Type=simple
ExecStart=/opt/hadoop/bin/hdfs --daemon start namenode
ExecStop=/opt/hadoop/bin/hdfs --daemon stop namenode
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/hdfs-datanode.service <<'EOF'
[Unit]
Description=HDFS DataNode
After=network.target

[Service]
Type=simple
ExecStart=/opt/hadoop/bin/hdfs --daemon start datanode
ExecStop=/opt/hadoop/bin/hdfs --daemon stop datanode
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/yarn-rm.service <<'EOF'
[Unit]
Description=YARN ResourceManager
After=network.target

[Service]
Type=simple
ExecStart=/opt/hadoop/bin/yarn --daemon start resourcemanager
ExecStop=/opt/hadoop/bin/yarn --daemon stop resourcemanager
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/yarn-nm.service <<'EOF'
[Unit]
Description=YARN NodeManager
After=network.target

[Service]
Type=simple
ExecStart=/opt/hadoop/bin/yarn --daemon start nodemanager
ExecStop=/opt/hadoop/bin/yarn --daemon stop nodemanager
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/mapred-history.service <<'EOF'
[Unit]
Description=MapReduce History Server
After=network.target

[Service]
Type=simple
ExecStart=/opt/hadoop/bin/mapred --daemon start historyserver
ExecStop=/opt/hadoop/bin/mapred --daemon stop historyserver
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
test -f /etc/systemd/system/hdfs-namenode.service
test -f /etc/systemd/system/hdfs-datanode.service
test -f /etc/systemd/system/yarn-rm.service
test -f /etc/systemd/system/yarn-nm.service
test -f /etc/systemd/system/mapred-history.service
/opt/hadoop/bin/hadoop version | grep -F "$HADOOP_VERSION"
