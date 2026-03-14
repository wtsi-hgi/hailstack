#!/usr/bin/env bash
set -euo pipefail

archive_path="/tmp/hadoop-${HADOOP_VERSION}.tar.gz"
install_dir="/opt/hadoop-${HADOOP_VERSION}"

curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o "$archive_path"
tar -xzf "$archive_path" -C /opt
ln -sfn "$install_dir" /opt/hadoop

cat >/etc/systemd/system/hadoop-namenode.service <<'EOF'
[Unit]
Description=Hadoop NameNode
After=network.target

[Service]
Type=simple
ExecStart=/opt/hadoop/bin/hdfs --daemon start namenode
ExecStop=/opt/hadoop/bin/hdfs --daemon stop namenode
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/hadoop-datanode.service <<'EOF'
[Unit]
Description=Hadoop DataNode
After=network.target

[Service]
Type=simple
ExecStart=/opt/hadoop/bin/hdfs --daemon start datanode
ExecStop=/opt/hadoop/bin/hdfs --daemon stop datanode
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
test -f /etc/systemd/system/hadoop-namenode.service
test -f /etc/systemd/system/hadoop-datanode.service
/opt/hadoop/bin/hadoop version | grep -F "$HADOOP_VERSION"