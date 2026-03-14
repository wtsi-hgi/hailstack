#!/usr/bin/env bash
set -euo pipefail

archive_path="/tmp/hadoop-${HADOOP_VERSION}.tar.gz"
install_dir="/opt/hadoop-${HADOOP_VERSION}"

curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o "$archive_path"
tar -xzf "$archive_path" -C /opt
ln -sfn "$install_dir" /opt/hadoop

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