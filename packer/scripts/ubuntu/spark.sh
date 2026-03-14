#!/usr/bin/env bash
set -euo pipefail

archive_path="/tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
install_dir="/opt/spark-${SPARK_VERSION}-bin-hadoop3"

curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o "$archive_path"
tar -xzf "$archive_path" -C /opt
ln -sfn "$install_dir" /opt/spark

cat >/etc/systemd/system/spark-master.service <<'EOF'
[Unit]
Description=Spark Master
After=network.target

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-master.sh
ExecStop=/opt/spark/sbin/stop-master.sh

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/spark-worker.service <<'EOF'
[Unit]
Description=Spark Worker
After=network.target

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-worker.sh spark://127.0.0.1:7077
ExecStop=/opt/spark/sbin/stop-worker.sh

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/spark-history-server.service <<'EOF'
[Unit]
Description=Spark History Server
After=network.target

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-history-server.sh
ExecStop=/opt/spark/sbin/stop-history-server.sh

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
test -f /etc/systemd/system/spark-master.service
test -f /etc/systemd/system/spark-worker.service
test -f /etc/systemd/system/spark-history-server.service
/opt/spark/bin/spark-shell --version 2>&1 | grep -F "$SPARK_VERSION"