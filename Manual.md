# Manual Hailstack Cluster Bring-Up

It is feasible to bring up a small Hailstack-style cluster manually with
the OpenStack CLI. The important caveat is that `hailstack create` is not just a
thin wrapper around `openstack server create`: it orders the resources, records
state in Pulumi, checks quotas and missing resources, renders node-specific
cloud-init, and cleans up safely. If you do it by hand, you own those details.

This document explains the core of the cluster bring-up path and gives a
copy-pastable tutorial for a minimal cluster: one master, one worker, no data
volume, no Lustre network, and no Netdata. It assumes the Hailstack image already
exists in Glance as `hailstack-<bundle-id>`. Building that image is the job of
`hailstack build-image`, Packer, and the scripts in `packer/scripts/`.

## What `hailstack create` Does

At the OpenStack level, cluster creation is essentially:

1. Resolve the configured bundle and require an image named
   `hailstack-<bundle-id>`.
2. Create one OpenStack keypair from the first configured public SSH key.
3. Create a master security group and a worker security group.
4. Add public ingress rules for selected service ports.
5. Add all-TCP internal rules between the master and worker groups.
6. Create Neutron ports first, so the private IPs are known before boot.
7. Render master and worker cloud-init with those private IPs.
8. Boot one master server and `N` worker servers from the Hailstack image.
9. Optionally create or attach a Cinder volume to the master.
10. Allocate or associate a floating IP on the master port.

The cloud-init is the heart of the cluster. It writes `/etc/hosts`, SSH
authorized keys, Hadoop config, Spark config, Jupyter config, nginx proxy config,
and then starts the systemd services baked into the image.

## Minimal Manual Tutorial

Run these commands from a Linux shell that has the OpenStack CLI configured.
Either set `OS_CLOUD` or source your `openrc.sh` first.

### 1. Set Variables

Adjust the values before pasting.

```bash
set -euo pipefail

# If you use an openrc.sh file instead of OS_CLOUD, source it before this block.
# source ./openrc.sh

export CLUSTER="manual-hail"
export BUNDLE="hail-0.2.137-gnomad-3.0.4-r2"
export IMAGE="hailstack-${BUNDLE}"
export NETWORK="cloudforms_network"
export FLOATING_IP_POOL="public"
export MASTER_FLAVOR="m2.2xlarge"
export WORKER_FLAVOR="m2.2xlarge"
export SSH_USERNAME="ubuntu"
export SSH_PUBLIC_KEY_PATH="${HOME}/.ssh/id_rsa.pub"
export HAILSTACK_WEB_PASSWORD="choose-a-strong-password"

openstack image show "${IMAGE}" >/dev/null
openstack network show "${NETWORK}" >/dev/null
openstack flavor show "${MASTER_FLAVOR}" >/dev/null
openstack flavor show "${WORKER_FLAVOR}" >/dev/null
test -s "${SSH_PUBLIC_KEY_PATH}"
```

### 2. Create Security Groups, Ports, and Fixed IPs

Hailstack creates ports before servers. That lets it put the final private IPs
into cloud-init.

```bash
MASTER_SG_ID="$(
  openstack security group create \
    --description "Master security group for ${CLUSTER}" \
    "${CLUSTER}-master-sg" \
    -f value -c id
)"

WORKER_SG_ID="$(
  openstack security group create \
    --description "Worker security group for ${CLUSTER}" \
    "${CLUSTER}-worker-sg" \
    -f value -c id
)"

# Master public/service ports. Hailstack opens these by default on the master.
for port in 22 80 443 7077 8888 9820; do
  openstack security group rule create \
    --ingress --ethertype IPv4 --protocol tcp \
    --dst-port "${port}:${port}" \
    --remote-ip 0.0.0.0/0 \
    "${MASTER_SG_ID}"
done

# Worker service ports. Workers normally have no floating IP, so these are
# reachable only from networks that can already reach the worker port.
openstack security group rule create \
  --ingress --ethertype IPv4 --protocol tcp \
  --dst-port 9866:9866 \
  --remote-ip 0.0.0.0/0 \
  "${WORKER_SG_ID}"

openstack security group rule create \
  --ingress --ethertype IPv4 --protocol tcp \
  --dst-port 7078:7099 \
  --remote-ip 0.0.0.0/0 \
  "${WORKER_SG_ID}"

# Internal cluster traffic. This mirrors Hailstack's all_tcp_internal behavior.
openstack security group rule create \
  --ingress --ethertype IPv4 --protocol tcp \
  --dst-port 1:65535 \
  --remote-group "${WORKER_SG_ID}" \
  "${MASTER_SG_ID}"

openstack security group rule create \
  --ingress --ethertype IPv4 --protocol tcp \
  --dst-port 1:65535 \
  --remote-group "${MASTER_SG_ID}" \
  "${WORKER_SG_ID}"

openstack security group rule create \
  --ingress --ethertype IPv4 --protocol tcp \
  --dst-port 1:65535 \
  --remote-group "${WORKER_SG_ID}" \
  "${WORKER_SG_ID}"

MASTER_PORT_ID="$(
  openstack port create \
    --network "${NETWORK}" \
    --security-group "${MASTER_SG_ID}" \
    "${CLUSTER}-master-port" \
    -f value -c id
)"

WORKER_PORT_ID="$(
  openstack port create \
    --network "${NETWORK}" \
    --security-group "${WORKER_SG_ID}" \
    "${CLUSTER}-worker-port-01" \
    -f value -c id
)"

port_ip() {
  openstack port show "$1" -f json | python3 -c '
import ast
import json
import re
import sys

data = json.load(sys.stdin)
fixed_ips = data.get("fixed_ips") or data.get("Fixed IP Addresses")
if isinstance(fixed_ips, str):
    try:
        fixed_ips = ast.literal_eval(fixed_ips)
    except (SyntaxError, ValueError):
        match = re.search(r"ip_address=([^, ]+)", fixed_ips)
        if match is None:
            raise SystemExit(f"Could not parse fixed IPs: {fixed_ips!r}")
        print(match.group(1).strip(chr(34) + chr(39)))
        raise SystemExit(0)
if isinstance(fixed_ips, dict):
    print(fixed_ips["ip_address"])
else:
    print(fixed_ips[0]["ip_address"])
'
}

MASTER_PRIVATE_IP="$(port_ip "${MASTER_PORT_ID}")"
WORKER_PRIVATE_IP="$(port_ip "${WORKER_PORT_ID}")"

printf 'Master private IP: %s\n' "${MASTER_PRIVATE_IP}"
printf 'Worker private IP: %s\n' "${WORKER_PRIVATE_IP}"
```

### 3. Render Cloud-Init

This minimal cloud-init mirrors the essential Hailstack renderer. It configures
one master and one worker. The full app generates the same kind of file for each
worker, with all worker IPs listed in `/etc/hosts`.

```bash
SSH_PUBLIC_KEY="$(cat "${SSH_PUBLIC_KEY_PATH}")"
WEB_PASSWORD_B64="$(
  python3 -c 'import base64, os; print(base64.b64encode(os.environ["HAILSTACK_WEB_PASSWORD"].encode()).decode())'
)"

cat > master.cloud-init <<EOF
#!/usr/bin/env bash
set -euo pipefail

# Hailstack bundle ${BUNDLE}
WEB_PASSWORD="\$(python3 -c 'import base64; print(base64.b64decode("${WEB_PASSWORD_B64}").decode())')"

install -d -m 0755 \\
  /etc/nginx/sites-enabled \\
  /etc/nginx/ssl \\
  /etc/hadoop/conf \\
  /etc/spark/conf \\
  /etc/jupyter

install -d -m 0700 /home/${SSH_USERNAME}/.ssh

cat >/etc/hosts <<'EOF_HOSTS'
127.0.0.1 localhost
127.0.1.1 master ${CLUSTER}-master
${WORKER_PRIVATE_IP} worker-01 ${CLUSTER}-worker-01
EOF_HOSTS

cat >/home/${SSH_USERNAME}/.ssh/authorized_keys <<'EOF_AUTH_KEYS'
${SSH_PUBLIC_KEY}
EOF_AUTH_KEYS
chmod 600 /home/${SSH_USERNAME}/.ssh/authorized_keys
chown -R ${SSH_USERNAME}:${SSH_USERNAME} /home/${SSH_USERNAME}/.ssh

cat >/etc/hadoop/conf/core-site.xml <<'EOF_CORE_SITE'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9820</value>
  </property>
</configuration>
EOF_CORE_SITE

cat >/etc/hadoop/conf/hdfs-site.xml <<'EOF_HDFS_SITE'
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/home/${SSH_USERNAME}/data/hdfs/name</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF_HDFS_SITE

cat >/etc/spark/conf/spark-defaults.conf <<'EOF_SPARK_DEFAULTS'
spark.master spark://master:7077
spark.history.fs.logDirectory file:///home/${SSH_USERNAME}/data/spark-history
spark.pyspark.python /opt/hailstack/overlay-venv/bin/python
spark.hadoop.hailstack.bundle ${BUNDLE}
EOF_SPARK_DEFAULTS

WEB_PASSWORD="\${WEB_PASSWORD}" python3 >/etc/jupyter/jupyter_server_config.py <<'PY'
import os

password = os.environ["WEB_PASSWORD"]
print("from jupyter_server.auth import passwd")
print("c.ServerApp.base_url = '/jupyter/'")
print("c.ServerApp.password_required = True")
print(f"c.ServerApp.password = passwd({password!r})")
PY

htpasswd -bc /etc/nginx/.hailstack-htpasswd hailstack "\${WEB_PASSWORD}"
openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \\
  -keyout /etc/nginx/ssl/hailstack.key \\
  -out /etc/nginx/ssl/hailstack.crt \\
  -subj '/CN=hailstack'

cat >/etc/nginx/sites-enabled/hailstack.conf <<'EOF_NGINX'
server {
  listen 80;
  listen 443 ssl;
  server_name _;
  ssl_certificate /etc/nginx/ssl/hailstack.crt;
  ssl_certificate_key /etc/nginx/ssl/hailstack.key;
  auth_basic 'Hailstack';
  auth_basic_user_file /etc/nginx/.hailstack-htpasswd;

  location /jupyter/ {
    proxy_pass http://127.0.0.1:8888/jupyter/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade \$http_upgrade;
    proxy_set_header Connection 'upgrade';
    proxy_set_header Host \$host;
  }
  location /spark/ { proxy_pass http://127.0.0.1:8080/; }
  location /sparkhist/ { proxy_pass http://127.0.0.1:18080/; }
  location /yarn/ { proxy_pass http://127.0.0.1:8088/; }
  location /mapreduce/ { proxy_pass http://127.0.0.1:19888/; }
  location /hdfs/ { proxy_pass http://127.0.0.1:9870/; }
  location /nm01/ { proxy_pass http://worker-01:8042/; }
}
EOF_NGINX

install -d -m 0755 \\
  /home/${SSH_USERNAME}/data \\
  /home/${SSH_USERNAME}/data/hdfs \\
  /home/${SSH_USERNAME}/data/hdfs/name \\
  /home/${SSH_USERNAME}/data/spark-history

[ -f /home/${SSH_USERNAME}/data/hdfs/name/current/VERSION ] || \\
  /opt/hadoop/bin/hdfs namenode -format -nonInteractive

for service in hdfs-namenode yarn-rm mapred-history spark-master spark-history-server jupyter-lab nginx; do
  systemctl enable "\${service}"
  systemctl restart "\${service}" || systemctl start "\${service}"
done
EOF

cat > worker-01.cloud-init <<EOF
#!/usr/bin/env bash
set -euo pipefail

# Hailstack bundle ${BUNDLE}
install -d -m 0755 /etc/hadoop/conf /etc/spark/conf
install -d -m 0700 /home/${SSH_USERNAME}/.ssh

cat >/etc/hosts <<'EOF_HOSTS'
127.0.0.1 localhost
${MASTER_PRIVATE_IP} master ${CLUSTER}-master
${WORKER_PRIVATE_IP} worker-01 ${CLUSTER}-worker-01
127.0.1.1 worker-01 ${CLUSTER}-worker-01
EOF_HOSTS

cat >/home/${SSH_USERNAME}/.ssh/authorized_keys <<'EOF_AUTH_KEYS'
${SSH_PUBLIC_KEY}
EOF_AUTH_KEYS
chmod 600 /home/${SSH_USERNAME}/.ssh/authorized_keys
chown -R ${SSH_USERNAME}:${SSH_USERNAME} /home/${SSH_USERNAME}/.ssh

cat >/etc/hadoop/conf/core-site.xml <<'EOF_CORE_SITE'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9820</value>
  </property>
</configuration>
EOF_CORE_SITE

cat >/etc/hadoop/conf/hdfs-site.xml <<'EOF_HDFS_SITE'
<configuration>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/home/${SSH_USERNAME}/data/hdfs/data</value>
  </property>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>true</value>
  </property>
</configuration>
EOF_HDFS_SITE

cat >/etc/spark/conf/spark-defaults.conf <<'EOF_SPARK_DEFAULTS'
spark.master spark://master:7077
spark.history.fs.logDirectory file:///home/${SSH_USERNAME}/data/spark-history
spark.pyspark.python /opt/hailstack/overlay-venv/bin/python
spark.hadoop.hailstack.bundle ${BUNDLE}
EOF_SPARK_DEFAULTS

install -d -m 0755 /etc/systemd/system/spark-worker.service.d
cat >/etc/systemd/system/spark-worker.service.d/hailstack.conf <<'EOF_SPARK_WORKER'
[Service]
ExecStart=
ExecStart=/opt/spark/sbin/start-worker.sh spark://master:7077
EOF_SPARK_WORKER
systemctl daemon-reload

install -d -m 0755 \\
  /home/${SSH_USERNAME}/data \\
  /home/${SSH_USERNAME}/data/hdfs \\
  /home/${SSH_USERNAME}/data/hdfs/data

for service in hdfs-datanode yarn-nm spark-worker; do
  systemctl enable "\${service}"
  systemctl restart "\${service}" || systemctl start "\${service}"
done
EOF

chmod 600 master.cloud-init worker-01.cloud-init
```

### 4. Boot the Servers and Attach a Floating IP

```bash
openstack keypair create \
  --public-key "${SSH_PUBLIC_KEY_PATH}" \
  "${CLUSTER}-keypair"

openstack server create \
  --wait \
  --image "${IMAGE}" \
  --flavor "${MASTER_FLAVOR}" \
  --key-name "${CLUSTER}-keypair" \
  --nic "port-id=${MASTER_PORT_ID}" \
  --user-data master.cloud-init \
  --property "cluster_name=${CLUSTER}" \
  --property "bundle_id=${BUNDLE}" \
  --property "role=master" \
  "${CLUSTER}-master"

openstack server create \
  --wait \
  --image "${IMAGE}" \
  --flavor "${WORKER_FLAVOR}" \
  --key-name "${CLUSTER}-keypair" \
  --nic "port-id=${WORKER_PORT_ID}" \
  --user-data worker-01.cloud-init \
  --property "cluster_name=${CLUSTER}" \
  --property "bundle_id=${BUNDLE}" \
  --property "role=worker" \
  "${CLUSTER}-worker-01"

FLOATING_IP="$(
  openstack floating ip create "${FLOATING_IP_POOL}" \
    -f json |
    python3 -c 'import json, sys; data=json.load(sys.stdin); print(data.get("floating_ip_address") or data.get("Floating IP Address"))'
)"

openstack floating ip set --port "${MASTER_PORT_ID}" "${FLOATING_IP}"

printf 'Master: https://%s/jupyter/\n' "${FLOATING_IP}"
printf 'Basic auth username: hailstack\n'
printf 'Basic auth password: %s\n' "${HAILSTACK_WEB_PASSWORD}"
```

### 5. Check Boot Progress

Cloud-init and the service starts may take several minutes after the servers
become `ACTIVE`.

```bash
ssh "${SSH_USERNAME}@${FLOATING_IP}" 'sudo cloud-init status --wait'
ssh "${SSH_USERNAME}@${FLOATING_IP}" 'systemctl --no-pager --failed'
ssh "${SSH_USERNAME}@${FLOATING_IP}" 'systemctl --no-pager status hdfs-namenode spark-master jupyter-lab nginx'
ssh "${SSH_USERNAME}@${FLOATING_IP}" '/opt/hadoop/bin/hdfs dfsadmin -report'
```

Then open:

```text
https://<floating-ip>/jupyter/
```

Use basic-auth username `hailstack` and the password from
`HAILSTACK_WEB_PASSWORD`.

## Manual Cleanup

This deletes the tutorial resources. Check the names before running if you used
different variables.

```bash
set -euo pipefail

openstack server delete --wait "${CLUSTER}-worker-01" || true
openstack server delete --wait "${CLUSTER}-master" || true

if [ -n "${FLOATING_IP:-}" ]; then
  openstack floating ip delete "${FLOATING_IP}" || true
fi

openstack port delete "${WORKER_PORT_ID}" || true
openstack port delete "${MASTER_PORT_ID}" || true
openstack security group delete "${CLUSTER}-worker-sg" || true
openstack security group delete "${CLUSTER}-master-sg" || true
openstack keypair delete "${CLUSTER}-keypair" || true
```

## Matching the Full App

To scale this tutorial to match `hailstack create` more closely:

- Create one worker port per worker before rendering cloud-init.
- Put every worker private IP into the master and worker `/etc/hosts` files.
- Name workers as `${CLUSTER}-worker-01`, `${CLUSTER}-worker-02`, and so on.
- Add `/nm02/`, `/nm03/`, and so on to the nginx config for NodeManager UIs.
- Add port `19999` and Netdata stream config if `monitoring = "netdata"`.
- Add S3A properties to `core-site.xml` if runtime S3 credentials are needed.
- Add a second Neutron port per node on the Lustre network if configured, then
  add the `/lustre` mount entry to cloud-init.
- For a data volume, create or choose a Cinder volume, attach it to the master,
  pass the volume ID into master cloud-init, set up LUKS and ext4 on the master,
  export `/home/<user>/data` over NFS, and mount that NFS export on workers.
- Keep your own state file with server IDs, port IDs, volume IDs, and floating
  IPs. Pulumi normally does this for Hailstack.

The safest manual starting point is to leave data volumes and Lustre out until
the basic master-worker path is healthy.
