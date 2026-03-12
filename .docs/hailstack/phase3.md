# Phase 3: Pulumi Infrastructure

Ref: [spec.md](spec.md) sections D2, D3, D4, D5, F1, L1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `python-implementor` and `python-reviewer`
skills.

## Items

### Item 3.1: D2 - Pulumi stack and OpenStack resources

spec.md section: D2

Implement create_cluster_resources() in pulumi/resources.py.
Create keypair, security groups (master + worker with
configurable rules), network ports, master/worker instances,
floating IP management, optional volume attachment, all tagged
with cluster name. Generate Pulumi exports: master_public_ip,
master_private_ip, worker_private_ips, worker_names,
cluster_name, bundle_id. Covering all 10 acceptance tests
from D2.

- [ ] implemented
- [ ] reviewed

### Item 3.2: D3 - Cloud-init provisioning (master)

spec.md section: D3

Implement generate_master_cloud_init() in pulumi/cloud_init.py.
Generate bash script that configures /etc/hosts, enables systemd
services (hdfs-namenode, yarn-rm, mapred-history, spark-master,
spark-history-server, jupyter-lab, nginx, nfs-server if volume,
netdata if monitoring), creates nginx reverse proxy with /jupyter,
/spark, /sparkhist, /yarn, /mapreduce, /hdfs, /nm<NN>,
/netdata paths, sets up htpasswd and SSL, configures LUKS if
volume attached, creates NFS export. Covering all 16 acceptance
tests from D3.

- [ ] implemented
- [ ] reviewed

### Item 3.3: D4 - Cloud-init provisioning (workers)

spec.md section: D4

Implement generate_worker_cloud_init() in pulumi/cloud_init.py.
Generate bash script that configures /etc/hosts, enables systemd
services (hdfs-datanode, yarn-nm, spark-worker, netdata if
monitoring), waits for master NFS port, mounts shared data,
configures netdata streaming to master if enabled. Covering
all 8 acceptance tests from D4.

- [ ] implemented
- [ ] reviewed

### Item 3.4: D5 - Floating IP management

spec.md section: D5

Implement floating IP allocation and release in
pulumi/resources.py. If floating_ip empty, allocate new FIP
and associate to master. If floating_ip provided, use that
address. On destroy, release FIP to pool. Covering all 3
acceptance tests from D5.

- [ ] implemented
- [ ] reviewed

### Item 3.5: F1 - Volume lifecycle via Pulumi

spec.md section: F1

Implement volume creation/attachment in pulumi/resources.py.
Three modes: create new (volumes.create=true), attach existing
(existing_volume_id), or none. On destroy, delete or preserve
based on preserve_on_destroy flag. Lustre network support with
separate ports. LUKS encryption handled in cloud-init layer.
Covering all 8 acceptance tests from F1.

- [ ] implemented
- [ ] reviewed

### Item 3.6: L1 - Netdata configuration

spec.md section: L1

Enhance cloud-init generation in pulumi/cloud_init.py to include
Netdata configuration when monitoring="netdata". Master enables
stream accept with random UUID4 API key. Workers configured as
stream clients pointing to master. Master Netdata config includes
HDFS JMX endpoint monitoring and health alarms for HDFS capacity.
Nginx proxies /netdata. Covering all 6 acceptance tests from L1.

- [ ] implemented
- [ ] reviewed
