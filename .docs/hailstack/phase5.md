# Phase 5: Operations

Ref: [spec.md](spec.md) sections G1, G2, G3, H1, H2, I1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `python-implementor` and `python-reviewer`
skills.

## Items

### Item 5.1: G1 - Install command

spec.md section: G1

Implement install() command in commands/install.py. Support
--system, --python, --file, --smoke-test, --ssh-key options.
Load config, resolve Pulumi stack nodes, merge inline + file
package lists, validate environment, run Ansible playbook,
collect per-node results. Retry with exponential backoff
(1/2/4s, 3 attempts) on node failures. Record rollout manifest
to S3. Log progress to stderr for each stage. Covering all
14 acceptance tests from G1.

- [x] implemented
- [x] reviewed

### Item 5.2: G2 - Ansible runner for installs

spec.md section: G2

Implement run_install_playbook() in ansible/runner.py and
install.yml playbook. Dynamic inventory from Pulumi stack.
Tasks: apt-get for system packages, uv pip install to overlay
venv at /opt/hailstack/overlay-venv (with --system-site-packages
to inherit base venv). Base venv immutable, overlay receives all
user packages. Return list of NodeResult per host. Covering all
5 acceptance tests from G2.

- [x] implemented
- [x] reviewed

### Item 5.3: G3 - Rollout manifest storage

spec.md section: G3

Implement RolloutManifest and NodeResult Pydantic models, and
upload_rollout() function in storage/rollout.py. Upload manifest
JSON with SHA-256 hash to
hailstack/<cluster>/rollouts/<timestamp>/manifest.json and
per-node results to
hailstack/<cluster>/rollouts/<timestamp>/nodes/<hostname>.json
in Ceph S3. Include
cluster_name, timestamp (ISO 8601), package lists, success/fail
counts. Covering all 3 acceptance tests from G3.

- [x] implemented
- [x] reviewed

### Item 5.4: H1 - Status command

spec.md section: H1

Implement status() command in commands/status.py. Default: show
Pulumi stack outputs (cluster name, bundle, master IP, workers,
volume). With --detailed: SSH health probes (systemd service
statuses, CPU/MEM/DISK usage). With --json: machine-parseable
JSON output. Handle unreachable nodes gracefully. Covering all
7 acceptance tests from H1.

- [x] implemented
- [x] reviewed

### Item 5.5: H2 - SSH health probe module

spec.md section: H2

Implement check_service_health() and gather_resource_usage()
async functions in ssh/health.py. SSH to nodes, check systemd
services (master: spark-master, hdfs-namenode, yarn-rm,
spark-history-server, jupyter-lab, nginx, netdata,
mapred-history, nfs-server; worker: spark-worker, hdfs-datanode,
yarn-nm, netdata). Gather CPU%, MEM%, DISK% via top/df. Use
asyncio.gather() for parallel probes. Covering all 4 acceptance
tests from H2.

- [x] implemented
- [x] reviewed

### Item 5.6: I1 - Reboot command

spec.md section: I1

Implement reboot() command in commands/reboot.py. Default: reboot
all workers. --node: specific worker. Error on master references.
SSH to node(s), run sudo reboot, wait for SSH connectivity to
return (up to 5 min, with backoff retry). Covering all 6
acceptance tests from I1.

- [x] implemented
- [x] reviewed
