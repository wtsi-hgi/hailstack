# Phase 4: Cluster Lifecycle Commands

Ref: [spec.md](spec.md) sections D1, J1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `python-implementor` and `python-reviewer`
skills.

## Items

### Item 4.1: D1 - Create command with dry-run

spec.md section: D1

Implement create() command in commands/create.py. Wire to Pulumi
stack: load config, validate, resolve bundle, query Glance for
image, run Pulumi preview if --dry-run, else Pulumi up. Check
ceph_s3 credentials before Pulumi calls. Output master/worker
IPs on success. Handle ImageNotFoundError and S3Error. Covering
all 7 acceptance tests from D1.

- [ ] implemented
- [ ] reviewed

### Item 4.2: J1 - Destroy command with confirmation

spec.md section: J1

Implement destroy() command in commands/destroy.py. Show Pulumi
preview, prompt "Do you want to destroy cluster '<name>'? Type
the cluster name to confirm:". Accept only exact match. If
--dry-run, show plan only. On confirmed destroy, run Pulumi
destroy, release floating IP. Covering all 4 acceptance tests
from J1.

- [ ] implemented
- [ ] reviewed
