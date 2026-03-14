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
stack: load config, validate, resolve bundle, run pre-flight
resource validation (query OpenStack for image, flavours, network,
compute/volume quotas, floating IP availability, existing volume
existence; collect all failures into single error), run Pulumi
preview if --dry-run, else Pulumi up. Check ceph_s3 credentials
before Pulumi calls. On success, print master floating IP
prominently as final stdout line. On Pulumi up failure, run
automatic cleanup (Pulumi destroy) so no orphaned resources
remain. Log progress to stderr for each stage. Handle
ImageNotFoundError, ResourceNotFoundError, QuotaExceededError,
and S3Error. Covering all 17 acceptance tests from D1.

- [x] implemented
- [x] reviewed

### Item 4.2: J1 - Destroy command with confirmation

spec.md section: J1

Implement destroy() command in commands/destroy.py. Show Pulumi
preview, prompt "Do you want to destroy cluster '<name>'? Type
the cluster name to confirm:". Accept only exact match. If
--dry-run, show plan only. On confirmed destroy, run Pulumi
destroy, release floating IP. Log progress to stderr for each
stage. On success, final stdout line is
"Cluster '<name>' destroyed.". Covering all 6 acceptance
tests from J1.

- [x] implemented
- [x] reviewed
