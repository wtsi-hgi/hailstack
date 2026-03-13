# Phase 6: Packaging, CI & Integration Tests

Ref: [spec.md](spec.md) sections M1, M2, M3, O1, O2, O3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `python-implementor` and `python-reviewer`
skills.

For parallel batch items, use separate subagents per item.
Launch review subagents using the `python-reviewer` skill
(review all items in the batch together in a single review
pass).

## Items

### Batch 1 (parallel)

#### Item 6.1: M1 - Apptainer definition [parallel with 6.2, 6.3]

spec.md section: M1

Create Apptainer.def in repo root. Bootstrap from
python:3.14-slim. Install curl, unzip, OpenSSH, uuid-runtime,
gnupg, Pulumi, Packer 1.11.2, Ansible, hailstack package.
Environment: add Pulumi to PATH. Runscript: exec hailstack "$@".
Final SIF < 500MB. Covering all 4 acceptance tests from M1.

- [ ] implemented
- [ ] reviewed

#### Item 6.2: M2 - GitHub Actions CI workflow [parallel with 6.1, 6.3]

spec.md section: M2

Create .github/workflows/ci.yml. Three jobs: (1) lint: ruff check
+ ruff format --check, (2) typecheck: pyright --strict, (3) test:
pytest with coverage. Trigger on push to main and all PRs.
Covering all 4 acceptance tests from M2.

- [ ] implemented
- [ ] reviewed

#### Item 6.3: M3 - GitHub Actions release workflow [parallel with 6.1, 6.2]

spec.md section: M3

Create .github/workflows/release.yml. Trigger on v* tag push.
Build Apptainer SIF. Attach as GitHub release asset.
Auto-generate release notes from commits. Covering all 3
acceptance tests from M3.

- [ ] implemented
- [ ] reviewed

### Batch 2 (parallel, after batch 1 is reviewed)

#### Item 6.4: O1 - Pulumi integration tests [parallel with 6.5, 6.6]

spec.md section: O1

Create tests/integration/test_pulumi_lifecycle.py. Use Pulumi
automation API with mocked OpenStack provider. Test
create/destroy lifecycle: validate preview succeeds, exports
present (master_public_ip, worker_private_ips, cluster_name,
bundle_id), destroy succeeds. Covering all 3 acceptance tests
from O1.

- [ ] implemented
- [ ] reviewed

#### Item 6.5: O2 - Image verification smoke tests [parallel with 6.4, 6.6]

spec.md section: O2

Create tests/integration/test_packer_scripts.py. Verify each
provisioner script in packer/scripts/ubuntu/ ends with
version-check command. Mock environment with version vars.
Test scripts/base.sh exits 0. Covering all 2 acceptance
tests from O2.

- [ ] implemented
- [ ] reviewed

#### Item 6.6: O3 - End-to-end workflow test skeleton [parallel with 6.4, 6.5]

spec.md section: O3

Create tests/integration/test_e2e_skeleton.py. Define pytest
fixtures for OpenStack credentials, temp config, cleanup.
Mark tests with @pytest.mark.integration (skip by default).
Create test stubs for full lifecycle: build-image, create,
install, status (default + detailed), reboot, destroy.
Covering all 3 acceptance tests from O3.

- [ ] implemented
- [ ] reviewed
