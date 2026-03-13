# Phase 2: Packer Image Building

Ref: [spec.md](spec.md) sections E1, E2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `python-implementor` and `python-reviewer`
skills.

## Items

### Item 2.1: E1 - Build-image command

spec.md section: E1

Implement build_image_cmd() command in commands/build_image.py
and build_image() function in packer/builder.py. Load config,
resolve bundle (or use --bundle override), query Glance for base
image, run Packer build with version variables, upload built
image to Glance as hailstack-<bundle-id>. Requires Phase 1
(config + matrix). Log progress to stderr for each stage.
Covering all 16 acceptance tests from E1.

- [ ] implemented
- [ ] reviewed

### Item 2.2: E2 - Packer template structure

spec.md section: E2

Create packer/hailstack.pkr.hcl HCL2 template with variables
for all bundle versions plus base_image, ssh_username, flavor,
network, floating_ip_pool. Create provisioner scripts:
scripts/base.sh and scripts/ubuntu/{packages,hadoop,spark,hail,
jupyter,gnomad,uv,netdata}.sh. Each script includes version
checks. Covering all 4 acceptance tests from E2.

- [ ] implemented
- [ ] reviewed
