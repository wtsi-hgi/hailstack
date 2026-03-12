# Phase 1: Foundation

Ref: [spec.md](spec.md) sections A1, A2, A3, B1, C1, C2, K1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `python-implementor` and `python-reviewer`
skills.

## Items

### Item 1.1: A1 - CLI structure and help

spec.md section: A1

Implement Typer CLI entry point with 7 commands (create, destroy,
reboot, build-image, install, status, convert-auth). Top-level
Typer app with --version flag. Covering all 4 acceptance tests
from A1.

- [ ] implemented
- [ ] reviewed

### Item 1.2: A2 - Config file loading and validation

spec.md section: A2

Implement `load_config()` function in config/parser.py. Load TOML
via tomllib, parse into Pydantic ClusterConfig model. Validate
cluster name regex and handle syntax errors. Covering all 6
acceptance tests from A2.

- [ ] implemented
- [ ] reviewed

### Item 1.3: A3 - Environment variable and .env support

spec.md section: A3

Implement `_substitute_env_vars()` recursive function in
config/parser.py. Support $VAR and ${VAR} syntax in string values.
Integrate python-dotenv for .env file loading. Covering all 4
acceptance tests from A3.

- [ ] implemented
- [ ] reviewed

### Item 1.4: B1 - Cluster config TOML schema

spec.md section: B1

Implement Pydantic models in config/schema.py: ClusterConfig,
SecurityGroupConfig, SecurityGroups, VolumeConfig, S3Config,
CephS3Config, SSHKeysConfig, DNSConfig, ExtrasConfig,
PackerConfig. Include field validators for cluster name, SSH
keys, volume config, monitoring field, num_workers, floating_ip,
ceph_s3 requirements. Create example-config.toml in repo root
with Sanger defaults. Covering all 14 acceptance tests from B1.

- [ ] implemented
- [ ] reviewed

### Item 1.5: C1 - Matrix structure and bundle query

spec.md section: C1

Implement CompatibilityMatrix class in config/compatibility.py.
Parse bundles.toml (repo root) with flat [bundle."<id>"] sections.
Bundle IDs follow the pattern hail-<hail_ver>-gnomad-<gnomad_ver>-r<revision>
(e.g. "hail-0.2.137-gnomad-3.0.4-r2"). Each bundle has explicit
hail, spark, hadoop, java, python, scala, gnomad, and status fields.
Implement get_bundle(), get_default(), list_bundles() methods
returning Bundle Pydantic models. Covering all 8 acceptance tests
from C1.

- [ ] implemented
- [ ] reviewed

### Item 1.6: C2 - Bundle validation at CLI time

spec.md section: C2

Implement `validate_bundle()` function in config/validator.py.
Validate config.bundle exists in CompatibilityMatrix. Use matrix
default if bundle empty. Only validate for create/build-image
commands; allow any bundle for other commands. Covering all 4
acceptance tests from C2.

- [ ] implemented
- [ ] reviewed

### Item 1.7: K1 - convert-auth command

spec.md section: K1

Implement convert-auth command in commands/convert_auth.py. Read
OS_AUTH_URL, OS_PROJECT_NAME, OS_USERNAME and optional fields
from env. Generate clouds.yaml YAML output. Support --write flag
to write to ~/.config/openstack/clouds.yaml with backup of
existing file. Covering all 6 acceptance tests from K1.

- [ ] implemented
- [ ] reviewed
