# Hailstack Specification

## Overview

Hailstack is a CLI tool for provisioning and managing Spark/Hadoop
clusters on OpenStack with integrated Hail genomic analysis, JupyterLab,
and optional Netdata monitoring. Built as a single Python codebase
(Typer CLI, Pulumi IaC, Packer images, Ansible post-config) with version
compatibility matrices, fat pre-baked VM images, and Ceph S3-backed
shared team state.

Key design goals: version compatibility matrices prevent
Spark/Hadoop/Hail breakage by validating bundles at CLI time. Fat Packer
images eliminate fragile post-boot provisioning. Pulumi provides
single-tool infrastructure management. S3-backed state enables
team-shared cluster management. Structured for AI agent
comprehension and modification.

CLI commands: `create`, `destroy`, `reboot`, `build-image`, `install`,
`status`, `convert-auth`. Single TOML config per cluster. Distributed
via Apptainer SIF (no root required). Deployed from HPC environments.

## Architecture

**Tech stack:** Python 3.14 + Typer (CLI), Pulumi Python + OpenStack
provider (IaC), Packer HCL2 (image building), cloud-init (boot
provisioning), Ansible (post-creation installs), Ceph S3 (Pulumi state
+ rollout manifests). Note: hailstack CLI itself runs on Python 3.14
(per project conventions). Cluster VMs use the Python version specified
by the selected bundle (e.g. 3.12 in bundles.toml).

**Pulumi backend:** Configured via `pulumi.automation` API with
`backend_url=f"s3://{ceph_s3.bucket}?endpoint={ceph_s3.endpoint}"`.
Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
set from `ceph_s3.access_key` and `ceph_s3.secret_key` before Pulumi
automation calls. No local state files.

**OpenStack authentication:** All commands that access OpenStack
(create, destroy, build-image, status) require OpenStack credentials
via `clouds.yaml` or `OS_*` environment variables. The `convert-auth`
command helps generate `clouds.yaml` from legacy `openrc.sh`.

**Project layout:**

```text
pyproject.toml
bundles.toml
example-config.toml
src/hailstack/
  __init__.py
  py.typed
  cli/
    __init__.py
    main.py
    commands/
      __init__.py
      create.py
      destroy.py
      reboot.py
      build_image.py
      install.py
      status.py
      convert_auth.py
  config/
    __init__.py
    parser.py
    schema.py
    validator.py
    compatibility.py
  pulumi/
    __init__.py
    stack.py
    resources.py
    cloud_init.py
  packer/
    __init__.py
    builder.py
  ansible/
    __init__.py
    runner.py
  storage/
    __init__.py
    s3.py
    rollout.py
  ssh/
    __init__.py
    client.py
    health.py
  errors.py
  version.py
tests/
  conftest.py
  cli/
    test_main.py
    commands/
      test_create.py
      test_destroy.py
      test_reboot.py
      test_build_image.py
      test_install.py
      test_status.py
      test_convert_auth.py
  config/
    test_parser.py
    test_schema.py
    test_validator.py
    test_compatibility.py
  pulumi/
    test_stack.py
    test_resources.py
    test_cloud_init.py
  packer/
    test_builder.py
  ansible/
    test_runner.py
  storage/
    test_s3.py
    test_rollout.py
  ssh/
    test_client.py
    test_health.py
packer/
  hailstack.pkr.hcl
  scripts/
    base.sh
    ubuntu/
      packages.sh
      hadoop.sh
      spark.sh
      hail.sh
      jupyter.sh
      netdata.sh
      gnomad.sh
      uv.sh
ansible/
  install.yml
  roles/
    packages/
      tasks/
        main.yml
.github/
  workflows/
    ci.yml
    release.yml
Apptainer.def
```

**Data flow:** User TOML config -> parse + validate + query compatibility
matrix -> Pulumi queries OpenStack Glance for Packer image by bundle
name -> Pulumi creates stack (instances, security groups, volumes,
floating IP) with cloud-init user data -> cloud-init enables services
and applies cluster-specific config -> cluster operational.

**State:** Pulumi state in Ceph S3 only (no local state files). Rollout
manifests from `install` command also in Ceph S3.

**Error hierarchy:**

```python
class HailstackError(Exception): ...
class ConfigError(HailstackError): ...
class ValidationError(HailstackError): ...
class BundleNotFoundError(ValidationError): ...
class NetworkError(HailstackError): ...
class PulumiError(HailstackError): ...
class AnsibleError(HailstackError): ...
class S3Error(HailstackError): ...
class PackerError(HailstackError): ...
class SSHError(HailstackError): ...
class ImageNotFoundError(HailstackError): ...
class ResourceNotFoundError(HailstackError): ...
class QuotaExceededError(HailstackError): ...
```

**Error policy:** Validation errors raise immediately. Network/SSH errors
retry with exponential backoff (1/2/4s, 3 attempts). All errors logged
to stderr via `logging` module. No bare `except:`.

**Progress logging:** All commands log progress to stderr via the
`logging` module at INFO level. Each major stage (config validation,
resource checks, infrastructure creation, service startup) is logged
as it begins and completes. On successful cluster creation, the
master floating IP is printed prominently to stdout as the final
output line.

## A: CLI & Entry Point

### A1: CLI structure and help

As a user, I want `hailstack --help` to list all commands, and each
command's `--help` to show all options with descriptions.

Entry point: `hailstack` console script via pyproject.toml. Top-level
Typer app with 7 commands. `--version` flag shows semver.

**Package:** `hailstack/cli/`
**File:** `cli/main.py`
**Test file:** `tests/cli/test_main.py`

```python
import typer
from typing import Annotated

app = typer.Typer(
    name="hailstack",
    help="Provision and manage Spark/Hadoop/Hail clusters on OpenStack.",
)

@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option("--version", help="Show version and exit."),
    ] = None,
) -> None:
    """Hailstack CLI."""
    if version:
        from hailstack.version import __version__
        typer.echo(f"hailstack {__version__}")
        raise typer.Exit()
```

**Acceptance tests:**

1. When `hailstack --help` invoked, then output contains "Usage:" and
   lists create, destroy, reboot, build-image, install, status,
   convert-auth.
2. When `hailstack --version` invoked, then prints
   "hailstack <semver>" and exits 0.
3. When `hailstack nonexistent` invoked, then exit code 2 and stderr
   contains "No such command".
4. When `hailstack create --help` invoked, then shows --config,
   --dry-run, --dotenv options with descriptions.

### A2: Config file loading and validation

As a user, I want hailstack to load my TOML config, validate all fields
before touching infrastructure, and report clear errors for syntax
issues, missing required fields, or invalid values.

Load TOML via `tomllib`. Parse into Pydantic `ClusterConfig` model.
Validate cluster name: `^[a-z][a-z0-9-]{1,62}$` (lowercase
alphanumeric + hyphens, 2-63 chars, no underscores, starts with
letter). Validate bundle exists in matrix (for create/build-image
only). Apply defaults for optional fields.

**Package:** `hailstack/config/`
**File:** `config/parser.py`
**Test file:** `tests/config/test_parser.py`

```python
def load_config(
    path: Path,
    dotenv_file: Path | None = None,
) -> ClusterConfig:
    """Load TOML config, apply env var substitution, validate."""
    ...
```

**Acceptance tests:**

1. Given valid TOML with all required fields, when `load_config` called,
   then returns `ClusterConfig` with values matching file content.
2. Given TOML missing `cluster.name`, then raises `ConfigError` with
   "cluster.name" in message.
3. Given `cluster.name = "test_cluster"` (underscore), then raises
   `ValidationError` with "Invalid cluster name".
4. Given `cluster.name = "A"` (1 char), then raises `ValidationError`.
5. Given `cluster.name = "9starts-with-digit"`, then raises
   `ValidationError`.
6. Given TOML syntax error, then raises `ConfigError` with line number.

### A3: Environment variable and .env support

As a user, I want secrets in env vars or .env files, not in
Git-tracked TOML.

String values in TOML support `$VAR` and `${VAR}` syntax. If
`--dotenv` provided, load via `python-dotenv` before parsing.
Undefined vars resolve to empty string (logged as warning).

**Package:** `hailstack/config/`
**File:** `config/parser.py`
**Test file:** `tests/config/test_parser.py`

```python
def _substitute_env_vars(obj: str | dict | list | int | float | bool) -> str | dict | list | int | float | bool:
    """Recursively replace $VAR and ${VAR} in string values."""
    ...
```

**Acceptance tests:**

1. Given `.env` with `KEY=val123`, TOML with `field = "$KEY"`, and
   `--dotenv .env`, then field resolves to "val123".
2. Given `field = "${UNDEFINED}"` with var unset, then field is "" and
   warning logged.
3. Given nested TOML (dict of lists of strings) with env vars, then
   all substituted recursively.
4. Given non-string values (int, bool), then not modified.

## B: Configuration Schema

### B1: Cluster config TOML schema

As a maintainer, I want a single documented TOML schema with logical
sections, sensible defaults, and example Sanger values.

Sections:

- `[cluster]`: name, bundle, num_workers, master_flavour,
  worker_flavour, network_name, lustre_network, ssh_username,
  monitoring, floating_ip.
- `[volumes]`: create, name, size_gb, preserve_on_destroy,
  existing_volume_id.
- `[s3]`: endpoint, access_key, secret_key (for S3A/Ceph data
  storage, injected into Hadoop core-site.xml on all nodes).
- `[ceph_s3]`: endpoint, bucket, access_key, secret_key (for
  Pulumi state backend and rollout manifest storage).
- `[ssh_keys]`: public_keys (list of key strings).
- `[security_groups.master]`: ssh, http, https, spark_master,
  jupyter, hdfs, netdata (booleans).
- `[security_groups.worker]`: hdfs, spark_worker,
  all_tcp_internal (booleans).
- `[dns]`: search_domains (optional string).
- `[extras]`: system_packages, python_packages (optional lists).
- `[packer]`: base_image (name or ID of existing OpenStack image),
  flavour (build instance flavour), floating_ip_pool (optional).

**Package:** `hailstack/config/`
**File:** `config/schema.py`
**Test file:** `tests/config/test_schema.py`

```python
from pydantic import BaseModel, ConfigDict, field_validator

class SecurityGroupConfig(BaseModel):
    model_config = ConfigDict(strict=True)
    ssh: bool = True
    http: bool = False
    https: bool = False
    spark_master: bool = False
    spark_worker: bool = False
    jupyter: bool = False
    hdfs: bool = False
    netdata: bool = False
    all_tcp_internal: bool = False

class SecurityGroups(BaseModel):
    master: SecurityGroupConfig = SecurityGroupConfig(
        ssh=True, http=True, https=True, spark_master=True,
        jupyter=True, hdfs=True, netdata=True,
    )
    worker: SecurityGroupConfig = SecurityGroupConfig(
        ssh=False, hdfs=True, spark_worker=True, all_tcp_internal=True,
    )

class VolumeConfig(BaseModel):
    create: bool = False
    name: str = ""
    size_gb: int = 100
    preserve_on_destroy: bool = False
    existing_volume_id: str = ""

    @field_validator("existing_volume_id")
    @classmethod
    def no_both_create_and_existing(
        cls, v: str, info: ValidationInfo,
    ) -> str:
        ...

class S3Config(BaseModel):
    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""

class CephS3Config(BaseModel):
    endpoint: str = ""
    bucket: str = "hailstack-state"
    access_key: str = ""
    secret_key: str = ""

class SSHKeysConfig(BaseModel):
    public_keys: list[str] = []

    @field_validator("public_keys")
    @classmethod
    def at_least_one_key(
        cls, v: list[str],
    ) -> list[str]:
        if not v:
            raise ValueError(
                "At least one SSH public key required",
            )
        return v

class DNSConfig(BaseModel):
    search_domains: str = ""

class ExtrasConfig(BaseModel):
    system_packages: list[str] = []
    python_packages: list[str] = []

class PackerConfig(BaseModel):
    base_image: str  # required: name or ID of source image
    flavour: str = "m2.medium"
    floating_ip_pool: str = ""
    # Note: PackerConfig controls image build settings only.
    # Cluster runtime settings (floating_ip, master_flavour,
    # network_name) come from ClusterConfig.

class ClusterConfig(BaseModel):
    model_config = ConfigDict(strict=True)
    name: str
    bundle: str = ""  # empty = use default from matrix
    num_workers: int = 2
    master_flavour: str
    worker_flavour: str = ""  # empty = use master_flavour
    network_name: str = "cloudforms_network"
    lustre_network: str = ""
    ssh_username: str = "ubuntu"
    monitoring: str = "netdata"
    floating_ip: str = ""  # empty = auto-allocate
    volumes: VolumeConfig = VolumeConfig()
    s3: S3Config = S3Config()
    ceph_s3: CephS3Config = CephS3Config()
    ssh_keys: SSHKeysConfig = SSHKeysConfig()
    security_groups: SecurityGroups = SecurityGroups()
    dns: DNSConfig = DNSConfig()
    extras: ExtrasConfig = ExtrasConfig()
    packer: PackerConfig | None = None  # required for build-image
```

**File:** `example-config.toml` (repo root, checked in with Sanger
defaults)

```toml
# Hailstack cluster configuration.
# Copy this file, adjust values, and pass via --config.

[cluster]
name = "my-cluster"
bundle = "hail-0.2.137-gnomad-3.0.4-r2"
num_workers = 4
master_flavour = "m2.2xlarge"
# worker_flavour defaults to master_flavour if omitted
# worker_flavour = "m2.2xlarge"
network_name = "cloudforms_network"
# lustre_network = "lustre_network"
ssh_username = "ubuntu"
monitoring = "netdata"  # "netdata" or "none"
# floating_ip = "1.2.3.4"  # omit to auto-allocate

[packer]
base_image = "ubuntu-22.04"  # source image name in OpenStack
flavour = "m2.medium"  # instance flavour for Packer build
# floating_ip_pool = "public"  # omit if not needed

[volumes]
create = true
name = "my-data-vol"
size_gb = 500
preserve_on_destroy = false
# existing_volume_id = "uuid"  # mutually exclusive with create

[s3]
endpoint = "cog.sanger.ac.uk"
access_key = "$S3A_ACCESS_KEY"
secret_key = "$S3A_SECRET_KEY"

[ceph_s3]
endpoint = "cog.sanger.ac.uk"
bucket = "hailstack-state"
access_key = "$CEPH_S3_ACCESS_KEY"
secret_key = "$CEPH_S3_SECRET_KEY"

[ssh_keys]
public_keys = [
  "ssh-rsa AAAA... user@host1",
  "ssh-rsa BBBB... user@host2",
]

[security_groups.master]
ssh = true
http = true
https = true
spark_master = true
jupyter = true
hdfs = true
netdata = true

[security_groups.worker]
hdfs = true
spark_worker = true
all_tcp_internal = true

[dns]
search_domains = "internal.sanger.ac.uk"

[extras]
system_packages = ["libpq-dev", "mc"]
python_packages = ["pandas", "scikit-learn"]
```

**Acceptance tests:**

1. Given minimal TOML with only `[cluster]` name + master_flavour,
   when parsed, then all other sections populated with defaults.
2. Given `worker_flavour` omitted, then `worker_flavour` equals
   `master_flavour` after parsing.
3. Given `monitoring = "none"`, then `monitoring` field is "none".
4. Given `security_groups.master` omitted, then defaults applied
   (ssh=true, http=true, https=true, spark_master=true, jupyter=true,
   hdfs=true, netdata=true).
5. Given `ssh_keys.public_keys` with 3 entries, then all parsed.
6. Given `ssh_keys.public_keys` empty list, then raises
   `ValidationError` "At least one SSH public key required".
7. Given both `volumes.create = true` and `volumes.existing_volume_id`
   set, then raises `ValidationError`.
8. Given `monitoring = "prometheus"` (unsupported), then raises
   `ValidationError` with "must be 'netdata' or 'none'".
9. Given `num_workers = 0`, then raises `ValidationError`
   "num_workers must be >= 1".
10. Given `packer.base_image` omitted and `build-image` invoked,
    then raises `ConfigError` "packer.base_image required".
11. Given `packer.base_image = "ubuntu-22.04"`, then
    `PackerConfig.base_image` is "ubuntu-22.04".
12. Given `extras.system_packages = ["pkg1"]`, then parsed into
    list field.
13. Given `floating_ip = "not-an-ip"`, then raises
    `ValidationError` "floating_ip must be valid IPv4 address".
14. Given `ceph_s3.endpoint` empty with `create` invoked, then
    raises `ConfigError` "ceph_s3 credentials required".

## C: Compatibility Matrix

### C1: Matrix structure and bundle query

As a developer, I want a checked-in TOML file listing all supported
version bundles as flat entries with explicit human-friendly IDs.
Each bundle defines the full set of component versions. The same
Hail+Gnomad pair may appear in multiple bundles with different
underlying component versions (e.g. security-patched Spark/Hadoop).
Bundle IDs follow the pattern
`hail-<hail_ver>-gnomad-<gnomad_ver>-r<revision>`. The revision
suffix (`-r1`, `-r2`, etc.) distinguishes bundles sharing the same
Hail+Gnomad but with different infrastructure component versions.

**File:** `bundles.toml` (repo root)

```toml
[default]
bundle = "hail-0.2.137-gnomad-3.0.4-r2"

# Flat bundles with explicit IDs.
# Revision suffix distinguishes bundles sharing the same
# Hail+Gnomad pair but with different infrastructure versions.

[bundle."hail-0.2.137-gnomad-3.0.4-r1"]
hail = "0.2.137"
spark = "3.5.4"
hadoop = "3.4.0"
java = "11"
python = "3.12"
scala = "2.12.18"
gnomad = "3.0.4"
status = "supported"

[bundle."hail-0.2.137-gnomad-3.0.4-r2"]
hail = "0.2.137"
spark = "3.5.6"
hadoop = "3.4.1"
java = "11"
python = "3.12"
scala = "2.12.18"
gnomad = "3.0.4"
status = "latest"

[bundle."hail-0.2.136-gnomad-3.0.4-r1"]
hail = "0.2.136"
spark = "3.5.4"
hadoop = "3.4.0"
java = "11"
python = "3.12"
scala = "2.12.18"
gnomad = "3.0.4"
status = "supported"
```

Each `[bundle."<id>"]` section defines all component versions
explicitly. The bundle `id` is taken directly from the TOML key.

**Package:** `hailstack/config/`
**File:** `config/compatibility.py`
**Test file:** `tests/config/test_compatibility.py`

```python
from pydantic import BaseModel

class Bundle(BaseModel):
    id: str          # from TOML key: [bundle."<id>"]
    hail: str        # explicit field
    spark: str
    hadoop: str
    java: str
    python: str
    scala: str
    gnomad: str
    status: str  # "latest", "supported", "deprecated"

class CompatibilityMatrix:
    def __init__(self, path: Path) -> None:
        """Parse bundles.toml. Read bundle id and all component
        versions from [bundle."<id>"] sections."""
        ...
    def get_bundle(self, bundle_id: str) -> Bundle: ...
    def get_default(self) -> Bundle: ...
    def list_bundles(self) -> list[Bundle]: ...
```

**Acceptance tests:**

1. Given bundles.toml with 3 bundles, when initialised, then all 3
   accessible by ID.
2. When `get_default()` called, then returns bundle matching
   `[default].bundle`.
3. When `get_bundle("hail-0.2.137-gnomad-3.0.4-r2")` called, then
   returns bundle with `hail="0.2.137"`, `spark="3.5.6"`,
   `hadoop="3.4.1"`, `gnomad="3.0.4"`.
4. When `get_bundle("nonexistent")` called, then raises
   `BundleNotFoundError` listing available IDs.
5. When `list_bundles()` called, then returns all bundles.
6. Given empty bundles.toml (no `[bundle.*]` sections), then raises
   `ConfigError`.
7. Given two bundles `hail-0.2.137-gnomad-3.0.4-r1` and
   `hail-0.2.137-gnomad-3.0.4-r2`, when both queried, then they
   return different spark and hadoop versions (`r1` has
   `spark="3.5.4"`, `hadoop="3.4.0"`; `r2` has `spark="3.5.6"`,
   `hadoop="3.4.1"`).
8. Given bundle entry missing required field (e.g. `spark` omitted),
   then raises `ConfigError` with field name in message.

### C2: Bundle validation at CLI time

As a user, I want hailstack to refuse to proceed if my config specifies
a bundle not in the matrix. No override mechanism.

Validation runs for `create` and `build-image` commands only.
`status`, `reboot`, `destroy`, `install` allow any bundle (existing
clusters may use removed bundles).

**Package:** `hailstack/config/`
**File:** `config/validator.py`
**Test file:** `tests/config/test_validator.py`

```python
def validate_bundle(
    config: ClusterConfig,
    matrix: CompatibilityMatrix,
) -> Bundle:
    """Return Bundle if valid, raise BundleNotFoundError otherwise."""
    ...
```

**Acceptance tests:**

1. Given config.bundle in matrix, then returns Bundle.
2. Given config.bundle not in matrix, then raises
   `BundleNotFoundError` with message listing available bundles.
3. Given config.bundle empty, then uses matrix default.
4. When `destroy` invoked with config referencing removed bundle,
   then no validation error.

## D: Cluster Creation

### D1: Create command with dry-run

As a user, I want `hailstack create --config my.toml [--dry-run]` to
preview or provision infrastructure.

Workflow: load config -> validate -> resolve bundle -> pre-flight
resource validation (query OpenStack for image, flavours, network;
collect all failures) -> if `--dry-run`: Pulumi preview (no state
mutation) -> else: Pulumi up -> on success: log master floating IP
prominently -> on failure: automatic cleanup (Pulumi destroy) to
release all partially-created resources.

Pre-flight validation queries the OpenStack API before any Pulumi
calls to verify that the requested image, master flavour, worker
flavour, and network all exist. It also checks OpenStack compute
and volume quotas to confirm sufficient capacity for the requested
instances, vCPUs, RAM, and volume storage. If `floating_ip` is
specified, it verifies the IP exists and is not already associated.
If `volumes.existing_volume_id` is specified, it verifies the
volume exists. All failures are collected and reported in a single
error message so the user can fix everything in one pass.

**Package:** `hailstack/cli/commands/`
**File:** `commands/create.py`
**Test file:** `tests/cli/commands/test_create.py`

```python
@app.command()
def create(
    config: Annotated[
        str, typer.Option("--config", help="Cluster config TOML path."),
    ] = "./hailstack.toml",
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Preview without creating."),
    ] = False,
    dotenv: Annotated[
        str | None, typer.Option("--dotenv", help="Path to .env file."),
    ] = None,
) -> None:
    """Create a new Hailstack cluster."""
    ...
```

**Acceptance tests:**

1. Given valid config + image exists, when `--dry-run`, then Pulumi
   preview output shown to stdout and no resources created.
2. Given valid config + image exists, when no `--dry-run`, then Pulumi
   applies and outputs master floating IP.
3. Given image not found in Glance, then raises `ImageNotFoundError`
   with message "Run: hailstack build-image".
4. Given invalid config, then `ValidationError` before any Pulumi
   calls.
5. Given `--dry-run`, then exit code 0 and no Pulumi state written.
6. Given ceph_s3.endpoint empty or credentials missing, then raises
   `ConfigError` "Ceph S3 credentials required for Pulumi state
   backend" before any Pulumi calls.
7. Given ceph_s3 credentials invalid (auth failure), then raises
   `S3Error` with clear message including endpoint name.
8. Given master_flavour not found in OpenStack, then raises
   `ResourceNotFoundError` naming the unavailable flavour.
9. Given network_name not found in OpenStack, then raises
   `ResourceNotFoundError` naming the unavailable network.
10. Given image missing AND flavour missing, then single
    `ResourceNotFoundError` listing both unavailable resources.
11. Given Pulumi up fails mid-creation, then automatic cleanup
    runs (Pulumi destroy) so no orphaned resources remain.
12. On successful create, final stdout line is
    "Cluster '<name>' created. Master IP: <floating-ip>".
13. During create, progress messages logged to stderr for each
    stage: config loaded, bundle resolved, pre-flight passed,
    creating infrastructure, cluster ready.
14. Given compute quota insufficient for requested instances, then
    raises `QuotaExceededError` naming the exceeded quota
    (e.g. "instances: need 5, available 2").
15. Given `floating_ip = "1.2.3.4"` but IP not found or already
    associated, then `ResourceNotFoundError` naming the IP.
16. Given `volumes.existing_volume_id` set but volume not found,
    then `ResourceNotFoundError` naming the volume ID.
17. Given flavour missing AND quota exceeded, then single error
    listing both the unavailable flavour and the quota breach.

### D2: Pulumi stack and OpenStack resources

As a developer, I want Pulumi to manage: keypair, security groups,
network ports, instances (master + N workers), optional volume,
floating IP, with cloud-init user data per role.

Pulumi stack name: `hailstack-<cluster-name>`. Backend: Ceph S3. All
resources tagged with cluster name. Worker names formatted as
`<cluster-name>-worker-01`, `<cluster-name>-worker-02`, etc.

**Package:** `hailstack/pulumi/`
**File:** `pulumi/resources.py`
**Test file:** `tests/pulumi/test_resources.py`

OpenStack resources created:

| Resource | Name pattern | Notes |
|----------|-------------|-------|
| Keypair | `<name>-keypair` | First SSH public key (for OpenStack API) |
| SG master | `<name>-master-sg` | Rules per config |
| SG worker | `<name>-worker-sg` | Rules per config |
| SG rule: all TCP master<->worker | | Bidirectional |
| Port master | `<name>-master-port` | Main network |
| Port worker | `<name>-worker-port-NN` | Main network |
| Port lustre (optional) | `<name>-lustre-port-NN` | Lustre network |
| Instance master | `<name>-master` | master cloud-init |
| Instance worker | `<name>-worker-NN` | worker cloud-init |
| FloatingIP | `<name>-fip` | Auto or existing |
| FIP association | `<name>-fip-assoc` | To master |
| Volume (optional) | `<name>-vol` | If create=true |
| Volume attach | `<name>-vol-attach` | To master |

Security group rules (ingress, IPv4):

Master SG (when toggle=true):
| Toggle | Port | Protocol | Source |
|--------|------|----------|--------|
| ssh | 22 | TCP | 0.0.0.0/0 |
| http | 80 | TCP | 0.0.0.0/0 |
| https | 443 | TCP | 0.0.0.0/0 |
| spark_master | 7077 | TCP | 0.0.0.0/0 |
| hdfs | 9820 | TCP | 0.0.0.0/0 |
| jupyter | 8888 | TCP | 0.0.0.0/0 |
| netdata | 19999 | TCP | 0.0.0.0/0 |
| (always) | all TCP | TCP | worker SG |

Worker SG (when toggle=true):
| Toggle | Port | Protocol | Source |
|--------|------|----------|--------|
| hdfs | 9866 | TCP | 0.0.0.0/0 |
| spark_worker | 7078-7099 | TCP | 0.0.0.0/0 |
| all_tcp_internal | all TCP | TCP | master SG |
| all_tcp_internal | all TCP | TCP | worker SG |

Pulumi exports:
- `master_public_ip`: floating IP address
- `master_private_ip`: internal IP
- `worker_private_ips`: list of internal IPs
- `worker_names`: list of instance names
- `cluster_name`: cluster name
- `bundle_id`: bundle used

```python
def create_cluster_resources(
    config: ClusterConfig,
    bundle: Bundle,
) -> dict[str, pulumi.Output]: ...
```

**Acceptance tests:**

1. Given num_workers=3, then 4 instances created (1 master + 3
   workers).
2. Given security_groups.master.ssh=true, then SG rule for TCP 22
   from 0.0.0.0/0 exists on master SG.
3. Given security_groups.master.netdata=false, then no TCP 19999
   rule on master SG.
4. Always: all TCP from worker SG allowed on master SG.
5. Given volumes.create=true, size_gb=500, then 500GB volume created
   and attached to master.
6. Given floating_ip empty, then new FIP allocated.
7. Given floating_ip="1.2.3.4", then that IP associated to master.
8. Pulumi exports contain master_public_ip and worker_private_ips.
9. Given ssh_keys.public_keys with 3 entries, then all 3 keys
   injected into authorized_keys on all nodes via cloud-init.
10. Given lustre_network set but network not found in OpenStack,
    then PulumiError with "Network '<name>' not found".

### D3: Cloud-init provisioning (master)

As a developer, I want cloud-init on the master to configure hosts
file, generate SSL cert, set up nginx reverse proxy, create
htpasswd, configure LUKS keyfile (if volume), enable services, and
install extra cloud-init-time packages.

Master cloud-init enables these systemd services:
- `hdfs-namenode`, `yarn-rm`, `mapred-history` (Hadoop)
- `spark-master`, `spark-history-server` (Spark)
- `jupyter-lab` (JupyterLab, port 8888, base URL /jupyter)
- `nginx` (reverse proxy, ports 80/443)
- `nfs-server` (if volume attached, exports /home/<ssh_username>/data)
- `netdata` (if monitoring=netdata)

All software is pre-installed in the Packer image. Cloud-init only
generates cluster-specific config and starts services. Cloud-init
writes config files to dedicated paths (e.g.
`/etc/nginx/sites-enabled/hailstack.conf`,
`/etc/hadoop/conf/core-site.xml`) and never overwrites
package-managed default config files.

Nginx reverse proxy paths (all behind basic auth on HTTP/HTTPS):
| Path | Backend | Notes |
|------|---------|-------|
| /jupyter | localhost:8888 | WebSocket-aware |
| /spark | localhost:8080 | Spark Master UI |
| /sparkhist | localhost:18080 | Spark History UI |
| /yarn | localhost:8088 | YARN RM UI |
| /mapreduce | localhost:19888 | MR History UI |
| /nm<NN> | worker-NN:8042 | YARN NodeMgr UI |
| /hdfs | localhost:9870 | HDFS NameNode UI |
| /netdata | localhost:19999 | Netdata (if enabled) |

Secrets from env vars (injected via Pulumi user data):
- `HAILSTACK_WEB_PASSWORD`: nginx htpasswd + Jupyter password
- `HAILSTACK_VOLUME_PASSWORD`: LUKS keyfile (if volume)

**Package:** `hailstack/pulumi/`
**File:** `pulumi/cloud_init.py`
**Test file:** `tests/pulumi/test_cloud_init.py`

```python
def generate_master_cloud_init(
    config: ClusterConfig,
    bundle: Bundle,
    worker_ips: list[str],
) -> str:
    """Return cloud-init user-data bash script for master."""
    ...

def generate_worker_cloud_init(
    config: ClusterConfig,
    bundle: Bundle,
    master_ip: str,
    worker_index: int,
) -> str:
    """Return cloud-init user-data bash script for worker."""
    ...
```

**Acceptance tests:**

1. Given monitoring="netdata", then master cloud-init contains
   `systemctl enable netdata`.
2. Given monitoring="none", then no netdata references in output.
3. Given num_workers=3, then /etc/hosts block contains master +
   3 worker entries.
4. Cloud-init output contains nginx config with /jupyter, /spark,
   /sparkhist, /yarn, /mapreduce, /hdfs proxy locations.
5. Given HAILSTACK_WEB_PASSWORD set, then htpasswd file created.
6. Given HAILSTACK_WEB_PASSWORD not set, then raises
   `ConfigError` "HAILSTACK_WEB_PASSWORD required" before
   cluster creation.
7. Given volumes.create=true, then cloud-init contains LUKS keyfile
   setup and NFS export of /home/<ssh_username>/data.
8. Given volumes.create=true and HAILSTACK_VOLUME_PASSWORD set, then
   cloud-init creates LUKS container using that password as keyfile
   content.
9. Given volumes.create=true and HAILSTACK_VOLUME_PASSWORD not set,
   then raises `ConfigError` "HAILSTACK_VOLUME_PASSWORD required
   when volumes.create=true".
10. Given dns.search_domains="internal.sanger.ac.uk", then
    /etc/resolv.conf updated with search domain.
11. Given extras.system_packages=["libpq-dev"], then cloud-init
    includes apt-get install of those packages.
12. Given extras.python_packages=["pandas"], then cloud-init
    includes uv pip install of those packages into the overlay venv.
13. Given 3 SSH public keys in config, then cloud-init writes all
    3 to /home/<ssh_username>/.ssh/authorized_keys.
14. Given s3.endpoint and s3.access_key set, then cloud-init writes
    core-site.xml with `fs.s3a.endpoint`, `fs.s3a.access.key`,
    `fs.s3a.secret.key`, and `fs.s3a.connection.maximum=100`.
15. Given s3.endpoint empty, then core-site.xml omits S3A
    properties.
16. Given lustre_network set, then cloud-init configures
    /lustre mount point on master.
17. Nginx config written to `/etc/nginx/sites-enabled/hailstack.conf`;
    cloud-init output does NOT reference `/etc/nginx/nginx.conf`.
18. Hadoop/Spark config written under `/etc/hadoop/conf/` and
    `/etc/spark/conf/`; cloud-init does not overwrite
    package-managed defaults (e.g. no writes to
    `/etc/hadoop/hadoop-env.sh` or `/etc/default/hadoop`).

### D4: Cloud-init provisioning (workers)

As a developer, I want cloud-init on workers to configure hosts,
enable worker services, mount NFS from master (if volume).

Worker cloud-init enables:
- `hdfs-datanode`, `yarn-nm` (Hadoop)
- `spark-worker` (Spark, connects to spark-master:7077)
- `netdata` (if monitoring=netdata, streams to master)

Workers wait for master NFS (port 2049) before mounting
/home/<ssh_username>/data.

**Package:** `hailstack/pulumi/`
**File:** `pulumi/cloud_init.py`
**Test file:** `tests/pulumi/test_cloud_init.py`

**Acceptance tests:**

1. Worker cloud-init contains `systemctl enable spark-worker`.
2. Worker cloud-init /etc/hosts contains master and all workers.
3. Given volume attached on master, then worker mounts
   master:/home/<ssh_username>/data via NFS.
4. Given monitoring="netdata", then worker cloud-init enables
   netdata with stream config pointing to master.
5. Given s3.endpoint set, then worker cloud-init writes
   core-site.xml with same S3A properties as master.
6. Given lustre_network set, then worker cloud-init configures
   /lustre mount point.
7. Given extras.system_packages=["libpq-dev"], then worker
   cloud-init includes apt-get install of those packages.
8. Given extras.python_packages=["pandas"], then worker cloud-init
   includes uv pip install into the overlay venv.
9. Worker cloud-init writes config files to dedicated paths
   (e.g. `/etc/hadoop/conf/core-site.xml`) and does not
   overwrite package-managed defaults.

### D5: Floating IP management

As a user, I want floating IP auto-allocated if not specified, or
use existing IP. On destroy, always release.

**Package:** `hailstack/pulumi/`
**File:** `pulumi/resources.py`
**Test file:** `tests/pulumi/test_resources.py`

**Acceptance tests:**

1. Given floating_ip empty, then new FIP allocated and associated
   to master.
2. Given floating_ip="1.2.3.4", then that IP used.
3. On destroy, FIP released to pool.

## E: Image Building

### E1: Build-image command

As a user, I want `hailstack build-image --config my.toml
[--bundle <id>]` to build a Packer image for a version bundle and
upload it to OpenStack Glance.

If `--bundle` omitted, uses bundle from config (or matrix default).
Image name: `hailstack-<bundle-id>`. Built directly in OpenStack
(Packer openstack builder). Based on existing image specified in
config or by Packer var.

Packer template: HCL2 at `packer/hailstack.pkr.hcl`. Provisioner
scripts organised by OS (`packer/scripts/ubuntu/`). Extensible
architecture: add `packer/scripts/<os>/` directories for other OS
support; only Ubuntu implemented now.

Fat image includes: Hadoop, Spark, Hail, Java/JDK, Scala, Python,
Gnomad, uv, JupyterLab, Netdata, nginx. All at versions specified by
the bundle. Systemd service unit files for all services. No secrets,
no cluster-specific config. Hail is always present in every image
at the bundle's hail version (not optional).

Image verification: Packer provisioner scripts include embedded
version checks as final steps (e.g. `hadoop version | grep`,
`python --version | grep`). If any check fails, the Packer build
fails before the image is uploaded to Glance.

SSH username from config used as Packer SSH communicator user.

**Package:** `hailstack/packer/`
**File:** `packer/builder.py`
**Test file:** `tests/packer/test_builder.py`

```python
def build_image(
    config: ClusterConfig,
    bundle: Bundle,
) -> str:
    """Run packer build using config.packer settings. Return image ID.

    Packer variable mapping:
      bundle_id       <- bundle.id
      hail_version    <- bundle.hail
      spark_version   <- bundle.spark
      hadoop_version  <- bundle.hadoop
      java_version    <- bundle.java
      python_version  <- bundle.python
      scala_version   <- bundle.scala
      gnomad_version  <- bundle.gnomad
      base_image      <- config.packer.base_image
      ssh_username    <- config.ssh_username
      flavor          <- config.packer.flavour
      network         <- config.network_name
      floating_ip_pool <- config.packer.floating_ip_pool
    """
    ...
```

CLI command:

```python
@app.command(name="build-image")
def build_image_cmd(
    config: Annotated[
        str, typer.Option("--config", help="Cluster config TOML path."),
    ] = "./hailstack.toml",
    bundle: Annotated[
        str | None,
        typer.Option("--bundle", help="Bundle ID (default: from config)."),
    ] = None,
    dotenv: Annotated[
        str | None, typer.Option("--dotenv", help="Path to .env file."),
    ] = None,
) -> None:
    """Build a Packer image for a version bundle."""
    ...
```

**Acceptance tests:**

1. Given valid bundle, when build-image invoked, then Packer runs
   with correct variable values (versions, image name).
2. Image named `hailstack-hail-0.2.137-gnomad-3.0.4-r2` uploaded
   to Glance.
3. Given bundle not in matrix, then `BundleNotFoundError` before
   Packer invoked.
4. Given Packer build fails, then `PackerError` with stderr output.
5. Image contains Hadoop at bundle's hadoop version (verified by
   checking `/opt/hadoop/bin/hadoop version` output).
6. Image contains Spark at bundle's spark version.
7. Image contains Hail at bundle's hail version.
8. Image contains Java/JDK at bundle's java version.
9. Image contains Python at bundle's python version.
10. Image contains Scala at bundle's scala version.
11. Image contains Gnomad at bundle's gnomad version.
12. Image contains uv.
13. Image contains systemd unit files for all services.
14. Image does NOT contain SSH keys or cluster-specific config.
15. Given `--bundle hail-0.2.136-gnomad-3.0.4-r1` with config
    specifying bundle `hail-0.2.137-gnomad-3.0.4-r2`, then image
    built for `hail-0.2.136-gnomad-3.0.4-r1` (CLI override takes
    precedence).
16. During build-image, progress messages logged to stderr for
    each stage: config loaded, bundle resolved, Packer starting,
    image uploaded.

### E2: Packer template structure

As a developer, I want a Packer HCL2 template with variables for all
bundle versions, base image, and SSH username. Provisioner scripts
modular by OS.

**File:** `packer/hailstack.pkr.hcl`

```hcl
variable "bundle_id"       { type = string }
variable "hail_version"    { type = string }
variable "spark_version"   { type = string }
variable "hadoop_version"  { type = string }
variable "java_version"    { type = string }
variable "python_version"  { type = string }
variable "scala_version"   { type = string }
variable "gnomad_version"  { type = string }
variable "base_image"      { type = string }
variable "ssh_username"    { type = string default = "ubuntu" }
variable "flavor"          { type = string }
variable "network"         { type = string }
variable "floating_ip_pool" { type = string default = "" }

source "openstack" "hailstack" {
  image_name       = "hailstack-${var.bundle_id}"
  source_image     = var.base_image
  flavor           = var.flavor
  ssh_username     = var.ssh_username
  networks         = [var.network]
  floating_ip_pool = var.floating_ip_pool
}

build {
  sources = ["source.openstack.hailstack"]

  provisioner "shell" {
    scripts = [
      "scripts/base.sh",
      "scripts/ubuntu/packages.sh",
      "scripts/ubuntu/hadoop.sh",
      "scripts/ubuntu/spark.sh",
      "scripts/ubuntu/hail.sh",
      "scripts/ubuntu/jupyter.sh",
      "scripts/ubuntu/gnomad.sh",
      "scripts/ubuntu/uv.sh",
      "scripts/ubuntu/netdata.sh",
    ]
    environment_vars = [
      "HADOOP_VERSION=${var.hadoop_version}",
      "SPARK_VERSION=${var.spark_version}",
      "HAIL_VERSION=${var.hail_version}",
      "JAVA_VERSION=${var.java_version}",
      "PYTHON_VERSION=${var.python_version}",
      "SCALA_VERSION=${var.scala_version}",
      "GNOMAD_VERSION=${var.gnomad_version}",
    ]
  }
}
```

**Acceptance tests:**

1. Packer template validates with `packer validate`.
2. Given different bundle versions, then environment vars passed to
   provisioner scripts match.
3. All 9 provisioner scripts exist and are executable:
   `scripts/base.sh` and `scripts/ubuntu/{packages,hadoop,spark,
   hail,jupyter,gnomad,uv,netdata}.sh`.
4. Built image contains base Python venv at
   `/opt/hailstack/base-venv` with Hail, PySpark, JupyterLab, and
   Gnomad pre-installed via uv.

## F: Volume Management

### F1: Volume lifecycle via Pulumi

As a user, I want Pulumi to create, attach, and optionally preserve
volumes. LUKS encryption for NFS volumes.

Three modes:
1. `volumes.create=true`: Pulumi creates new volume of
   `volumes.size_gb` GB, attaches to master.
2. `volumes.existing_volume_id` set: Pulumi attaches existing volume.
3. Neither: no volume.

On destroy:
- `preserve_on_destroy=true`: volume detached but not deleted.
- `preserve_on_destroy=false` (default): volume deleted.

LUKS encryption handled in cloud-init using HAILSTACK_VOLUME_PASSWORD:
1. Create keyfile from password.
2. Create LUKS container on volume device.
3. Open LUKS, create ext4 filesystem if new.
4. Mount to /home/<ssh_username>/data.
5. Export via NFS to workers.

Lustre: separate unencrypted provider network type. Configured via
`lustre_network` in config. Network ports created for all nodes.
Mount path: /lustre.

**Package:** `hailstack/pulumi/`
**File:** `pulumi/resources.py`
**Test file:** `tests/pulumi/test_resources.py`

**Acceptance tests:**

1. Given volumes.create=true, size_gb=500, then Pulumi creates
   500GB volume and attaches to master.
2. Given volumes.existing_volume_id="uuid", then Pulumi attaches
   that volume (no creation).
3. Given both create=true and existing_volume_id set, then
   `ValidationError` at config time (never reaches Pulumi).
4. Given preserve_on_destroy=true, on destroy, then volume not
   deleted.
5. Given preserve_on_destroy=false, on destroy, then volume
   deleted.
6. Given lustre_network set, then Lustre ports created for master
   and all workers.
7. Master cloud-init for volume includes LUKS setup, ext4 create,
   mount to /home/<ssh_username>/data, NFS export.
8. Worker cloud-init mounts master:/home/<ssh_username>/data to local
   /home/<ssh_username>/data.

## G: Post-Creation Software Installation

The `install` command adds supplementary system and Python packages
to running clusters -- packages outside the bundle (e.g. pandas,
libpq-dev). It does not modify bundle components (Spark, Hadoop,
Hail); changing those requires `destroy` + `create` with a new
bundle (see Appendix: Key Decisions).

### G1: Install command

As a user, I want `hailstack install` to add system and Python
packages to all running cluster nodes.

Supports inline args and file-based lists:
```shell
hailstack install --config my.toml \
  --system pkg1 pkg2 \
  --python pypkg1 pypkg2 \
  [--smoke-test "python -c 'import pkg'"] \
  [--file packages.toml]
```

Workflow:
1. Load config, resolve nodes from Pulumi stack outputs.
2. Build Ansible inventory from Pulumi outputs (master + workers).
3. Determine SSH private key: uses SSH agent (default) or explicit
   `--ssh-key` path. Ansible connects as `config.ssh_username`
   with this key to all nodes.
4. Run idempotent Ansible playbook on all nodes:
   - System packages via OS package manager (apt).
   - Python packages via uv into shared managed env (not system pip).
5. Record rollout:
   - Rollout manifest JSON to Ceph S3 with SHA-256 hash.
   - Per-node result JSON to Ceph S3.
   - Node-local `/var/lib/hailstack/software-state.json`.
6. Verify on each node:
   - Package present (dpkg -l / uv pip list).
   - Version matches expected.
   - Python import check succeeds (for Python packages).
   - Optional smoke test command succeeds.
7. Auto-retry with backoff on per-node failures (1/2/4s, 3 attempts).
8. On partial failure: write partial state to S3, exit non-zero.

Targets all nodes uniformly (no master/worker distinction).

packages.toml format:
```toml
[system]
packages = ["libpq-dev", "mc"]

[python]
packages = ["pandas>=2.0", "scikit-learn"]
```

**Package:** `hailstack/cli/commands/`
**File:** `commands/install.py`
**Test file:** `tests/cli/commands/test_install.py`

```python
@app.command()
def install(
    config: Annotated[str, typer.Option("--config")] = "./hailstack.toml",
    system: Annotated[list[str] | None, typer.Option("--system")] = None,
    python: Annotated[list[str] | None, typer.Option("--python")] = None,
    file: Annotated[str | None, typer.Option("--file")] = None,
    smoke_test: Annotated[str | None, typer.Option("--smoke-test")] = None,
    ssh_key: Annotated[
        str | None,
        typer.Option("--ssh-key", help="SSH private key path (default: agent)."),
    ] = None,
    dotenv: Annotated[str | None, typer.Option("--dotenv")] = None,
) -> None:
    """Install packages on all cluster nodes."""
    ...
```

**Acceptance tests:**

1. Given `--system libpq-dev`, then `dpkg -l libpq-dev` succeeds on
   all nodes after install.
2. Given `--python pandas`, then `uv pip list` in shared env shows
   pandas on all nodes.
3. Given `--file packages.toml` with system and python sections, then
   both installed.
4. Given `--system pkg1 --file packages.toml` where file contains
   `system=["pkg2"]`, then both pkg1 and pkg2 installed (inline
   and file args are merged, not mutually exclusive).
5. Given `--smoke-test "python -c 'import pandas'"`, then command
   runs on all nodes and exits 0; if exit non-zero on any node,
   that node marked as failed in rollout results.
6. Given `--python "pandas>=2.0"`, then installed version on every
   node is >= 2.0 (verified by version check in rollout).
7. Rollout manifest JSON written to Ceph S3 with SHA-256 hash field.
8. Per-node result JSON written to S3 with node name, success/fail,
   package list.
9. Node-local `/var/lib/hailstack/software-state.json` updated.
10. Given `--python pandas`, then `python -c 'import pandas'`
    succeeds on all nodes (import check).
11. Given one node unreachable (out of 4), then hailstack retries
    up to 3 times with exponential backoff (1/2/4s), then 3
    succeed, 1 fails, partial state written, exit code non-zero.
12. Re-running same install command is idempotent (no errors, no
    changes).
13. Given `--ssh-key /path/to/key`, then Ansible uses that key for
    SSH connections to all nodes.
14. During install, progress messages logged to stderr for each
    stage: config loaded, resolving nodes, running Ansible,
    verifying packages, uploading rollout manifest.

### G2: Ansible runner for installs

As a developer, I want an Ansible executor that runs the install
playbook on a dynamic inventory resolved from Pulumi.

**Package:** `hailstack/ansible/`
**File:** `ansible/runner.py`
**Test file:** `tests/ansible/test_runner.py`

```python
def run_install_playbook(
    inventory: dict[str, list[str]],
    system_packages: list[str],
    python_packages: list[str],
    ssh_username: str,
    ssh_key_path: Path,
) -> list[NodeResult]:
    """Run Ansible install playbook, return per-node results."""
    ...
```

**File:** `ansible/install.yml`

Ansible playbook with roles/packages role. Tasks:
- Install system packages via `apt` module.
- Install Python packages via `command` module running
  `uv pip install` into the overlay venv at
  `/opt/hailstack/overlay-venv` (not the base venv). The base venv
  at `/opt/hailstack/base-venv` (with Hail, PySpark, JupyterLab,
  and Gnomad) remains immutable.

Python environment strategy:
- **Base venv** (`/opt/hailstack/base-venv`): created by Packer,
  contains core stack (Hail, PySpark, JupyterLab, and Gnomad).
  Never modified post-image-creation.
- **Overlay venv** (`/opt/hailstack/overlay-venv`): created on first
  need -- either by cloud-init when `extras.python_packages` is
  non-empty, or by the first `install` invocation, whichever comes
  first. Inherits base venv packages via
  `--system-site-packages`, receives user-installed packages.
  Users' Python code can import from both.

**Acceptance tests:**

1. Given inventory with 4 hosts and packages=["mc"], then mc
   installed on all 4.
2. Given empty system_packages and python_packages=["requests"],
   then only Python install runs.
3. Returns list of `NodeResult` with hostname, success, packages.
4. Given base venv contains Hail, when install pandas, then Hail
   still in base venv unchanged and pandas in overlay venv.
5. Overlay venv can import both base packages and overlay packages.

### G3: Rollout manifest storage

As a developer, I want rollout manifests and per-node results
persisted in Ceph S3 for audit trail.

**Package:** `hailstack/storage/`
**File:** `storage/rollout.py`
**Test file:** `tests/storage/test_rollout.py`

```python
from pydantic import BaseModel

class RolloutManifest(BaseModel):
    cluster_name: str
    timestamp: str
    system_packages: list[str]
    python_packages: list[str]
    sha256: str  # hash of manifest content
    node_count: int
    success_count: int

class NodeResult(BaseModel):
    hostname: str
    success: bool
    system_installed: list[str]
    python_installed: list[str]
    errors: list[str]

def upload_rollout(
    manifest: RolloutManifest,
    node_results: list[NodeResult],
    ceph_s3_config: CephS3Config,
    cluster_name: str,
) -> str:
    """Upload manifest + results to Ceph S3. Return S3 key."""
    ...
```

S3 key structure (timestamp is ISO 8601 UTC, e.g.
`20250101T120000Z`):
`hailstack/<cluster>/rollouts/<timestamp>/manifest.json`
`hailstack/<cluster>/rollouts/<timestamp>/nodes/<hostname>.json`

**Acceptance tests:**

1. Given manifest + 3 node results, then 4 objects uploaded to S3.
2. Manifest JSON contains sha256 hash of content (excluding hash
   field itself).
3. S3 keys follow documented pattern.

## H: Cluster Status and Health

### H1: Status command

As a user, I want `hailstack status --config my.toml` to show cluster
state. Default: Pulumi stack outputs only (fast, no SSH). With
`--detailed`: SSH health probes + resource usage.

Default output (human-readable table):
```text
Cluster: my-cluster
Bundle:  hail-0.2.137-gnomad-3.0.4-r2
Master:  1.2.3.4 (m2.2xlarge)
Workers: 3
  worker-01: 10.0.0.2
  worker-02: 10.0.0.3
  worker-03: 10.0.0.4
Volume:  my-data-vol (500GB)
```

With `--detailed`: adds per-service systemd status checks via SSH
and per-node resource usage.

```text
Services:
  spark-master:         active (master)
  hdfs-namenode:        active (master)
  yarn-rm:              active (master)
  jupyter-lab:          active (master)
  spark-history-server: active (master)
  nginx:                active (master)
  netdata:              active (master)
  spark-worker:         active (worker-01, worker-02, worker-03)
  hdfs-datanode:        active (worker-01, worker-02, worker-03)
  yarn-nm:              active (worker-01, worker-02, worker-03)

Resources:
  master:    CPU 23%  MEM 45%  DISK 12%
  worker-01: CPU 67%  MEM 78%  DISK 8%
  worker-02: CPU 55%  MEM 62%  DISK 8%
  worker-03: CPU 12%  MEM 34%  DISK 8%
```

`--json` flag outputs machine-parseable JSON.

**Package:** `hailstack/cli/commands/`
**File:** `commands/status.py`
**Test file:** `tests/cli/commands/test_status.py`

```python
@app.command()
def status(
    config: Annotated[str, typer.Option("--config")] = "./hailstack.toml",
    detailed: Annotated[bool, typer.Option("--detailed")] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="JSON output."),
    ] = False,
    dotenv: Annotated[str | None, typer.Option("--dotenv")] = None,
) -> None:
    """Show cluster status."""
    ...
```

**Acceptance tests:**

1. Given running cluster, when status invoked (no --detailed), then
   table output includes cluster name, bundle, master IP, worker
   count and IPs.
2. Given --detailed, then SSH probes check systemd service statuses
   on all nodes.
3. Given --detailed, then per-node CPU%, MEM%, DISK% shown.
4. Given --json, then output is valid JSON matching documented
   schema.
5. Given --json --detailed, then JSON includes services and
   resources.
6. Given cluster not found in Pulumi state, then error message
   "Cluster not found".
7. Given --detailed and one worker unreachable via SSH, then that
   worker shown as "unreachable" (others still shown).

### H2: SSH health probe module

As a developer, I want an SSH module that checks systemd service
status and gathers resource metrics from cluster nodes.

Master services checked: spark-master, hdfs-namenode, yarn-rm,
spark-history-server, jupyter-lab, nginx, netdata (if enabled),
mapred-history, nfs-server (if volume).

Worker services checked: spark-worker, hdfs-datanode, yarn-nm,
netdata (if enabled).

**Package:** `hailstack/ssh/`
**File:** `ssh/health.py`
**Test file:** `tests/ssh/test_health.py`

```python
from pydantic import BaseModel

class ServiceStatus(BaseModel):
    name: str
    active: bool
    node: str

class NodeResources(BaseModel):
    hostname: str
    cpu_percent: float
    memory_percent: float
    disk_percent: float

async def check_service_health(
    hosts: list[str],
    ssh_username: str,
    services: dict[str, list[str]],  # hostname -> service list
) -> list[ServiceStatus]:
    """SSH to each host, check systemd service statuses."""
    ...

async def gather_resource_usage(
    hosts: list[str],
    ssh_username: str,
) -> list[NodeResources]:
    """SSH to each host, return CPU/memory/disk usage."""
    ...
```

The sync `status` command invokes async probes via
`asyncio.run()`. Concurrent SSH probes use `asyncio.gather()`
for parallel execution across nodes.

**Acceptance tests:**

1. Given host with spark-master active, then returns
   `ServiceStatus(name="spark-master", active=True, node="master")`.
2. Given host with stopped service, then active=False.
3. Given unreachable host, then `SSHError` raised for that host
   (others still checked).
4. Resource usage returns float percentages (0.0-100.0).

## I: Worker Reboot

### I1: Reboot command

As a user, I want `hailstack reboot --config my.toml [--node
worker-01]` to reboot workers. Default: all workers. `--node`:
specific individual worker.

No master reboot support. Resolves workers from Pulumi stack. SSH
to each worker, run `sudo reboot`. Wait for SSH connectivity to
return (up to 5 min timeout).

**Package:** `hailstack/cli/commands/`
**File:** `commands/reboot.py`
**Test file:** `tests/cli/commands/test_reboot.py`

```python
@app.command()
def reboot(
    config: Annotated[str, typer.Option("--config")] = "./hailstack.toml",
    node: Annotated[str | None, typer.Option("--node")] = None,
    dotenv: Annotated[str | None, typer.Option("--dotenv")] = None,
) -> None:
    """Reboot worker nodes."""
    ...
```

**Acceptance tests:**

1. Given no --node, then all workers rebooted.
2. Given --node worker-01, then only worker-01 rebooted.
3. Given --node nonexistent, then error "Worker not found".
4. Given --node master (or any master reference), then error
   "Cannot reboot master node".
5. After reboot, SSH connectivity verified within 5 minutes.
6. Given worker does not return SSH connectivity within 5 minutes,
   then raises `SSHError` with timeout message.

## J: Cluster Destruction

### J1: Destroy command with confirmation

As a user, I want `hailstack destroy --config my.toml [--dry-run]` to
destroy the cluster with interactive confirmation (like Terraform).

Workflow:
1. Load config, find Pulumi stack.
2. Show resources to be destroyed (Pulumi preview).
3. Prompt: "Do you want to destroy cluster '<name>'? Type the
   cluster name to confirm: "
4. If input matches cluster name: Pulumi destroy.
5. If `--dry-run`: show plan only, skip confirmation.
6. On destroy: floating IP released to pool.

**Package:** `hailstack/cli/commands/`
**File:** `commands/destroy.py`
**Test file:** `tests/cli/commands/test_destroy.py`

```python
@app.command()
def destroy(
    config: Annotated[str, typer.Option("--config")] = "./hailstack.toml",
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    dotenv: Annotated[str | None, typer.Option("--dotenv")] = None,
) -> None:
    """Destroy a Hailstack cluster."""
    ...
```

**Acceptance tests:**

1. Given --dry-run, then plan shown and no resources destroyed.
2. Given correct cluster name typed at prompt, then Pulumi destroy
   runs.
3. Given incorrect name at prompt, then abort with "Aborted" message.
4. After destroy, floating IP released.
5. During destroy, progress messages logged to stderr for each
   stage: config loaded, previewing resources, awaiting
   confirmation, destroying infrastructure, cleanup complete.
6. On successful destroy, final stdout line is
   "Cluster '<name>' destroyed.".

## K: Authentication Conversion

### K1: convert-auth command

As a user, I want `hailstack convert-auth` to convert OpenStack
openrc.sh environment variables into clouds.yaml format. Run after
`source openrc.sh`.

Reads from current env vars:

Required (error if missing):
- OS_AUTH_URL, OS_PROJECT_NAME, OS_USERNAME

Optional (omitted from output if not set):
- OS_PASSWORD, OS_REGION_NAME, OS_PROJECT_DOMAIN_NAME,
  OS_USER_DOMAIN_NAME, OS_IDENTITY_API_VERSION

Writes valid clouds.yaml to stdout by default. `--write` flag
writes to `~/.config/openstack/clouds.yaml` (creating dirs),
backing up existing file to `clouds.yaml.bak.<timestamp>`.

Output format:
```yaml
clouds:
  openstack:
    auth:
      auth_url: <OS_AUTH_URL>
      project_name: <OS_PROJECT_NAME>
      username: <OS_USERNAME>
      password: <OS_PASSWORD>
      user_domain_name: <OS_USER_DOMAIN_NAME>
      project_domain_name: <OS_PROJECT_DOMAIN_NAME>
    region_name: <OS_REGION_NAME>
    identity_api_version: <OS_IDENTITY_API_VERSION>
```

**Package:** `hailstack/cli/commands/`
**File:** `commands/convert_auth.py`
**Test file:** `tests/cli/commands/test_convert_auth.py`

```python
@app.command(name="convert-auth")
def convert_auth(
    write: Annotated[
        bool,
        typer.Option("--write", help="Write to ~/.config/openstack/clouds.yaml."),
    ] = False,
) -> None:
    """Convert openrc.sh env vars to clouds.yaml format."""
    ...
```

**Acceptance tests:**

1. Given OS_AUTH_URL, OS_PROJECT_NAME, OS_USERNAME set, then valid
   YAML printed to stdout.
2. Given OS_AUTH_URL not set, then `ConfigError` "OS_AUTH_URL not
   set. Source your openrc.sh first."
3. Given --write, then file written to
   ~/.config/openstack/clouds.yaml.
4. Given --write with existing file, then backup created as
   `clouds.yaml.bak.<timestamp>`.
5. YAML output parseable and contains all provided env vars.
6. Given OS_PASSWORD not set, then omitted from output (not error).

## L: Monitoring

### L1: Netdata configuration

As a developer, I want Netdata pre-installed in Packer images and
enabled/disabled via cloud-init based on `monitoring` config field.

When monitoring="netdata":
- Master: Netdata started, HDFS JMX monitoring configured,
  accepts streams from workers, health alarms for HDFS capacity
  (warn >70%, crit >80%), missing blocks, dead nodes.
- Workers: Netdata started, streams metrics to master.
- Nginx proxies Netdata dashboard at /netdata with basic auth.

When monitoring="none": Netdata not started on any node.

Netdata streaming uses a random UUID4 API key generated at cluster
creation time by the cloud-init generator. The key is embedded in
both master and worker cloud-init scripts (master as accept key,
workers as stream destination key). Not persisted elsewhere.

**Package:** `hailstack/pulumi/`
**File:** `pulumi/cloud_init.py`
**Test file:** `tests/pulumi/test_cloud_init.py`

**Acceptance tests:**

1. Given monitoring="netdata", then master cloud-init enables
   netdata service and configures stream accept.
2. Given monitoring="netdata", then worker cloud-init configures
   stream destination as master IP with API key.
3. Given monitoring="netdata", then nginx config includes /netdata
   proxy with basic auth.
4. Given monitoring="none", then no netdata references in any
   cloud-init output.
5. Master Netdata config includes HDFS JMX endpoint
   (http://localhost:9870/jmx) and per-worker DataNode endpoints
   (http://worker-NN:9864/jmx).
6. Health alarms configured: HDFS capacity warn >70%, crit >80%.

## M: Apptainer & CI/CD

### M1: Apptainer definition

As a user, I want to run hailstack from an Apptainer container on
HPC without root. Container packages: Python + hailstack CLI +
Pulumi + Packer + Ansible.

Cluster VMs are OpenStack instances (not containers). Apptainer
only packages the tooling.

**File:** `Apptainer.def`

```apptainer
Bootstrap: docker
From: python:3.14-slim

%post
  apt-get update && apt-get install -y \
    curl unzip openssh-client uuid-runtime gnupg
  # Install Pulumi
  curl -fsSL https://get.pulumi.com | sh
  # Install Packer
  curl -fsSL https://releases.hashicorp.com/packer/1.11.2/\
    packer_1.11.2_linux_amd64.zip -o /tmp/packer.zip \
    && unzip -o /tmp/packer.zip -d /usr/local/bin \
    && rm /tmp/packer.zip
  # Install Ansible
  pip install ansible
  # Install hailstack
  pip install /opt/hailstack
  # Clean up
  apt-get clean && rm -rf /var/lib/apt/lists/*

%environment
  export PATH="/root/.pulumi/bin:$PATH"

%runscript
  exec hailstack "$@"
```

**Acceptance tests:**

1. Built SIF contains `hailstack` executable.
2. `apptainer run hailstack.sif --version` prints version.
3. SIF contains `pulumi`, `packer`, `ansible` executables.
4. SIF size < 500MB.

### M2: GitHub Actions CI workflow

As a developer, I want CI to run tests, lint, type-check on every
push/PR.

**File:** `.github/workflows/ci.yml`

Jobs:
1. `lint`: ruff check + ruff format --check
2. `typecheck`: pyright (strict mode)
3. `test`: pytest with coverage

**Acceptance tests:**

1. CI runs on push to main and all PRs.
2. Lint job fails if ruff reports errors.
3. Type-check job fails if `pyright --strict` reports errors.
4. Test job runs all tests and reports coverage.

### M3: GitHub Actions release workflow

As a developer, I want CI to build and publish Apptainer SIF on
tagged releases.

**File:** `.github/workflows/release.yml`

Triggers on version tags (v*). Builds Apptainer SIF. Uploads as
GitHub release asset.

**Acceptance tests:**

1. Workflow triggers on `v*` tag push.
2. SIF asset attached to GitHub release.
3. Release notes auto-generated from commits.

## N: Documentation

### N1: User README

As a non-technical user, I want a fool-proof README that walks me
through setup, configuration, and every command with examples.

**File:** `README.md`

Sections:
1. **What is Hailstack?** - One paragraph.
2. **Prerequisites** - OpenStack account, clouds.yaml (with
   convert-auth instructions), Ceph S3 bucket, Apptainer installed.
3. **Quick Start** - Step-by-step:
   a. Download SIF from GitHub releases.
   b. Create alias: `alias hailstack='apptainer run hailstack.sif'`
   c. Source openrc.sh, run `hailstack convert-auth --write`.
   d. Copy example-config.toml, edit values.
   e. Set env vars (HAILSTACK_WEB_PASSWORD, S3 keys) in .env.
   f. `hailstack build-image --config my.toml` (first time only).
   g. `hailstack create --config my.toml --dotenv .env`.
   h. Access JupyterLab at https://<master-ip>/jupyter.
4. **Configuration Reference** - Every TOML field with description,
   type, default, and example.
5. **Commands Reference** - Each command with synopsis, all options,
   and example invocations:
   - create, destroy, reboot, build-image, install, status,
     convert-auth.
6. **Compatibility Bundles** - Flat bundle structure with revision
   scheme for security-patched variants, how to list bundles,
   select one, what each field means.
7. **Volume Management** - Creating vs attaching, LUKS encryption,
   NFS, preserve on destroy.
8. **Security** - Security groups, SSL, basic auth, SSH keys.
9. **Monitoring** - Enabling/disabling Netdata, accessing dashboard.
10. **Troubleshooting** - Common issues and solutions.
11. **FAQ** - Frequently asked questions.

**Acceptance tests:**

1. README contains all 11 sections listed above.
2. Quick Start has exactly 8 lettered steps (a-h).
3. Commands Reference documents all 7 commands with examples.
4. Configuration Reference documents every field in
   example-config.toml.
5. All code examples use `hailstack` (not osdataproc).

### N2: Developer and AI documentation

As a developer or AI agent, I want docs covering project structure,
testing, building, and contributing.

**File:** `CONTRIBUTING.md`

Sections:
1. **Project Structure** - Module layout with one-line descriptions.
2. **Development Setup** - `uv sync`, `uv run pytest`, `uv run ruff`,
   `uv run pyright`.
3. **Testing** - How to run tests, write new tests, test conventions
   (pytest, fixtures, parametrize).
4. **Building Apptainer Image** - Local build instructions.
5. **Adding a New Bundle** - Step-by-step: edit bundles.toml, run
   build-image, test, submit PR.
6. **Architecture Decisions** - Why Pulumi over Terraform, why fat
   images, why S3 state.

**File:** `AI.md`

AI agent-oriented reference:
1. **Coding Conventions** - Python 3.14, strict typing, Pydantic v2,
   Typer CLI patterns.
2. **Commands** - Exact uv/ruff/pyright/pytest commands.
3. **Error Hierarchy** - Exception classes and when to use each.
4. **Key Modules** - What each module does, dependencies.
5. **Common Tasks** - How to add a CLI command, add a config field,
   update a Packer script, add a test.

**Acceptance tests:**

1. CONTRIBUTING.md contains all 6 sections.
2. AI.md contains all 5 sections.
3. Both files reference the same test commands.
4. AI.md lists all error classes from errors.py.

## O: Integration Test Scaffolding

### O1: Pulumi integration tests with mocked provider

As a developer, I want integration test scaffolding that validates
the full Pulumi automation workflow using mocked OpenStack providers.

**Package:** `tests/integration/`
**File:** `tests/integration/test_pulumi_lifecycle.py`

Uses Pulumi automation API with a mocked OpenStack provider to
exercise the full create/destroy lifecycle without real
infrastructure.

**Acceptance tests:**

1. Given valid config and mocked Glance with matching image, when
   create invoked, then Pulumi preview succeeds and reports
   expected resource count.
2. Given mocked Pulumi up, when create invoked, then all expected
   exports (master_public_ip, worker_private_ips, cluster_name,
   bundle_id) are present.
3. Given created stack, when destroy invoked with confirmation,
   then Pulumi destroy succeeds and stack is removed.

### O2: Image verification smoke tests

As a developer, I want Packer build verification to be testable in
CI via script-level checks.

**File:** `tests/integration/test_packer_scripts.py`

**Acceptance tests:**

1. Each provisioner script in `packer/scripts/ubuntu/` ends with a
   version-check command that exits non-zero on mismatch.
2. Given a mock environment with version vars set, when
   `scripts/base.sh` runs, then exits 0.

### O3: End-to-end workflow test skeleton

As a developer, I want a documented test skeleton for full cluster
lifecycle testing (build -> create -> install -> status -> destroy).

**File:** `tests/integration/test_e2e_skeleton.py`

Provides pytest fixtures and test stubs for end-to-end testing
against a real OpenStack environment. Marked with
`@pytest.mark.integration` (skipped by default in CI).

**Acceptance tests:**

1. Integration tests skipped unless `--run-integration` flag passed.
2. Test skeleton defines fixtures for: OpenStack credentials,
   temporary config TOML, cleanup (destroy on teardown).
3. Skeleton contains test stubs for: build-image, create, install
   packages, status (default + detailed), reboot workers, destroy.

## Implementation Order

### Phase 1: Foundation (Sequential)

Stories: A1, A2, A3, B1, C1, C2, K1.

Establish CLI skeleton, config parsing with Pydantic models, env var
substitution, compatibility matrix loading and validation, and the
standalone convert-auth command. All unit-testable with no
infrastructure dependencies. Every subsequent phase depends on config
loading.

Implementor skill: `python-implementor`
(`/nfs/users/nfs_s/sb10/.agents/skills/python-implementor/SKILL.md`)
Reviewer skill: `python-reviewer`
(`/nfs/users/nfs_s/sb10/.agents/skills/python-reviewer/SKILL.md`)
Conventions skill: `python-conventions`
(`/nfs/users/nfs_s/sb10/.agents/skills/python-conventions/SKILL.md`)

### Phase 2: Packer Image Building (Sequential)

Stories: E1, E2.

Build the Packer template and Python builder module. Requires Phase 1
(config + matrix) to resolve bundle versions. Output: fat VM images
in OpenStack Glance. Integration-testable by validating Packer
template and generated variables.

### Phase 3: Pulumi Infrastructure (Sequential)

Stories: D2, D3, D4, D5, F1, L1.

Pulumi stack creation with all OpenStack resources, cloud-init
generation for master and workers, floating IP management, volume
lifecycle, and monitoring config. Requires Phase 1 (config + matrix).
Testable with Pulumi mocks and cloud-init output unit tests.

### Phase 4: Cluster Lifecycle Commands (Sequential)

Stories: D1, J1.

Wire create and destroy commands to Pulumi stack. Requires Phase 3
(Pulumi resources). Includes dry-run support and destroy confirmation
prompt.

### Phase 5: Operations (Sequential)

Stories: G1, G2, G3, H1, H2, I1.

Post-creation install command with Ansible runner and S3 rollout
storage. Status command with SSH health probes. Reboot command.
Requires Phase 3 (Pulumi stack for node resolution) and Phase 1
(S3 config).

### Phase 6: Packaging, CI & Integration Tests (Parallel)

Stories: M1, M2, M3, O1, O2, O3.

Apptainer definition, GitHub Actions workflows, and integration
test scaffolding. Independent of other phases except needing the
final package structure. O1 requires Phase 3 (Pulumi resource
definitions).

### Phase 7: Documentation (Sequential)

Stories: N1, N2.

README and developer docs. Should be written last when all features
are implemented and stable.

## Appendix: Key Decisions

**Single TOML config:** One file per cluster containing all
parameters. Example with Sanger defaults checked in; users copy and
gitignore. Env var substitution for secrets.

**Fat Packer images:** All software pre-installed at build time.
Cloud-init only generates cluster-specific config (hosts, nginx,
SSL, htpasswd, LUKS, service enablement). Eliminates post-boot
download failures and version drift.

**Pulumi over Terraform:** Single Python language for CLI + IaC.
Pulumi automation API enables programmatic control, dry-run via
preview, and structured outputs without shell-exec wrapping.

**S3-only Pulumi state:** Team-shared clusters require centralised
state. Ceph S3 backend prevents concurrent-apply conflicts. No
local state files.

**No update command:** Destroy + recreate is simpler with fat images
for bundle-level changes (Spark, Hadoop, Hail, Java versions). Avoids
in-place upgrade complexity. Data preserved via S3 and optionally
preserved volumes. The `install` command (section G) handles
supplementary packages (system and Python) on running clusters --
these are user add-ons outside the bundle, not bundle upgrades.

**Bundle selection:** Users pick from named presets (not individual
versions). Matrix locked in repo, changes via PRs. Prevents
untested version combinations.

**Configurable SSH username:** Supports multiple OS bases (Ubuntu,
Rocky) without hardcoding.

**Testing strategy:** Unit tests mock all infrastructure (Pulumi,
OpenStack API, SSH, S3). Cloud-init generation tested by
string/regex assertions on output. Config parsing tested exhaustively
with valid/invalid inputs. Integration test scaffolding (section O)
provides Pulumi automation with mocked providers and end-to-end
test skeletons for real OpenStack environments.

**Error policy:** Validation errors (config, bundle) raise
immediately. Network/SSH errors retry with exponential backoff
(1/2/4s, 3 attempts). All errors logged to stderr; specific error
classes enable programmatic handling.
