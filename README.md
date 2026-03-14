# Hailstack

## What is Hailstack?

Hailstack is a command-line tool for building OpenStack images and provisioning Spark, Hadoop, Hail, and JupyterLab clusters on OpenStack. It validates a single TOML configuration file, uses compatibility bundles from `bundles.toml` to keep Hail and infrastructure versions aligned, stores Pulumi state and install rollout manifests in Ceph S3, and exposes the cluster services through an nginx-protected master node.

## Prerequisites

- An OpenStack project with quota for one master VM, at least one worker VM, networking, floating IPs, and optional Cinder volumes.
- OpenStack credentials available either through `~/.config/openstack/clouds.yaml` or exported `OS_*` variables. If you still use an `openrc.sh` file, source it and run `hailstack convert-auth --write` once to generate `clouds.yaml`.
- A Ceph S3 bucket for Pulumi state and install rollout manifests. `create`, `destroy`, `status`, `reboot`, and `install` all rely on the `ceph_s3` settings in your config.
- Apptainer installed on the machine where you will run the CLI.
- SSH public keys ready to inject into cluster nodes.
- Environment variables for any secrets referenced from the config. With the shipped example config, that normally means `S3A_ACCESS_KEY`, `S3A_SECRET_KEY`, `CEPH_S3_ACCESS_KEY`, `CEPH_S3_SECRET_KEY`, `HAILSTACK_WEB_PASSWORD`, and, when a data volume is enabled, `HAILSTACK_VOLUME_PASSWORD`.

## Quick Start

a. Download the latest `hailstack.sif` artifact from the GitHub releases page for this repository.

b. Create the shell alias so every example below resolves to the Apptainer image.

```bash
alias hailstack='apptainer run hailstack.sif'
```

c. Source your OpenStack `openrc.sh` file, then generate `clouds.yaml` in the standard OpenStack location.

```bash
source ./openrc.sh
hailstack convert-auth --write
```

d. Copy the example config and edit the cluster name, flavours, network names, bundle, bucket names, SSH keys, and any optional sections you need.

```bash
cp example-config.toml my-cluster.toml
```

e. Put your secrets in a `.env` file and keep the TOML file free of credentials. If you keep `volumes.create = true` or set `volumes.existing_volume_id`, include `HAILSTACK_VOLUME_PASSWORD` as well.

```dotenv
HAILSTACK_WEB_PASSWORD=choose-a-strong-password
HAILSTACK_VOLUME_PASSWORD=choose-a-volume-passphrase
S3A_ACCESS_KEY=your-s3a-access-key
S3A_SECRET_KEY=your-s3a-secret-key
CEPH_S3_ACCESS_KEY=your-ceph-state-access-key
CEPH_S3_SECRET_KEY=your-ceph-state-secret-key
```

f. Build the OpenStack image for your chosen bundle the first time you use a new bundle or base image.

```bash
hailstack build-image --config my-cluster.toml
```

g. Create the cluster from your validated config and `.env` file.

```bash
hailstack create --config my-cluster.toml --dotenv .env
```

h. After `create` finishes, open JupyterLab on the master node through the nginx proxy.

```text
https://<master-ip>/jupyter/
```

## Configuration Reference

The CLI reads TOML with `tomllib`, substitutes `$VAR` and `${VAR}` in string values, optionally loads a `.env` file before parsing, and then validates the result with Pydantic. Defaults below are the current implementation defaults, not recommendations.

### `[cluster]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `cluster.name` | Cluster identifier used in OpenStack resource names and the Pulumi stack name. Must match `^[a-z][a-z0-9-]{1,62}$`. | `string` | required | `my-cluster` |
| `cluster.bundle` | Compatibility bundle ID. Leave empty to use `[default].bundle` from `bundles.toml` for bundle-aware commands. | `string` | `""` | `hail-0.2.137-gnomad-3.0.4-r2` |
| `cluster.num_workers` | Number of worker nodes to create. Must be at least `1`. | `integer` | `2` | `4` |
| `cluster.master_flavour` | OpenStack flavour for the master VM. | `string` | required | `m2.2xlarge` |
| `cluster.worker_flavour` | OpenStack flavour for workers. If omitted, Hailstack reuses `cluster.master_flavour`. | `string` | `""` then resolved to `cluster.master_flavour` | `m2.2xlarge` |
| `cluster.network_name` | Required OpenStack network for the management interface on all nodes. | `string` | `cloudforms_network` | `cloudforms_network` |
| `cluster.lustre_network` | Optional second network name added to every node for Lustre access. | `string` | `""` | `lustre_network` |
| `cluster.lustre_mount_target` | Lustre mount target written to `/etc/fstab` when `cluster.lustre_network` is set. | `string` | `10.1.0.1@tcp:/fsx` | `192.0.2.10@tcp:/fsx` |
| `cluster.ssh_username` | Login user used by SSH-based commands and cloud-init paths. | `string` | `ubuntu` | `ubuntu` |
| `cluster.monitoring` | Monitoring mode. Only `netdata` and `none` are accepted. | `string` | `netdata` | `netdata` |
| `cluster.floating_ip` | Existing unassociated IPv4 address to attach to the master. Leave empty to allocate one. | `string` | `""` | `1.2.3.4` |
| `cluster.floating_ip_pool` | Floating IP pool name used when `create` needs to allocate a master IP instead of reusing `cluster.floating_ip`. | `string` | `""` | `public` |

### `[packer]`

`[packer]` is optional overall, but `build-image` requires it.

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `packer.base_image` | Source image name that Packer should boot before provisioning the Hailstack image. | `string` | required when `[packer]` is present | `ubuntu-22.04` |
| `packer.flavour` | OpenStack flavour used during the image build. | `string` | `m2.medium` | `m2.medium` |
| `packer.floating_ip_pool` | Floating IP pool name used by Packer while building an image. `create` uses `cluster.floating_ip_pool`, but the legacy packer field is still accepted for backwards compatibility. | `string` | `""` | `public` |

### `[volumes]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `volumes.create` | Create and attach a new Cinder data volume on the master. Mutually exclusive with `volumes.existing_volume_id`. | `boolean` | `false` | `true` |
| `volumes.name` | Name to assign to a newly created Cinder volume. | `string` | `""` | `my-data-vol` |
| `volumes.size_gb` | Size of a newly created Cinder volume in GiB. | `integer` | `100` | `500` |
| `volumes.preserve_on_destroy` | Keep a newly created volume when `destroy` removes the cluster. | `boolean` | `false` | `false` |
| `volumes.existing_volume_id` | Existing Cinder volume UUID to attach instead of creating a new one. | `string` | `""` | `uuid` |

### `[s3]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `s3.endpoint` | S3A endpoint injected into Hadoop configuration on cluster nodes. | `string` | `""` | `cog.sanger.ac.uk` |
| `s3.access_key` | Access key for runtime S3A access from Spark and Hadoop. Supports env-var substitution. | `string` | `""` | `$S3A_ACCESS_KEY` |
| `s3.secret_key` | Secret key for runtime S3A access from Spark and Hadoop. Supports env-var substitution. | `string` | `""` | `$S3A_SECRET_KEY` |

### `[ceph_s3]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `ceph_s3.endpoint` | Ceph S3 endpoint used for the Pulumi backend and install rollout uploads. | `string` | `""` | `cog.sanger.ac.uk` |
| `ceph_s3.bucket` | Bucket that stores Pulumi state and rollout manifests. | `string` | `hailstack-state` | `hailstack-state` |
| `ceph_s3.access_key` | Ceph S3 access key. Required for Pulumi-backed commands such as `create`, `destroy`, `status`, `reboot`, and for rollout uploads during `install`. | `string` | `""` | `$CEPH_S3_ACCESS_KEY` |
| `ceph_s3.secret_key` | Ceph S3 secret key paired with `ceph_s3.access_key`. Required anywhere the Pulumi backend or rollout storage is used. | `string` | `""` | `$CEPH_S3_SECRET_KEY` |

### `[ssh_keys]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `ssh_keys.public_keys` | Public keys written into `authorized_keys` on all nodes. At least one non-empty key is required. The first key is also used for the OpenStack keypair resource. | `list[string]` | empty list, but validation requires at least one key | `["ssh-rsa AAAA... user@host1", "ssh-rsa BBBB... user@host2"]` |

### `[security_groups.master]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `security_groups.master.ssh` | Expose TCP `22` on the master. | `boolean` | `true`; if you provide a partial `[security_groups.master]` table, omitted fields fall back to the base security-group defaults instead of the master preset | `true` |
| `security_groups.master.http` | Expose TCP `80` on the master. | `boolean` | `true`; omitted inside a partial `[security_groups.master]` table resolves to `false` | `true` |
| `security_groups.master.https` | Expose TCP `443` on the master. | `boolean` | `true`; omitted inside a partial `[security_groups.master]` table resolves to `false` | `true` |
| `security_groups.master.spark_master` | Expose the Spark master RPC port on TCP `7077`. | `boolean` | `true`; omitted inside a partial `[security_groups.master]` table resolves to `false` | `true` |
| `security_groups.master.jupyter` | Expose the Jupyter backend port on TCP `8888`. The usual public path is still the nginx proxy at `/jupyter/`. | `boolean` | `true`; omitted inside a partial `[security_groups.master]` table resolves to `false` | `true` |
| `security_groups.master.hdfs` | Expose the HDFS namenode service port on TCP `9820`. | `boolean` | `true`; omitted inside a partial `[security_groups.master]` table resolves to `false` | `true` |
| `security_groups.master.netdata` | Expose Netdata on TCP `19999` when monitoring is enabled. | `boolean` | `true`; omitted inside a partial `[security_groups.master]` table resolves to `false` | `true` |

### `[security_groups.worker]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `security_groups.worker.spark_worker` | Expose TCP `7078-7099` on workers for Spark worker processes. | `boolean` | `true`; omitted inside a partial `[security_groups.worker]` table resolves to `false` | `true` |
| `security_groups.worker.hdfs` | Expose TCP `9866` on workers for HDFS datanodes. | `boolean` | `true`; omitted inside a partial `[security_groups.worker]` table resolves to `false` | `true` |
| `security_groups.worker.all_tcp_internal` | Allow all internal TCP traffic from master and worker security groups to worker nodes. | `boolean` | `true`; omitted inside a partial `[security_groups.worker]` table resolves to `false` | `true` |

### `[dns]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `dns.search_domains` | Search domain string appended to `/etc/resolv.conf` on cluster nodes. | `string` | `""` | `internal.sanger.ac.uk` |

### `[extras]`

| Field | Description | Type | Default | Example |
| --- | --- | --- | --- | --- |
| `extras.system_packages` | Extra Debian packages installed by cloud-init during cluster creation. | `list[string]` | empty list | `["libpq-dev", "mc"]` |
| `extras.python_packages` | Extra Python packages installed into the overlay virtual environment during cluster creation. | `list[string]` | empty list | `["pandas", "scikit-learn"]` |

## Commands Reference

Every command below is available from `hailstack --help`. Examples assume you are running the Apptainer alias from Quick Start.

### `hailstack create`

Synopsis: Create a new cluster from a TOML configuration file.

Options:

| Option | Meaning |
| --- | --- |
| `--config PATH` | Path to the cluster configuration TOML file. |
| `--dry-run` | Validate the config, resolve the bundle, run OpenStack pre-flight checks, and show the Pulumi preview without creating resources. |
| `--dotenv PATH` | Load environment variables from a `.env` file before parsing the config. |

Examples:

```bash
hailstack create --config my-cluster.toml --dotenv .env
hailstack create --config my-cluster.toml --dotenv .env --dry-run
```

### `hailstack destroy`

Synopsis: Destroy an existing cluster from a TOML configuration file.

Options:

| Option | Meaning |
| --- | --- |
| `--config PATH` | Path to the cluster configuration TOML file. |
| `--dry-run` | Show the destroy preview without deleting resources. |
| `--dotenv PATH` | Load environment variables from a `.env` file before parsing the config. |

Examples:

```bash
hailstack destroy --config my-cluster.toml --dotenv .env
hailstack destroy --config my-cluster.toml --dotenv .env --dry-run
```

`destroy` requires an exact cluster-name confirmation unless `--dry-run` is used.

### `hailstack reboot`

Synopsis: Reboot worker nodes in an existing cluster.

Options:

| Option | Meaning |
| --- | --- |
| `--config PATH` | Path to the cluster configuration TOML file. Defaults to `./hailstack.toml`. |
| `--node TEXT` | Optional worker name selector. If omitted, all workers are rebooted. The master cannot be rebooted through this command. |
| `--dotenv PATH` | Load environment variables from a `.env` file before parsing the config. |

Examples:

```bash
hailstack reboot --config my-cluster.toml --dotenv .env
hailstack reboot --config my-cluster.toml --dotenv .env --node my-cluster-worker-02
```

### `hailstack build-image`

Synopsis: Build a Hailstack image for a selected compatibility bundle.

Options:

| Option | Meaning |
| --- | --- |
| `--config PATH` | Path to the cluster configuration TOML file. Defaults to `./hailstack.toml`. |
| `--bundle TEXT` | Bundle ID override. If omitted, the command uses `cluster.bundle` or the default bundle. |
| `--dotenv PATH` | Load environment variables from a `.env` file before parsing the config. |

Examples:

```bash
hailstack build-image --config my-cluster.toml
hailstack build-image --config my-cluster.toml --bundle hail-0.2.137-gnomad-3.0.4-r2
```

### `hailstack install`

Synopsis: Install extra packages on all existing cluster nodes and upload a rollout manifest to Ceph S3.

Options:

| Option | Meaning |
| --- | --- |
| `--config PATH` | Path to the cluster configuration TOML file. Defaults to `./hailstack.toml`. |
| `--system TEXT` | One or more system packages to install with `apt`. Repeat the option to add more packages. |
| `--python TEXT` | One or more Python packages to install with `uv`. Repeat the option to add more packages. |
| `--file PATH` | TOML package list file merged with `--system` and `--python`. |
| `--smoke-test TEXT` | Command to run on every node after installation. |
| `--ssh-key PATH` | SSH private key path. If omitted, the command relies on your SSH agent. |
| `--dotenv PATH` | Load environment variables from a `.env` file before parsing the config. |

Examples:

```bash
hailstack install --config my-cluster.toml --dotenv .env --system mc --system tree
hailstack install --config my-cluster.toml --dotenv .env --python pandas --smoke-test "python -c 'import pandas'"
```

### `hailstack status`

Synopsis: Show the current cluster summary and, optionally, detailed SSH health information.

Options:

| Option | Meaning |
| --- | --- |
| `--config PATH` | Path to the cluster configuration TOML file. Defaults to `./hailstack.toml`. |
| `--detailed` | Probe services and resource usage over SSH. |
| `--json` | Emit JSON instead of the plain-text summary. |
| `--dotenv PATH` | Load environment variables from a `.env` file before parsing the config. |

Examples:

```bash
hailstack status --config my-cluster.toml --dotenv .env
hailstack status --config my-cluster.toml --dotenv .env --detailed --json
```

### `hailstack convert-auth`

Synopsis: Convert current `OS_*` environment variables into an OpenStack `clouds.yaml` document.

Options:

| Option | Meaning |
| --- | --- |
| `--write` | Write the generated YAML to `~/.config/openstack/clouds.yaml`, backing up any existing file as `clouds.yaml.bak.<timestamp>`. |

Examples:

```bash
hailstack convert-auth
hailstack convert-auth --write
```

## Compatibility Bundles

Hailstack ships a flat compatibility matrix in `bundles.toml`. Each bundle has an explicit ID such as `hail-0.2.137-gnomad-3.0.4-r2`, where the `-rN` suffix distinguishes infrastructure revisions that keep the same Hail and gnomAD pairing but change supporting components such as Spark or Hadoop.

Current checked-in bundles are:

| Bundle ID | Hail | Spark | Hadoop | Java | Python | Scala | gnomAD | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `hail-0.2.136-gnomad-3.0.4-r1` | `0.2.136` | `3.5.4` | `3.4.0` | `11` | `3.12` | `2.12.18` | `3.0.4` | `supported` |
| `hail-0.2.137-gnomad-3.0.4-r1` | `0.2.137` | `3.5.4` | `3.4.0` | `11` | `3.12` | `2.12.18` | `3.0.4` | `supported` |
| `hail-0.2.137-gnomad-3.0.4-r2` | `0.2.137` | `3.5.6` | `3.4.1` | `11` | `3.12` | `2.12.18` | `3.0.4` | `latest` |

How Hailstack uses bundles:

- `build-image` and `create` resolve `cluster.bundle`. If it is blank, they fall back to `[default].bundle`.
- OpenStack image names are expected to match `hailstack-<bundle-id>`.
- Bundle fields map directly to the image build inputs: `hail`, `spark`, `hadoop`, `java`, `python`, `scala`, `gnomad`, and `status`.
- There is no dedicated `hailstack` subcommand to list bundles today. Inspect the shipped `bundles.toml` in the repository or in the container image when choosing a bundle.

## Volume Management

Shared storage is driven by the `[volumes]` section.

- If `volumes.create = true`, Hailstack creates a new Cinder volume, attaches it to the master as `/dev/vdb`, encrypts it with LUKS using `HAILSTACK_VOLUME_PASSWORD`, formats it as ext4 on first use, and mounts it at `/home/<ssh_username>/data`.
- If `volumes.existing_volume_id` is set, Hailstack attaches the existing volume and opens it with the same `HAILSTACK_VOLUME_PASSWORD`, but it does not run `luksFormat` again.
- When a data volume is enabled, the master exports `/home/<ssh_username>/data` over NFS and workers mount `master:/home/<ssh_username>/data` automatically.
- `volumes.preserve_on_destroy = true` only affects newly created volumes. It tells Pulumi to retain the created volume when the cluster is destroyed.
- If neither `volumes.create` nor `volumes.existing_volume_id` is set, no shared data volume or NFS export is configured.

## Security

The current implementation exposes services through security-group toggles plus nginx on the master node.

- `HAILSTACK_WEB_PASSWORD` is mandatory for cluster creation because cloud-init generates both the Jupyter password hash and the nginx basic-auth file.
- nginx listens on `80` and `443`, proxies `/jupyter/`, `/spark/`, `/sparkhist/`, `/yarn/`, `/mapreduce/`, `/hdfs/`, per-worker `/nmNN/` pages, and `/netdata/` when monitoring is enabled.
- The basic-auth username is `hailstack`.
- cloud-init generates a self-signed TLS certificate on the master at `/etc/nginx/ssl/hailstack.crt` with the matching key at `/etc/nginx/ssl/hailstack.key`.
- All `ssh_keys.public_keys` entries are written to `authorized_keys` on the cluster nodes. The first key is also registered as the OpenStack keypair.
- Public ingress is controlled by the `security_groups.master.*` and `security_groups.worker.*` booleans. Worker nodes default to internal-only access apart from the Spark worker and HDFS data-node ports.

## Monitoring

Monitoring is controlled by `cluster.monitoring`.

- `netdata` is the default. The master enables Netdata locally and accepts streaming data from workers through a shared API key generated at cluster creation time.
- Workers stream Netdata metrics to the master; they do not need a separate dashboard URL when the nginx proxy is in use.
- When monitoring is enabled, the dashboard is available at `https://<master-ip>/netdata/` behind the same nginx authentication as JupyterLab.
- If `cluster.monitoring = "none"`, Hailstack omits Netdata service setup and the `/netdata/` proxy path.
- `hailstack status --detailed` is separate from Netdata. It probes service health and resource usage over SSH even if Netdata is disabled.

## Troubleshooting

| Symptom | Likely cause | What to check |
| --- | --- | --- |
| `Bundle '<id>' not found` | `cluster.bundle` does not match any ID in `bundles.toml`. | Pick one of the bundle IDs listed in the Compatibility Bundles section or leave `cluster.bundle` empty to use the default bundle. |
| `Ceph S3 credentials required for Pulumi state backend` | `ceph_s3.endpoint`, `ceph_s3.bucket`, `ceph_s3.access_key`, or `ceph_s3.secret_key` is blank after env substitution. | Check the TOML values and the `.env` file passed through `--dotenv`. |
| `HAILSTACK_WEB_PASSWORD required` | The environment variable was not set before `create`. | Export the variable directly or add it to the `.env` file used by `create`. |
| `HAILSTACK_VOLUME_PASSWORD required when a data volume is attached` | A volume is enabled, but the LUKS passphrase is missing. | Add `HAILSTACK_VOLUME_PASSWORD` to your environment or disable the volume section. |
| `OpenStack CLI not found` | The host environment lacks the `openstack` client. | Use the packaged container entrypoint or install the expected OpenStack CLI in the environment that runs Hailstack. |
| `Network '<name>' not found`, missing image, or missing floating IP errors during `create` | Pre-flight resource checks failed. | Verify `cluster.network_name`, `cluster.lustre_network`, the built image name `hailstack-<bundle-id>`, and any fixed `cluster.floating_ip`. |
| `Timed out waiting for SSH connectivity to return` during `reboot` | The worker did not come back cleanly after reboot or SSH access is blocked. | Check the instance console, confirm the security-group SSH setting, and verify the configured `cluster.ssh_username`. |
| `Cluster not found` from `status` | The Pulumi stack for that cluster name does not exist in the configured Ceph backend. | Re-check `cluster.name`, Ceph S3 credentials, and whether the cluster was already destroyed. |

## FAQ

**Do I need to rebuild the image every time I create a cluster?**

No. Rebuild when you switch to a bundle that does not already have a matching `hailstack-<bundle-id>` image or when you change the base image and want a fresh artifact.

**Can I reuse an existing floating IP?**

Yes. Set `cluster.floating_ip` to an existing unassociated IPv4 address. Leave it blank to let Pulumi allocate one, optionally using `cluster.floating_ip_pool`.

**Can I turn monitoring off?**

Yes. Set `cluster.monitoring = "none"`. `status --detailed` still works because it uses SSH probes rather than Netdata.

**Can I add software after the cluster is running?**

Yes. Use `hailstack install` with `--system`, `--python`, or `--file`. The command uploads a rollout manifest to the configured Ceph S3 bucket.

**Where does cluster state live?**

Pulumi state and install rollout manifests live in the `ceph_s3.bucket` bucket, not in local state files.

**Can I attach an existing data volume instead of creating one?**

Yes. Set `volumes.existing_volume_id` and leave `volumes.create = false`. You still need `HAILSTACK_VOLUME_PASSWORD` so the cluster can open the LUKS volume on boot.
