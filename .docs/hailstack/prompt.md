# Hailstack Feature Description

Reimplement-from-scratch the user-visible features of osdataproc using:

* Frontend: Python 3.12 + Typer (typed, testable)
* Infra-as-code: Pulumi Python
* VM configuration: Packer + cloud-init
* Container: Apptainer (deploy from HPC without root)
* Version management: Single config file + compatibility matrix

Compared to the existing app, the new one's strengths will be:
* Single language
* A well-structured Python codebase with clear module boundaries, type hints, tests, and a single infrastructure-as-code tool that is dramatically easier for an AI agent to understand, modify, and validate
* Dry-run validation
* Version compatibility matrices (Spark↔Hadoop↔Hail), validated at CLI time, to solve things breaking when we update to a new base image which might force a new version of one of these.
* Integration test scaffolding.
* No hard-coding of stuff or structural fragility

It must avoid these problems with the existing app:
* We've had all sorts of issues with osdataproc over the last few years, and every time it breaks we have spent days between us trying to work out what is wrong.
* We need to be able to update Hail versions regularly, sometimes doing this will necessitate updating spark (and hadoop?) and it can be tricky to find a combination of the three which work.
* "We" do not have the technical ability or time to figure out these issues and want an LLM to just quickly fix things if needed.

The spec will be implemented in a new empty repo, so the spec shouldn't mention anything about existing code, files or structure.
It should retain the ability to work with Sanger-specific things, ie. any necessary config should be carried over as example configs or similar.
A clear fool-proof no-technical-ability README should be specified as part of the spec. At the end there should also be developer/AI docs covering testing, building, contributing etc..

## Notes

* Reboot should remain as a sub-command (workers only).
* Password for nginx and LUKS volume encryption should switch from interactive prompt to coming from an env var, with standard .env style support to read the env var.
* Preserve LUKS encryption for NFS and Lustre provider network support. Preserve S3A Ceph/S3-compatible storage config.
* Pulumi should fully manage volume lifecycle by default, but have options to not delete volume on cluster destroy, and to attach existing volume on cluster create.
* Keep the JupyterLab on master, baking in to image.
* Preserve all the web UIs exposed by the current nginx reverse proxy.
* The compatibility matrix is a checked-in file in the repo, updated via PRs. TOML format. Allows users to select from one of multiple known-good combos, defaulting to latest.
* Monitoring should be main config-file selectable (none, Netdata, in future possibility of prometheus+grafana etc.), only latest Netdata implemented for now.
* Switch from using openrc.sh for OpenStack auth to using clouds.yaml file, but spec a sub-command that converts from one to the other by using env vars after the user sources their openrc.sh file. Write to stdout by default, with flag option to write to standard location (backing up existing file) instead.
* Packer images should be "fat" with hadoop/spark/hail/scala/uv pre-installed at specific versions. Auto-upload to (build directly in) OpenStack. Based on an existing given image ID/name in OpenStack.
* Since we use fat images, update subcommand no longer needed. Keep destroy and recreate.
* Packer images need to support whatever base OS is on the supplied starter image. Extensible architecture, only Ubuntu support implemented now.
* Still support the ability to specify extra pkgs and python modules to be installed on all nodes (remove master/worker distinction) via some appropriate mechanism. At cluster creation time only using Pulumi injecting cloud-init, but also allow software installation on all nodes after cluster creation via very easy to use (just supply system/python pkg names) subcommand:
  * resolves nodes from the Pulumi stack
  * runs an idempotent Ansible playbook on all nodes
  * installs into the right place:
    * system packages via OS package manager
    * Python packages into a uv-managed shared env, not ad-hoc system pip
  1. Record the rollout:
     * rollout manifest in Ceph S3 (from which compute a hash to enable audit trail and verification etc.)
     * per-node result JSON in Ceph S3
     * node-local state file such as software-state.json
  2. Verify after apply:
     * package present on every node
     * version matches expected
     * Python import check succeeds
     * optional smoke test command succeeds
* Hail is always installed.
* Gnomad is currently installed as an extra install, but should be part of Packer images.
* The compatibility matrix includes spark, hadoop, hail, java/JDK, Python, Scala, Gnomad.
* Images are rebuilt when versions change using a sub-command.
* Keep it simple with OpenStack support only (not other clouds).
* Sanger-specific DNS must be kept as a config option (in the 1 main config file).
* Apptainer packages CLI+Pulumi+Packer+Ansible, with cluster VMs being OpenStack instances running Packer images, not containers. Apptainer pkg pre-built and distributed by GitHub release using CI. Spec a GitHub Actions workflow to be implemented with the rest of the app.
* Pulumi cluster state in Ceph S3 for shared team state. Config kept in 1 file with other config.
* Implement the TODO of allowing multiple public SSH keys in the main config file.
* Main user config file (cluster, pulumi, S3A, DNS, keys etc.) should be TOML format. Example with Sanger defaults checked in to repo. Users make a gitignored copy and adjust for actual usage. One copied config per cluster.
* Validate and reject bad cluster names (eg. has underscore).
* Allow config of separate flavours for master and workers, default to using same for both.
* SSH username should be configurable (no ubuntu hardcode).
* Support using an existing floating IP, auto-allocate if needed.
* Dry run on create/destroy using `--dry-run` flag.
* Suggested CLI subcommands: create, destroy, reboot, build-image, convert-auth, install (see above) plus new status to show cluster info. Destroy should retain interactive terraform-style confirmation. Status should display pulumi stack outputs, service health (ssh checks of spark, hdfs, Jupyter, any other significant services) and simple resource usage.
* Call the new app "hailstack".
* Gnomad is always baked into every Packer image (not optional per-cluster).
* All software (Hadoop, Spark, Hail, Jupyter, Gnomad, Scala, uv, monitoring agents) is pre-baked in the Packer image. Secrets, SSL certificates, nginx config, and cluster-specific configuration are generated at cluster creation time.
* The `install` subcommand always targets all nodes uniformly — no master/worker distinction for post-creation package installation.
* When monitoring (Netdata) is enabled, it is exposed via the nginx reverse proxy with basic auth, preserving the current architecture.
* Python environment strategy: the Packer image includes a base uv-managed environment with the core stack (Hail, PySpark, Jupyter, Gnomad). User-installed Python packages via the `install` subcommand go into an overlay/extension environment layered on top, keeping the base immutable.
* SSH username is configurable and applies to both Packer image building and cluster access.
* If a user specifies version combinations not present in the compatibility matrix, hailstack errors and refuses to proceed. No override mechanism.
* LUKS encryption applies to NFS volumes only. Lustre is a separate unencrypted provider network type.
* Pulumi stack state is stored only in Ceph S3 as the single source of truth for team-shared cluster management. No local state files.
* JupyterLab ships with defaults in the Packer image. Users customise Jupyter (extensions, kernels, config) post-creation via the `install` subcommand, not at image build time.
* The `status` subcommand shows Pulumi stack outputs by default (quick, no SSH). A `--detailed` flag triggers SSH probes to check systemd service statuses (Spark Master, HDFS NameNode, YARN RM, JupyterLab, Netdata if enabled) and per-node resource summaries (CPU%, memory%, disk%).
* There is a single universal Packer image used for both master and worker nodes. Role-specific configuration (services to enable, nginx config, etc.) is applied at cluster deploy time via Pulumi cloud-init.
* Monitoring config is a simple string field in the main TOML config: `monitoring = "netdata"` or `monitoring = "none"`. When enabled, Netdata is installed on all nodes and the dashboard is proxied via nginx with basic auth.
* SSH keys are NOT baked into Packer images. They are injected only at cluster creation time via Pulumi/cloud-init. The SSH username applies to both Packer builds and cluster access.
* Security group ingress rules are fully configurable via a `[security_groups]` block in the main TOML config. The example config ships with defaults matching the current osdataproc rules (SSH, Spark ports, HDFS, HTTP/HTTPS on master; all internal TCP between cluster nodes).
* Users select from named preset bundles in the compatibility matrix (e.g. "hail-0.2.137-gnomad-3.0.4-r2"), not individual component versions. Each bundle defines the full set: Spark, Hadoop, Hail, Java/JDK, Python, Scala, Gnomad. The matrix is repo-locked; changes require PRs to the checked-in TOML file.
* Ceph S3 credentials are stored in a `[ceph_s3]` block in the main TOML config with `endpoint`, `access_key`, `secret_key` fields. Used for both Pulumi state backend and rollout manifest storage.
* The `install` subcommand supports both inline args for quick use (`hailstack install --system pkg1 pkg2 --python pypkg1 pypkg2 [--smoke-test cmd]`) and a `--file packages.toml` flag for repeatable package lists.
* The compatibility matrix TOML uses flat `[bundle."<id>"]` entries with explicit human-friendly IDs following the pattern `hail-<hail_ver>-gnomad-<gnomad_ver>-r<revision>`. Each bundle lists all component versions explicitly. A `[default]` section points to the recommended bundle ID.
* Security groups config uses a simplified `[security_groups.master]` / `[security_groups.worker]` format with named boolean toggles (e.g. `http = true`, `spark_master = true`, `jupyter = true`, `netdata = true`, `ssh = true`, `hdfs = true`).
* Packer images are named after their bundle (e.g. `hailstack-hail-0.2.137-gnomad-3.0.4-r2`). The `create` subcommand auto-discovers the image in OpenStack Glance by name based on the selected bundle. The `build-image` subcommand uploads images with this naming convention.
* The `reboot` subcommand reboots all workers by default, with a `--node` flag to target specific individual worker nodes. No master reboot support.
* The `status` subcommand outputs a human-readable table by default, with a `--json` flag for machine-parseable JSON output.
* The `install` subcommand auto-retries with backoff on per-node failures, then fails and writes partial state (rollout manifest + per-node results) to Ceph S3 so progress is tracked. User re-runs manually to complete.
* Existing clusters with bundles removed from the compatibility matrix can still operate (install, status, reboot, destroy). Only `create` and `build-image` validate against the current matrix.
* On cluster destroy, floating IPs are always released back to the pool.
* The compatibility matrix must support the same Hail+Gnomad version pair with multiple different underlying component versions (e.g. security-patched Spark/Hadoop). This enables both exact reproducibility of previous clusters and security-patched variants. The matrix uses flat bundles under `[bundle."<id>"]` keys with explicit human-friendly IDs that encode the key information. Bundle IDs follow the pattern `hail-<hail_ver>-gnomad-<gnomad_ver>-r<revision>`, e.g. `hail-0.2.137-gnomad-3.0.4-r2`. Each bundle explicitly lists all component versions (hail, spark, hadoop, java, python, scala, gnomad). The `hail` and `gnomad` fields are explicit in the TOML (not derived from the key). The revision suffix (`-r1`, `-r2`, etc.) distinguishes bundles with the same Hail+Gnomad but different infrastructure component versions. The `[default]` section points to the recommended bundle ID. Packer images are named `hailstack-<bundle-id>`. The old hierarchical `[hail."<version>"]` keying is replaced by this flat `[bundle."<id>"]` structure.
