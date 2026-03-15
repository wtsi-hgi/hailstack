# Contributing to Hailstack

## Project Structure

Hailstack is a Python 3.14 CLI project with checked-in infrastructure assets.

- `src/hailstack/cli/` contains the Typer entry point and command implementations for `create`, `destroy`, `reboot`, `build-image`, `install`, `status`, and `convert-auth`.
- `src/hailstack/config/` handles TOML parsing, Pydantic schema validation, and compatibility bundle lookup from `bundles.toml`.
- `src/hailstack/pulumi/` contains Pulumi Automation API helpers, OpenStack resource definitions, and cloud-init rendering.
- `src/hailstack/packer/` contains the Packer wrapper; the actual template and shell provisioners live in `packer/`.
- `src/hailstack/ansible/`, `src/hailstack/storage/`, and `src/hailstack/ssh/` support post-create installs, rollout persistence to Ceph S3, and SSH-based health probes.
- `tests/` mirrors the source tree and also includes workflow and integration coverage.
- `Apptainer.def`, `bundles.toml`, `example-config.toml`, `packer/`, and `ansible/` are runtime assets expected from the repository root.

## Development Setup

Use the repo-local virtual environment and keep the toolchain aligned with CI.

```bash
uv sync --group dev
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pyright
uv run pytest tests/ -v
uv run pytest tests/ -v -k <pattern>
```

Notes:

- CI installs with `uv sync --group dev` on Python 3.14.
- `pyright` is configured in `pyproject.toml` to use `.venv`.
- The CLI expects to run from the repository root so relative assets such as `bundles.toml`, `packer/`, and `ansible/` resolve correctly.

## Testing

These are the canonical local checks. Keep them unchanged in developer-facing docs and scripts unless the toolchain changes.

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pyright
uv run pytest tests/ -v
uv run pytest tests/ -v -k <pattern>
```

Useful targeted test subsets:

- `uv run pytest tests/cli/ -v` for CLI help and command behavior.
- `uv run pytest tests/config/ -v` when editing config parsing, schema, or bundle logic.
- `uv run pytest tests/packer/ tests/pulumi/ -v` when changing infrastructure paths.
- `uv run pytest tests/test_apptainer_definition.py -v` and `uv run pytest tests/workflows/ -v` for packaging and workflow checks.

When writing new tests, keep them alongside the feature they validate: CLI behavior under `tests/cli/commands/`, config parsing in `tests/config/`, infrastructure behavior in `tests/pulumi/` and `tests/packer/`, and operational helpers under `tests/ansible/`, `tests/storage/`, or `tests/ssh/`.

Test conventions in this repo:

- Use pytest throughout.
- Prefer fixtures for reusable config files, fake runners, mocked stack outputs, and environment setup.
- Use parametrization when one behavior must be checked across multiple bundles, commands, or validation cases.
- Keep infrastructure-heavy coverage mocked unless a test is explicitly marked as integration.
- Add assertions at the command boundary, not only on internal helper functions, when behavior is user-visible.

## Building Apptainer Image

The checked-in definition stages the repo into `/opt/hailstack`, installs Pulumi and Packer, installs Ansible with `pip`, and then installs Hailstack as a regular `pip` package install (not in editable mode).

To match the release workflow exactly:

```bash
mkdir -p dist
sudo apptainer build "dist/hailstack-${GITHUB_REF_NAME}.sif" Apptainer.def
```

For local smoke checks you can substitute a fixed output name, for example `dist/hailstack-dev.sif`, but keep the definition and runtime assets in sync with the repository root.

## Adding a New Bundle

Bundles are defined in `bundles.toml` as explicit IDs under `[bundle."<id>"]`, with `[default].bundle` selecting the default bundle.

When adding a bundle:

1. Add a new entry in `bundles.toml` with `hail`, `spark`, `hadoop`, `java`, `python`, `scala`, `gnomad`, and `status`.
2. If the bundle should become the default, update `[default].bundle` to the new ID.
3. Run `hailstack build-image --config <config.toml> --bundle <bundle-id>` so the corresponding `hailstack-<bundle-id>` image exists in OpenStack Glance.
4. Run the compatibility and CLI validation tests before merging.
5. Submit a PR that includes the bundle change, any related image-build or provisioning updates, and the test results.

Recommended checks after editing `bundles.toml`:

```bash
uv run pytest tests/config/test_compatibility.py -v
uv run pytest tests/cli/test_bundle_validation.py -v
uv run pytest tests/cli/commands/test_build_image.py -v
```

## Architecture Decisions

- Pulumi is used instead of Terraform because the project needs Python-native orchestration, direct reuse of the validated Pydantic config objects, and testable lifecycle logic inside the same codebase as the CLI rather than a separate HCL layer.
- Bundle validation is command-time for `create` and `build-image`, not a generic rule for every command. That keeps day-2 operations working against older clusters even when a bundle is removed later.
- Pulumi state is stored only in Ceph S3. Commands set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the parsed config before invoking Pulumi.
- Cluster images are intentionally fat and pre-baked with Packer so create-time provisioning stays short and deterministic; cloud-init and the install playbook then handle node-specific configuration and post-create software rollout.
- The install workflow treats `/opt/hailstack/base-venv` as immutable and installs extra Python packages into `/opt/hailstack/overlay-venv`.
- The repository root is part of the runtime contract: the CLI, Apptainer image, and tests all assume `bundles.toml`, `packer/`, and `ansible/` exist at predictable root-relative paths.