# AI Notes for Hailstack

## Coding Conventions

- Target Python 3.14 and follow the strict typing setup in `pyproject.toml`.
- Use Pydantic v2 models for validated config, rollout, and probe data instead of ad hoc dictionaries when data crosses module boundaries.
- Keep code under `src/hailstack/` and mirror new behavior with pytest coverage under `tests/`.
- Prefer small modules with one responsibility: config parsing in `config/`, infrastructure orchestration in `pulumi/`, packaging in `packer/`, and operational helpers in `ansible/`, `ssh/`, and `storage/`.
- Use the project exception hierarchy from `src/hailstack/errors.py` instead of raising generic exceptions from command code.
- Preserve repository-root asset lookups. Several modules resolve `bundles.toml`, `packer/`, and `ansible/` relative to the checked-in repo.
- For CLI changes, follow existing Typer patterns in `src/hailstack/cli/commands/`: explicit option names, typed `Annotated` parameters, help text on every user-facing option, and command-boundary validation/error reporting.

## Commands

Use these commands as the default local workflow.

```bash
uv sync --group dev
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pyright
uv run pytest tests/ -v
uv run pytest tests/ -v -k <pattern>
```

Additional targeted commands that are often useful here:

```bash
uv run pytest tests/config/ -v
uv run pytest tests/cli/ -v
uv run pytest tests/pulumi/ tests/packer/ -v
uv run pytest tests/test_apptainer_definition.py tests/workflows/ -v
```

## Error Hierarchy

All current error classes from `src/hailstack/errors.py`:

```text
HailstackError
├── ConfigError
├── ValidationError
│   └── BundleNotFoundError
├── NetworkError
├── PackerError
├── PulumiError
├── AnsibleError
├── S3Error
├── SSHError
├── ImageNotFoundError
├── ResourceNotFoundError
└── QuotaExceededError
```

Practical usage:

- `ConfigError` and `ValidationError` cover bad local input and parsing failures.
- `BundleNotFoundError` is specifically for bundle matrix lookup failures.
- `NetworkError`, `ImageNotFoundError`, `ResourceNotFoundError`, and `QuotaExceededError` are used by create-time OpenStack preflight logic.
- `PackerError`, `PulumiError`, `AnsibleError`, `S3Error`, and `SSHError` map cleanly to external tool or remote-operation failures.

## Key Modules

- `src/hailstack/cli/main.py`: top-level Typer app and command registration.
- `src/hailstack/cli/commands/create.py`: config load, bundle resolution, OpenStack preflight, Pulumi preview/apply, and cleanup on failed apply.
- `src/hailstack/cli/commands/build_image.py`: bundle selection plus delegation into the Packer wrapper.
- `src/hailstack/cli/commands/install.py`: package-file parsing, Pulumi stack output resolution, Ansible install execution, retry logic, and rollout manifest upload.
- `src/hailstack/cli/commands/status.py`: summary rendering plus optional SSH health/resource probes.
- `src/hailstack/config/parser.py`, `src/hailstack/config/schema.py`, `src/hailstack/config/compatibility.py`, and `src/hailstack/config/validator.py`: the config pipeline from TOML load to command-time bundle checks.
- `src/hailstack/pulumi/stack.py`, `src/hailstack/pulumi/resources.py`, and `src/hailstack/pulumi/cloud_init.py`: Pulumi Automation API, OpenStack resource graph, and node bootstrap rendering.
- `src/hailstack/packer/builder.py`: validated Packer command construction and image ID extraction.
- `src/hailstack/ansible/runner.py`, `src/hailstack/storage/rollout.py`, and `src/hailstack/ssh/health.py`: post-create installs, rollout persistence, and async SSH health collection.

## Common Tasks

- Add or change CLI behavior: start in `src/hailstack/cli/commands/`, then update the matching tests under `tests/cli/commands/`.
- Change config fields or validation: edit `src/hailstack/config/schema.py` and `src/hailstack/config/parser.py`, then run `uv run pytest tests/config/ -v`.
- Update bundle semantics: edit `bundles.toml`, check `src/hailstack/config/compatibility.py` and `src/hailstack/config/validator.py`, then run the bundle-focused tests.
- Update a Packer script: edit the relevant file under `packer/scripts/`, keep the final version-check command intact, and run `uv run pytest tests/packer/ tests/integration/test_packer_scripts.py -v`.
- Change infrastructure behavior: touch `src/hailstack/pulumi/` or `src/hailstack/packer/`, then validate with the matching `tests/pulumi/` or `tests/packer/` modules.
- Change install or rollout logic: edit `src/hailstack/cli/commands/install.py`, `src/hailstack/ansible/runner.py`, or `src/hailstack/storage/rollout.py`, then run `uv run pytest tests/ansible/ tests/storage/ tests/cli/commands/test_install.py -v`.
- Add a test: place it next to the feature area it covers, prefer fixtures and parametrization for repeated setup, and run the narrowest relevant pytest target before the full suite.
- Change packaging or release behavior: update `Apptainer.def` or `.github/workflows/`, then run `uv run pytest tests/test_apptainer_definition.py tests/workflows/ -v`.