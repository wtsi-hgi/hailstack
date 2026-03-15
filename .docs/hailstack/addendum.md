# Hailstack Addendum

## Status

This document is a normative supplement to [spec.md](./spec.md).

Where this addendum is more specific than the main spec, this addendum
controls. Its purpose is to define requirements that must remain stable
for operational safety, compatibility, and supportability.

## 1. Deployed State Authority

Lifecycle commands must treat Pulumi Automation API stack outputs as the
authoritative deployed-state interface.

- Commands such as `status`, `install`, `reboot`, and `destroy` must read
  current deployed values from stack outputs when those values are
  available.
- Direct shell-oriented interfaces such as `pulumi stack output --stack
  ...` are not the normative command integration surface.
- If deployed stack outputs conflict with drifted local assumptions,
  deployed outputs take precedence for command behavior.

This requirement applies in particular to master IP resolution, attached
volume identity, and other update-sensitive resource metadata.

## 2. Managed Volume Requirements

Managed volume behavior is defined as follows:

- A managed volume must be identified by its attached volume ID or other
  explicit attachment identity, not by positional disk ordering.
- Master bootstrap and cloud-init logic must resolve the managed volume by
  stable attachment identity, including serial or attached ID, rather than
  by selecting the first non-root disk.
- Volume attachment must retain the explicit device input `/dev/vdb` for
  compatibility with existing stacks.
- Managed volumes may grow but must not shrink.
- A shrink request must fail during create preflight with a clear error,
  rather than being deferred to OpenStack failure behavior.
- Destroy-time state rehydration must preserve sufficient information to
  apply correct keep-or-delete behavior for managed volumes.

## 3. Floating IP Reuse Requirements

Floating IP behavior is defined as follows:

- A user-supplied floating IP must be modeled as a reused address that is
  associated with the instance, not as a newly allocated floating IP.
- Resource modeling for reused floating IPs must preserve compatibility
  with previously created stacks.
- Destroy-time state rehydration must preserve enough deployed-state
  information to apply the configured floating-IP retention behavior.

## 4. Monitoring And Service Exposure Requirements

Monitoring and service exposure behavior is defined as follows:

- `monitoring = "none"` must disable Netdata configuration.
- `monitoring = "none"` must also disable default public exposure of the
  Netdata port.
- Detailed `status` must recognize both the legacy
  `hailstack-jupyterlab` service name and the `jupyter-lab` unit while
  presenting one logical Jupyter service view.
- Install-time service management must disable the legacy Jupyter unit and
  enable the supported Jupyter service so the configured service survives
  reboot.

## 5. Retry And Timeout Requirements

Network and SSH handling is defined as follows:

- Transient OpenStack CLI failures during create preflight must retry
  using the project backoff policy of `1`, `2`, and `4` seconds.
- Transient SSH transport failures in health probes must retry using the
  same retry policy.
- SSH-based health and status commands must enforce a bounded command
  timeout so command execution cannot hang indefinitely.
- Reboot-time SSH dispatch and output collection must retry transport
  failures while surfacing non-transport failures directly.
- Transient endpoint, catalog, and similar service-discovery failures must
  not be misclassified as permanent missing-resource failures before the
  retry budget is exhausted.

## 6. CLI Progress And Help Requirements

Command user interface behavior is defined as follows:

- Commands that perform multi-stage work must emit stage-oriented progress
  logging.
- `reboot` must provide complete help text for its supported options.
- `convert-auth` must emit explicit progress logging.
- Command logger setup must remain correct across repeated invocations and
  must not retain stale `stderr` handlers between runs.

## 7. Dry-Run Backend Requirements

Pulumi backend behavior is defined as follows:

- `create` and `create --dry-run` both require the configured Pulumi
  backend credentials.
- Missing or invalid backend credentials must fail before preview or
  apply.
- There is no supported degraded local dry-run mode that bypasses the
  configured backend.

## 8. Auth Conversion Security Requirements

`convert-auth` write behavior is defined as follows:

- `hailstack convert-auth --write` must write `clouds.yaml` with mode
  `0600`.
- Backup files created while replacing an existing `clouds.yaml` must also
  be written with mode `0600`.
- When `--write` is used, the command must not echo the generated
  credential document to stdout.

## 9. Runtime Packaging Requirements

Runtime packaging behavior is defined as follows:

- CLI startup must not require eager creation of runtime workspace
  directories at import time.
- Runtime directory creation must occur lazily so the installed CLI
  remains usable in environments with restricted or non-writable `$HOME`
  states.
- The Apptainer image must install Pulumi into a path that is usable by
  non-root runtime users.
- Container and contributor documentation must describe the actual
  packaging model in use.

## 10. CI Toolchain Requirements

Continuous integration requirements are defined as follows:

- CI must install Packer in the `test` job so Packer-backed validation is
  exercised in automated testing.
- Packer setup in CI must be pinned to a stable action revision and must
  not depend on a moving branch ref.
- Local tests that require the `packer` binary may skip when that binary
  is absent, but CI must be configured so those validation paths run.

## 11. Documentation Requirements

Documentation is part of the supported product surface.

- User-facing documentation must describe actual CLI flags, behavior, and
  security-sensitive usage.
- Contributor documentation must match the real packaging and build
  model.
- Documentation requirements that materially affect user or contributor
  behavior should be protected by tests.

## 12. Revision Guidance

Any future revision of [spec.md](./spec.md) should fold the following
requirements into the main text rather than leaving them only here:

- deployed state authority via Automation API outputs
- identity-based managed volume handling with no shrink support
- association semantics for reused floating IPs
- monitoring disablement implying no default public Netdata exposure
- mandatory retry and timeout behavior for transient network and SSH paths
- secure `convert-auth --write` behavior with no stdout secret echo
- non-root-safe Apptainer runtime packaging
- CI coverage for critical external-tool paths such as Packer