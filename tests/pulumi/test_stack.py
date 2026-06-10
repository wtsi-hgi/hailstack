# Copyright (c) 2026 Genome Research Ltd.
#
# Author: Sendu Bala <sb10@sanger.ac.uk>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Acceptance tests for Pulumi automation stack selection semantics."""

import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from pulumi import automation as auto
from pulumi.automation._stack import StackInitMode
from semver import VersionInfo

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import PulumiError, S3Error
from hailstack.pulumi import stack as stack_module


class FakeAutoStack:
    """Capture preview and destroy interactions from the runner."""

    def __init__(self) -> None:
        """Initialise counters for the fake stack."""
        self.preview_calls = 0
        self.preview_destroy_calls = 0
        self.destroy_calls = 0
        self.up_calls = 0
        self.output_values: dict[str, object] = {}

    def preview(self, *, on_output: object) -> object:
        """Return a fake preview result."""
        del on_output
        self.preview_calls += 1
        return SimpleNamespace(stdout="preview\n")

    def preview_destroy(self, *, on_output: object) -> object:
        """Return a fake destroy preview result."""
        del on_output
        self.preview_destroy_calls += 1
        return SimpleNamespace(stdout="preview destroy\n")

    def destroy(self, *, remove: bool = False) -> None:
        """Record destroy requests from the runner."""
        assert remove is True
        self.destroy_calls += 1

    def up(self, *, on_output: object) -> object:
        """Return a fake create result with the required master IP output."""
        del on_output
        self.up_calls += 1
        return SimpleNamespace(
            stdout="created\n",
            outputs={
                "master_public_ip": SimpleNamespace(value="198.51.100.20"),
            },
        )

    def outputs(self) -> dict[str, object]:
        """Return fake stack outputs."""
        return {
            name: SimpleNamespace(value=value)
            for name, value in self.output_values.items()
        }


class RecordingPulumiCommand(stack_module._ResolvedPulumiCommand):
    """Record real Automation API command invocations without running Pulumi."""

    def __init__(self) -> None:
        """Initialise the command with the supported fake CLI version."""
        super().__init__(
            stack_module.PulumiCli(
                path=Path("/opt/hailstack/bin/pulumi"),
                version=stack_module.KNOWN_GOOD_PULUMI_CLI_VERSION,
            )
        )
        self.calls: list[list[str]] = []

    def run(
        self,
        args: list[str],
        cwd: str,
        additional_env: Mapping[str, str],
        on_output: auto.OnOutput | None = None,
        on_error: auto.OnOutput | None = None,
    ) -> auto.CommandResult:
        """Record the Pulumi command and return a minimal successful result."""
        del cwd, additional_env, on_output, on_error
        self.calls.append(list(args))
        if args[:3] == ["stack", "history", "--json"]:
            return auto.CommandResult(stdout="[]", stderr="", code=0)
        return auto.CommandResult(stdout="", stderr="", code=0)


def _config(*, endpoint: str = "https://ceph.example.invalid") -> ClusterConfig:
    """Return the subset of config the runner needs for tests."""
    return cast(
        ClusterConfig,
        SimpleNamespace(
            cluster=SimpleNamespace(name="test-cluster"),
            ceph_s3=SimpleNamespace(
                bucket="hailstack-state",
                endpoint=endpoint,
                access_key="state-access",
                secret_key="state-secret",
            ),
            volumes=SimpleNamespace(preserve_on_destroy=False),
        ),
    )


def _fake_pulumi_version_run(
    expected_path: Path,
    *,
    version: str = "v3.226.0",
) -> Callable[..., object]:
    """Return a fake subprocess.run that answers Pulumi version checks."""

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object | None = None,
        env: dict[str, str] | None = None,
        text: bool,
    ) -> object:
        del cwd, env
        assert capture_output is True
        assert check is False
        assert text is True
        assert args == [str(expected_path), "version"]
        return SimpleNamespace(returncode=0, stderr="", stdout=f"{version}\n")

    return fake_run


def _write_fake_home_pulumi(tmp_path: Path) -> Path:
    """Create a fake ~/.pulumi/bin/pulumi executable path for tests."""
    pulumi_path = tmp_path / "home" / ".pulumi" / "bin" / "pulumi"
    pulumi_path.parent.mkdir(parents=True)
    pulumi_path.write_text("#!/bin/sh\n", encoding="utf-8")
    pulumi_path.chmod(0o700)
    return pulumi_path


def _use_supported_pulumi_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make tests that mock Pulumi stacks use a supported resolved CLI."""

    def fake_resolve_pulumi_cli(
        self: stack_module.AutomationStackRunner,
    ) -> stack_module.PulumiCli:
        del self
        return stack_module.PulumiCli(
            path=Path("/opt/hailstack/bin/pulumi"),
            version=stack_module.KNOWN_GOOD_PULUMI_CLI_VERSION,
        )

    monkeypatch.setattr(
        stack_module.AutomationStackRunner,
        "_resolve_pulumi_cli",
        fake_resolve_pulumi_cli,
    )


def _real_stack_with_recording_command(
    tmp_path: Path,
    *,
    program: Callable[[], None] | None = None,
) -> tuple[auto.Stack, RecordingPulumiCommand]:
    """Return a real Automation API stack backed by a recording command."""
    command = RecordingPulumiCommand()
    workspace = auto.LocalWorkspace(
        work_dir=str(tmp_path),
        program=program,
        pulumi_command=command,
    )
    stack = auto.Stack("hailstack-test-cluster", workspace, StackInitMode.SELECT)
    command.calls.clear()
    return stack, command


def _capture_new_stack_preview_env(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, str]:
    """Return the Pulumi env passed to a first-time stack preview."""
    _use_supported_pulumi_cli(monkeypatch)
    fake_stack = FakeAutoStack()
    captured_envs: list[dict[str, str]] = []

    def fake_create_stack(**kwargs: object) -> FakeAutoStack:
        workspace_options = cast(auto.LocalWorkspaceOptions, kwargs["opts"])
        captured_envs.append(cast(dict[str, str], workspace_options.env_vars))
        return fake_stack

    monkeypatch.setattr(stack_module.auto, "create_stack", fake_create_stack)

    stack_module.AutomationStackRunner().preview(
        _config(),
        cast(Bundle, SimpleNamespace(id="bundle-id")),
        stack_exists=False,
    )

    assert len(captured_envs) == 1
    return captured_envs[0]


def _capture_persisted_create_env(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, str]:
    """Return the Pulumi env passed to a first-time persisted create."""
    _use_supported_pulumi_cli(monkeypatch)
    fake_stack = FakeAutoStack()
    captured_envs: list[dict[str, str]] = []

    def fake_create_or_select_stack(**kwargs: object) -> FakeAutoStack:
        workspace_options = cast(auto.LocalWorkspaceOptions, kwargs["opts"])
        captured_envs.append(cast(dict[str, str], workspace_options.env_vars))
        return fake_stack

    monkeypatch.setattr(
        stack_module.auto,
        "create_or_select_stack",
        fake_create_or_select_stack,
    )

    result = stack_module.AutomationStackRunner().up(
        _config(),
        cast(Bundle, SimpleNamespace(id="bundle-id")),
    )

    assert result.master_public_ip == "198.51.100.20"
    assert fake_stack.up_calls == 1
    assert len(captured_envs) == 1
    return captured_envs[0]


def test_preview_destroy_selects_existing_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use select-stack for destroy previews so dry runs do not create state."""
    _use_supported_pulumi_cli(monkeypatch)
    fake_stack = FakeAutoStack()
    calls: list[str] = []

    def fake_select_stack(**kwargs: object) -> FakeAutoStack:
        del kwargs
        calls.append("select")
        return fake_stack

    def fake_create_or_select_stack(**kwargs: object) -> FakeAutoStack:
        del kwargs
        calls.append("create_or_select")
        return fake_stack

    monkeypatch.setattr(stack_module.auto, "select_stack", fake_select_stack)
    monkeypatch.setattr(
        stack_module.auto,
        "create_or_select_stack",
        fake_create_or_select_stack,
    )

    result = stack_module.AutomationStackRunner().preview_destroy(_config())

    assert result == "preview destroy\n"
    assert calls == ["select"]
    assert fake_stack.preview_calls == 0
    assert fake_stack.preview_destroy_calls == 1


def test_preview_new_stack_allows_missing_runtime_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use placeholder runtime secrets when previewing a first create locally."""
    _use_supported_pulumi_cli(monkeypatch)
    fake_stack = FakeAutoStack()
    recorded_allow_missing_runtime_secrets: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        image_id: str | None = None,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
    ) -> None:
        del config, bundle, image_id, retain_created_volume
        recorded_allow_missing_runtime_secrets.append(allow_missing_runtime_secrets)

    def fake_create_stack(**kwargs: object) -> FakeAutoStack:
        program = kwargs.get("program")
        if program is not None:
            captured_program.append(cast(Callable[[], None], program))
        return fake_stack

    monkeypatch.setattr(
        stack_module,
        "create_cluster_resources",
        fake_create_cluster_resources,
    )
    monkeypatch.setattr(stack_module.auto, "create_stack", fake_create_stack)

    result = stack_module.AutomationStackRunner().preview(
        _config(),
        cast(Bundle, SimpleNamespace(id="bundle-id")),
        stack_exists=False,
    )

    assert result == "preview\n"
    assert len(captured_program) == 1
    captured_program[0]()
    assert recorded_allow_missing_runtime_secrets == [True]


def test_preview_new_stack_defaults_passphrase_for_ephemeral_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preview first-time creates non-interactively with the local backend."""
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE", raising=False)
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE_FILE", raising=False)

    env = _capture_new_stack_preview_env(monkeypatch)

    assert env["PULUMI_CONFIG_PASSPHRASE"] == _config().ceph_s3.secret_key
    assert "PULUMI_CONFIG_PASSPHRASE_FILE" not in env


def test_persisted_create_defaults_passphrase_to_state_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create first-time persisted stacks without requiring manual Pulumi setup."""
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE", raising=False)
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE_FILE", raising=False)

    env = _capture_persisted_create_env(monkeypatch)

    assert env["PULUMI_CONFIG_PASSPHRASE"] == _config().ceph_s3.secret_key
    assert "PULUMI_CONFIG_PASSPHRASE_FILE" not in env


def test_preview_new_stack_preserves_explicit_passphrase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect caller-provided Pulumi passphrases for first-time previews."""
    monkeypatch.setenv("PULUMI_CONFIG_PASSPHRASE", "caller-passphrase")
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE_FILE", raising=False)

    env = _capture_new_stack_preview_env(monkeypatch)

    assert env["PULUMI_CONFIG_PASSPHRASE"] == "caller-passphrase"


def test_preview_new_stack_preserves_explicit_passphrase_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect caller-provided Pulumi passphrase files for first-time previews."""
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE", raising=False)
    monkeypatch.setenv("PULUMI_CONFIG_PASSPHRASE_FILE", "/tmp/caller-passphrase")

    env = _capture_new_stack_preview_env(monkeypatch)

    assert env["PULUMI_CONFIG_PASSPHRASE_FILE"] == "/tmp/caller-passphrase"
    assert "PULUMI_CONFIG_PASSPHRASE" not in env


def test_destroy_raises_clear_error_when_stack_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail destroy with a clear missing-stack error instead of a false success."""
    _use_supported_pulumi_cli(monkeypatch)

    def fake_select_stack(**kwargs: object) -> FakeAutoStack:
        del kwargs
        raise RuntimeError("no stack named hailstack-test-cluster")

    monkeypatch.setattr(stack_module.auto, "select_stack", fake_select_stack)

    with pytest.raises(PulumiError, match="does not exist"):
        stack_module.AutomationStackRunner().destroy(_config())


def test_cleanup_failed_create_disables_volume_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use a non-retaining Pulumi program for failed first-time create cleanup."""
    _use_supported_pulumi_cli(monkeypatch)
    fake_stack = FakeAutoStack()
    recorded_retain_created_volume: list[bool | None] = []
    recorded_allow_missing_runtime_secrets: list[bool] = []
    recorded_allow_missing_ssh_public_keys: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        image_id: str | None = None,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
        allow_missing_ssh_public_keys: bool = False,
    ) -> None:
        del config, bundle, image_id
        recorded_retain_created_volume.append(retain_created_volume)
        recorded_allow_missing_runtime_secrets.append(allow_missing_runtime_secrets)
        recorded_allow_missing_ssh_public_keys.append(allow_missing_ssh_public_keys)

    def fake_select_stack(**kwargs: object) -> FakeAutoStack:
        program = kwargs.get("program")
        if program is not None:
            captured_program.append(cast(Callable[[], None], program))
        return fake_stack

    monkeypatch.setattr(
        stack_module,
        "create_cluster_resources",
        fake_create_cluster_resources,
    )
    monkeypatch.setattr(stack_module.auto, "select_stack", fake_select_stack)

    runner = stack_module.AutomationStackRunner()
    runner.cleanup_failed_create(
        _config(),
        cast(Bundle, SimpleNamespace(id="bundle-id")),
    )

    assert len(captured_program) == 1
    captured_program[0]()
    assert recorded_retain_created_volume == [False]
    assert recorded_allow_missing_runtime_secrets == [True]
    assert recorded_allow_missing_ssh_public_keys == [True]
    assert fake_stack.destroy_calls == 1


def test_destroy_uses_current_config_for_volume_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rebuild the Pulumi program so explicit destroy honors preserve_on_destroy."""
    _use_supported_pulumi_cli(monkeypatch)
    fake_stack = FakeAutoStack()
    recorded_retain_created_volume: list[bool | None] = []
    recorded_allow_missing_runtime_secrets: list[bool] = []
    recorded_allow_missing_ssh_public_keys: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        image_id: str | None = None,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
        allow_missing_ssh_public_keys: bool = False,
    ) -> None:
        del config, bundle, image_id
        recorded_retain_created_volume.append(retain_created_volume)
        recorded_allow_missing_runtime_secrets.append(allow_missing_runtime_secrets)
        recorded_allow_missing_ssh_public_keys.append(allow_missing_ssh_public_keys)

    def fake_select_stack(**kwargs: object) -> FakeAutoStack:
        program = kwargs.get("program")
        if program is not None:
            captured_program.append(cast(Callable[[], None], program))
        return fake_stack

    config = cast(
        ClusterConfig,
        SimpleNamespace(
            cluster=SimpleNamespace(name="test-cluster", bundle="bundle-id"),
            ceph_s3=SimpleNamespace(
                bucket="hailstack-state",
                endpoint="https://ceph.example.invalid",
                access_key="state-access",
                secret_key="state-secret",
            ),
            volumes=SimpleNamespace(preserve_on_destroy=True),
        ),
    )

    monkeypatch.setattr(
        stack_module,
        "create_cluster_resources",
        fake_create_cluster_resources,
    )
    monkeypatch.setattr(stack_module.auto, "select_stack", fake_select_stack)

    runner = stack_module.AutomationStackRunner()
    runner.destroy(config)

    assert len(captured_program) == 1
    captured_program[0]()
    assert recorded_retain_created_volume == [True]
    assert recorded_allow_missing_runtime_secrets == [True]
    assert recorded_allow_missing_ssh_public_keys == [True]
    assert fake_stack.destroy_calls == 1


def test_destroy_rehydrates_program_for_existing_floating_ip_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rebuild the Pulumi program so explicit destroy keeps a user-supplied IP."""
    _use_supported_pulumi_cli(monkeypatch)
    fake_stack = FakeAutoStack()
    recorded_retain_created_volume: list[bool | None] = []
    recorded_allow_missing_runtime_secrets: list[bool] = []
    recorded_allow_missing_ssh_public_keys: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        image_id: str | None = None,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
        allow_missing_ssh_public_keys: bool = False,
    ) -> None:
        del config, bundle, image_id
        recorded_retain_created_volume.append(retain_created_volume)
        recorded_allow_missing_runtime_secrets.append(allow_missing_runtime_secrets)
        recorded_allow_missing_ssh_public_keys.append(allow_missing_ssh_public_keys)

    def fake_select_stack(**kwargs: object) -> FakeAutoStack:
        program = kwargs.get("program")
        if program is not None:
            captured_program.append(cast(Callable[[], None], program))
        return fake_stack

    config = cast(
        ClusterConfig,
        SimpleNamespace(
            cluster=SimpleNamespace(
                name="test-cluster",
                bundle="bundle-id",
                floating_ip="198.51.100.10",
            ),
            ceph_s3=SimpleNamespace(
                bucket="hailstack-state",
                endpoint="https://ceph.example.invalid",
                access_key="state-access",
                secret_key="state-secret",
            ),
            volumes=SimpleNamespace(preserve_on_destroy=False),
        ),
    )

    monkeypatch.setattr(
        stack_module,
        "create_cluster_resources",
        fake_create_cluster_resources,
    )
    monkeypatch.setattr(stack_module.auto, "select_stack", fake_select_stack)

    runner = stack_module.AutomationStackRunner()
    runner.destroy(config)

    assert len(captured_program) == 1
    captured_program[0]()
    assert recorded_retain_created_volume == [None]
    assert recorded_allow_missing_runtime_secrets == [True]
    assert recorded_allow_missing_ssh_public_keys == [True]
    assert fake_stack.destroy_calls == 1


def test_pulumi_env_defaults_to_workspace_scoped_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use an isolated Pulumi home unless the caller already set one."""
    monkeypatch.delenv("PULUMI_HOME", raising=False)

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["PULUMI_HOME"].startswith(
        f"{tempfile.gettempdir()}/hailstack-pulumi-home/"
    )


def test_pulumi_env_preserves_explicit_pulumi_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect an explicit Pulumi home instead of overwriting it."""
    monkeypatch.setenv("PULUMI_HOME", "/tmp/custom-pulumi-home")

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["PULUMI_HOME"] == "/tmp/custom-pulumi-home"


def test_pulumi_env_defaults_passphrase_to_state_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the state secret as a stable default Pulumi passphrase."""
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE", raising=False)
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE_FILE", raising=False)

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["PULUMI_CONFIG_PASSPHRASE"] == _config().ceph_s3.secret_key
    assert "PULUMI_CONFIG_PASSPHRASE_FILE" not in env


def test_pulumi_env_preserves_explicit_passphrase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect caller-provided Pulumi passphrases for persisted stacks."""
    monkeypatch.setenv("PULUMI_CONFIG_PASSPHRASE", "caller-passphrase")
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE_FILE", raising=False)

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["PULUMI_CONFIG_PASSPHRASE"] == "caller-passphrase"


def test_pulumi_env_preserves_explicit_passphrase_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect caller-provided Pulumi passphrase files for persisted stacks."""
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE", raising=False)
    monkeypatch.setenv("PULUMI_CONFIG_PASSPHRASE_FILE", "/tmp/caller-passphrase")

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["PULUMI_CONFIG_PASSPHRASE_FILE"] == "/tmp/caller-passphrase"
    assert "PULUMI_CONFIG_PASSPHRASE" not in env


def test_pulumi_env_defaults_checksum_compatibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default S3 checksum behavior to Ceph-compatible AWS SDK settings."""
    monkeypatch.delenv("AWS_REQUEST_CHECKSUM_CALCULATION", raising=False)
    monkeypatch.delenv("AWS_RESPONSE_CHECKSUM_VALIDATION", raising=False)

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["AWS_REQUEST_CHECKSUM_CALCULATION"] == "when_required"
    assert env["AWS_RESPONSE_CHECKSUM_VALIDATION"] == "when_required"


def test_pulumi_env_preserves_explicit_checksum_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect caller-provided AWS SDK checksum behavior."""
    monkeypatch.setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_SUPPORTED")
    monkeypatch.setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "WHEN_SUPPORTED")

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["AWS_REQUEST_CHECKSUM_CALCULATION"] == "WHEN_SUPPORTED"
    assert env["AWS_RESPONSE_CHECKSUM_VALIDATION"] == "WHEN_SUPPORTED"


def test_backend_access_normalizes_bare_ceph_endpoint_and_defaults_region(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Login to documented bare Ceph endpoints with a valid S3 region."""
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    pulumi_path = _write_fake_home_pulumi(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("PATH", str(tmp_path / "venv" / "bin"))
    captured_args: list[list[str]] = []
    captured_envs: list[dict[str, str]] = []

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del capture_output, check, cwd, text
        captured_args.append(args)
        if args == [str(pulumi_path), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout="v3.226.0\n")
        captured_envs.append(env)
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)

    stack_module.AutomationStackRunner().check_backend_access(
        _config(endpoint="cog.sanger.ac.uk")
    )

    assert captured_args == [
        [str(pulumi_path), "version"],
        [
            str(pulumi_path),
            "login",
            "--non-interactive",
            "s3://hailstack-state?endpoint=https://cog.sanger.ac.uk",
        ],
    ]
    assert captured_envs[0]["AWS_REGION"] == "us-east-1"
    assert captured_envs[0]["AWS_DEFAULT_REGION"] == "us-east-1"
    assert captured_envs[0]["AWS_REQUEST_CHECKSUM_CALCULATION"] == "when_required"
    assert captured_envs[0]["AWS_RESPONSE_CHECKSUM_VALIDATION"] == "when_required"


def test_backend_access_uses_supported_home_pulumi_when_not_on_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Find the supported ~/.pulumi CLI even when only the venv is on PATH."""
    pulumi_path = _write_fake_home_pulumi(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("PATH", str(tmp_path / "venv" / "bin"))
    captured_args: list[list[str]] = []

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del capture_output, check, cwd, text
        captured_args.append(args)
        if args == [str(pulumi_path), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout="v3.226.0\n")
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)

    stack_module.AutomationStackRunner().check_backend_access(_config())

    assert captured_args == [
        [str(pulumi_path), "version"],
        [
            str(pulumi_path),
            "login",
            "--non-interactive",
            "s3://hailstack-state?endpoint=https://ceph.example.invalid",
        ],
    ]


def test_persisted_create_uses_resolved_pulumi_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Run automation operations with the same resolved Pulumi binary."""
    fake_stack = FakeAutoStack()
    pulumi_path = _write_fake_home_pulumi(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("PATH", str(tmp_path / "venv" / "bin"))
    monkeypatch.setattr(
        stack_module.subprocess,
        "run",
        _fake_pulumi_version_run(pulumi_path),
    )
    captured_commands: list[str] = []

    def fake_create_or_select_stack(**kwargs: object) -> FakeAutoStack:
        workspace_options = cast(auto.LocalWorkspaceOptions, kwargs["opts"])
        pulumi_command = workspace_options.pulumi_command
        assert pulumi_command is not None
        captured_commands.append(pulumi_command.command)
        return fake_stack

    monkeypatch.setattr(
        stack_module.auto,
        "create_or_select_stack",
        fake_create_or_select_stack,
    )

    result = stack_module.AutomationStackRunner().up(
        _config(),
        cast(Bundle, SimpleNamespace(id="bundle-id")),
    )

    assert result.master_public_ip == "198.51.100.20"
    assert captured_commands == [str(pulumi_path)]


def test_backend_access_prefers_path_pulumi_when_home_also_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Respect the user's PATH-selected Pulumi CLI before home fallback."""
    path_pulumi = tmp_path / "tools" / "bin" / "pulumi"
    path_pulumi.parent.mkdir(parents=True)
    path_pulumi.write_text("#!/bin/sh\n", encoding="utf-8")
    path_pulumi.chmod(0o700)
    home_pulumi = _write_fake_home_pulumi(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("PATH", str(path_pulumi.parent))
    captured_args: list[list[str]] = []

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del capture_output, check, cwd, env, text
        captured_args.append(args)
        if args == [str(path_pulumi), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout="v3.245.0\n")
        if args == [str(home_pulumi), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout="v3.226.0\n")
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)

    stack_module.AutomationStackRunner().check_backend_access(_config())

    assert captured_args == [
        [str(path_pulumi), "version"],
        [
            str(path_pulumi),
            "login",
            "--non-interactive",
            "s3://hailstack-state?endpoint=https://ceph.example.invalid",
        ],
    ]


def test_real_stack_preview_reaches_resolved_pulumi_command(
    tmp_path: Path,
) -> None:
    """Populate command.version so preview reaches Pulumi execution."""
    stack, command = _real_stack_with_recording_command(tmp_path)

    with pytest.raises(RuntimeError, match="summary event never found"):
        stack.preview()

    assert isinstance(command.version, VersionInfo)
    assert command.calls[0][0] == "preview"


def test_real_stack_preview_destroy_reaches_resolved_pulumi_command(
    tmp_path: Path,
) -> None:
    """Populate command.version so destroy previews reach Pulumi execution."""
    stack, command = _real_stack_with_recording_command(tmp_path)

    with pytest.raises(RuntimeError, match="summary event never found"):
        stack.preview_destroy()

    assert isinstance(command.version, VersionInfo)
    assert command.calls[0][:2] == ["destroy", "--preview-only"]


def test_real_stack_destroy_with_inline_program_reaches_resolved_pulumi_command(
    tmp_path: Path,
) -> None:
    """Populate command.version so inline destroy reaches Pulumi execution."""

    def pulumi_program() -> None:
        return

    stack, command = _real_stack_with_recording_command(
        tmp_path,
        program=pulumi_program,
    )

    with pytest.raises(AssertionError):
        stack.destroy()

    assert isinstance(command.version, VersionInfo)
    assert command.calls[0][0] == "destroy"


def test_resolved_pulumi_command_rejects_too_old_cli() -> None:
    """Fail clearly when Pulumi is older than Automation API supports."""
    with pytest.raises(PulumiError) as exc_info:
        stack_module._ResolvedPulumiCommand(
            stack_module.PulumiCli(
                path=Path("/opt/hailstack/bin/pulumi"),
                version="3.0.0",
            )
        )

    message = str(exc_info.value)
    assert "Pulumi Automation API requires Pulumi CLI 3.1.0 or newer" in message
    assert "/opt/hailstack/bin/pulumi (v3.0.0)" in message


def test_backend_access_allows_newer_cli_after_successful_login(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Let newer Pulumi CLIs proceed when checksum compatibility works."""
    pulumi_path = tmp_path / "tools" / "bin" / "pulumi"
    pulumi_path.parent.mkdir(parents=True)
    pulumi_path.write_text("#!/bin/sh\n", encoding="utf-8")
    pulumi_path.chmod(0o700)
    monkeypatch.setenv("PATH", str(pulumi_path.parent))
    captured_args: list[list[str]] = []
    captured_envs: list[dict[str, str]] = []

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del capture_output, check, cwd, text
        captured_args.append(args)
        if args == [str(pulumi_path), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout="v3.245.0\n")
        captured_envs.append(env)
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)

    stack_module.AutomationStackRunner().check_backend_access(_config())

    assert captured_args == [
        [str(pulumi_path), "version"],
        [
            str(pulumi_path),
            "login",
            "--non-interactive",
            "s3://hailstack-state?endpoint=https://ceph.example.invalid",
        ],
    ]
    assert captured_envs[0]["AWS_REQUEST_CHECKSUM_CALCULATION"] == "when_required"
    assert captured_envs[0]["AWS_RESPONSE_CHECKSUM_VALIDATION"] == "when_required"


@pytest.mark.parametrize("version_output", ["", "definitely-not-semver"])
def test_backend_access_rejects_unparseable_cli_version_before_automation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    version_output: str,
) -> None:
    """Fail clearly after login when the selected CLI version is not parseable."""
    pulumi_path = tmp_path / "tools" / "bin" / "pulumi"
    pulumi_path.parent.mkdir(parents=True)
    pulumi_path.write_text("#!/bin/sh\n", encoding="utf-8")
    pulumi_path.chmod(0o700)
    monkeypatch.setenv("PATH", str(pulumi_path.parent))
    captured_args: list[list[str]] = []
    automation_versions: list[VersionInfo | None] = []

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del capture_output, check, cwd, env, text
        captured_args.append(args)
        if args == [str(pulumi_path), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout=version_output)
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    def fake_create_or_select_stack(**kwargs: object) -> FakeAutoStack:
        workspace_options = cast(auto.LocalWorkspaceOptions, kwargs["opts"])
        pulumi_command = workspace_options.pulumi_command
        assert pulumi_command is not None
        automation_versions.append(pulumi_command.version)
        return FakeAutoStack()

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        stack_module.auto,
        "create_or_select_stack",
        fake_create_or_select_stack,
    )
    runner = stack_module.AutomationStackRunner()

    with pytest.raises(PulumiError) as exc_info:
        runner.check_backend_access(_config())
        runner.up(_config(), cast(Bundle, SimpleNamespace(id="bundle-id")))

    message = str(exc_info.value)
    assert "could not determine a parseable Pulumi CLI version" in message
    assert str(pulumi_path) in message
    assert captured_args == [
        [str(pulumi_path), "version"],
        [
            str(pulumi_path),
            "login",
            "--non-interactive",
            "s3://hailstack-state?endpoint=https://ceph.example.invalid",
        ],
    ]
    assert automation_versions == []


def test_backend_access_checksum_mismatch_with_newer_cli_hints_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Explain Ceph checksum mismatches after compatibility defaults are set."""
    pulumi_path = tmp_path / "tools" / "bin" / "pulumi"
    pulumi_path.parent.mkdir(parents=True)
    pulumi_path.write_text("#!/bin/sh\n", encoding="utf-8")
    pulumi_path.chmod(0o700)
    monkeypatch.setenv("PATH", str(pulumi_path.parent))

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del capture_output, check, cwd, env, text
        if args == [str(pulumi_path), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout="v3.245.0\n")
        return SimpleNamespace(
            returncode=1,
            stderr=(
                "error: failed to write .pulumi/meta.yaml: PutObject: "
                "XAmzContentSHA256Mismatch"
            ),
            stdout="",
        )

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)

    with pytest.raises(S3Error) as exc_info:
        stack_module.AutomationStackRunner().check_backend_access(
            _config(endpoint="https://cog.sanger.ac.uk/")
        )

    message = str(exc_info.value)
    assert "XAmzContentSHA256Mismatch" in message
    assert f"Hailstack used Pulumi CLI {pulumi_path} (v3.245.0)" in message
    assert "AWS_REQUEST_CHECKSUM_CALCULATION=when_required" in message
    assert "AWS_RESPONSE_CHECKSUM_VALIDATION=when_required" in message
    assert "fall back to the known-good Pulumi CLI 3.226.0" in message


def test_backend_access_signature_mismatch_with_checksum_vars_hints_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Treat signatures with checksum compatibility as likely bad S3 secrets."""
    pulumi_path = tmp_path / "tools" / "bin" / "pulumi"
    pulumi_path.parent.mkdir(parents=True)
    pulumi_path.write_text("#!/bin/sh\n", encoding="utf-8")
    pulumi_path.chmod(0o700)
    monkeypatch.setenv("PATH", str(pulumi_path.parent))

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del capture_output, check, cwd, env, text
        if args == [str(pulumi_path), "version"]:
            return SimpleNamespace(returncode=0, stderr="", stdout="v3.245.0\n")
        return SimpleNamespace(
            returncode=1,
            stderr=(
                'error: problem logging in: read ".pulumi/meta.yaml": '
                "StatusCode: 403, api error SignatureDoesNotMatch: UnknownError"
            ),
            stdout="",
        )

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)

    with pytest.raises(S3Error) as exc_info:
        stack_module.AutomationStackRunner().check_backend_access(
            _config(endpoint="cog.sanger.ac.uk")
        )

    message = str(exc_info.value)
    assert "SignatureDoesNotMatch" in message
    assert f"Hailstack used Pulumi CLI {pulumi_path} (v3.245.0)" in message
    assert "AWS_REQUEST_CHECKSUM_CALCULATION=when_required" in message
    assert "AWS_RESPONSE_CHECKSUM_VALIDATION=when_required" in message
    assert "check the Ceph S3 state-bucket credentials" in message
    assert "ceph_s3.access_key" in message
    assert "ceph_s3.secret_key" in message
    assert "--dotenv" in message
    assert "newer Pulumi CLI versions may fail" not in message


def test_pulumi_env_uses_caller_s3_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect a caller-provided S3 backend region."""
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["AWS_REGION"] == "eu-west-2"
    assert env["AWS_DEFAULT_REGION"] == "eu-west-2"


def test_cli_env_matches_automation_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use the same Pulumi environment for CLI reads and automation actions."""
    monkeypatch.delenv("PULUMI_HOME", raising=False)

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)

    assert runner.cli_env(_config()) == runner._pulumi_env(_config())
