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
from collections.abc import Callable
from types import SimpleNamespace
from typing import cast

import pytest
from pulumi import automation as auto

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

    def outputs(self) -> dict[str, object]:
        """Return fake stack outputs."""
        return {
            name: SimpleNamespace(value=value)
            for name, value in self.output_values.items()
        }


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


def _capture_new_stack_preview_env(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, str]:
    """Return the Pulumi env passed to a first-time stack preview."""
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


def test_preview_destroy_selects_existing_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use select-stack for destroy previews so dry runs do not create state."""
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
    fake_stack = FakeAutoStack()
    recorded_allow_missing_runtime_secrets: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
    ) -> None:
        del config, bundle, retain_created_volume
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

    assert env["PULUMI_CONFIG_PASSPHRASE"]
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
    fake_stack = FakeAutoStack()
    recorded_retain_created_volume: list[bool | None] = []
    recorded_allow_missing_runtime_secrets: list[bool] = []
    recorded_allow_missing_ssh_public_keys: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
        allow_missing_ssh_public_keys: bool = False,
    ) -> None:
        del config, bundle
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
    fake_stack = FakeAutoStack()
    recorded_retain_created_volume: list[bool | None] = []
    recorded_allow_missing_runtime_secrets: list[bool] = []
    recorded_allow_missing_ssh_public_keys: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
        allow_missing_ssh_public_keys: bool = False,
    ) -> None:
        del config, bundle
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
    fake_stack = FakeAutoStack()
    recorded_retain_created_volume: list[bool | None] = []
    recorded_allow_missing_runtime_secrets: list[bool] = []
    recorded_allow_missing_ssh_public_keys: list[bool] = []
    captured_program: list[Callable[[], None]] = []

    def fake_create_cluster_resources(
        config: object,
        bundle: object,
        *,
        retain_created_volume: bool | None = None,
        allow_missing_runtime_secrets: bool = False,
        allow_missing_ssh_public_keys: bool = False,
    ) -> None:
        del config, bundle
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


def test_pulumi_env_does_not_default_passphrase_for_persisted_stacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep passphrase defaults scoped away from persisted stack operations."""
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE", raising=False)
    monkeypatch.delenv("PULUMI_CONFIG_PASSPHRASE_FILE", raising=False)

    runner = stack_module.AutomationStackRunner(work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert "PULUMI_CONFIG_PASSPHRASE" not in env
    assert "PULUMI_CONFIG_PASSPHRASE_FILE" not in env


def test_backend_access_normalizes_bare_ceph_endpoint_and_defaults_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Login to documented bare Ceph endpoints with a valid S3 region."""
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
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
        captured_envs.append(env)
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(stack_module.subprocess, "run", fake_run)

    stack_module.AutomationStackRunner().check_backend_access(
        _config(endpoint="cog.sanger.ac.uk")
    )

    assert captured_args == [
        [
            "pulumi",
            "login",
            "--non-interactive",
            "s3://hailstack-state?endpoint=https://cog.sanger.ac.uk",
        ]
    ]
    assert captured_envs[0]["AWS_REGION"] == "us-east-1"
    assert captured_envs[0]["AWS_DEFAULT_REGION"] == "us-east-1"


def test_backend_access_checksum_mismatch_hints_supported_pulumi_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explain known Ceph checksum mismatches with the supported Pulumi version."""
    from hailstack.tool_versions import SUPPORTED_PULUMI_CLI_VERSION

    def fake_run(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: object,
        env: dict[str, str],
        text: bool,
    ) -> object:
        del args, capture_output, check, cwd, env, text
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
    assert f"Pulumi CLI {SUPPORTED_PULUMI_CLI_VERSION}" in message
    assert "newer Pulumi CLI versions may fail" in message


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
