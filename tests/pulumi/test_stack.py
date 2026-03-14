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

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import PulumiError
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


def _config() -> ClusterConfig:
    """Return the subset of config the runner needs for tests."""
    return cast(
        ClusterConfig,
        SimpleNamespace(
            cluster=SimpleNamespace(name="test-cluster"),
            ceph_s3=SimpleNamespace(
                bucket="hailstack-state",
                endpoint="https://ceph.example.invalid",
                access_key="state-access",
                secret_key="state-secret",
            ),
            volumes=SimpleNamespace(preserve_on_destroy=False),
        ),
    )


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
        recorded_allow_missing_runtime_secrets.append(
            allow_missing_runtime_secrets)

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
        recorded_allow_missing_runtime_secrets.append(
            allow_missing_runtime_secrets)
        recorded_allow_missing_ssh_public_keys.append(
            allow_missing_ssh_public_keys)

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
        recorded_allow_missing_runtime_secrets.append(
            allow_missing_runtime_secrets)
        recorded_allow_missing_ssh_public_keys.append(
            allow_missing_ssh_public_keys)

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
        recorded_allow_missing_runtime_secrets.append(
            allow_missing_runtime_secrets)
        recorded_allow_missing_ssh_public_keys.append(
            allow_missing_ssh_public_keys)

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

    runner = stack_module.AutomationStackRunner(
        work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["PULUMI_HOME"].startswith(
        f"{tempfile.gettempdir()}/hailstack-pulumi-home/"
    )


def test_pulumi_env_preserves_explicit_pulumi_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Respect an explicit Pulumi home instead of overwriting it."""
    monkeypatch.setenv("PULUMI_HOME", "/tmp/custom-pulumi-home")

    runner = stack_module.AutomationStackRunner(
        work_dir=stack_module.REPOSITORY_ROOT)
    env = runner._pulumi_env(_config())

    assert env["PULUMI_HOME"] == "/tmp/custom-pulumi-home"


def test_cli_env_matches_automation_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use the same Pulumi environment for CLI reads and automation actions."""
    monkeypatch.delenv("PULUMI_HOME", raising=False)

    runner = stack_module.AutomationStackRunner(
        work_dir=stack_module.REPOSITORY_ROOT)

    assert runner.cli_env(_config()) == runner._pulumi_env(_config())
