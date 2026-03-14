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

"""Integration tests for the mocked Pulumi create and destroy lifecycle."""

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypedDict, cast

import pulumi
import pulumi.runtime
import pytest
from typer.testing import CliRunner

from hailstack.cli.commands import create as create_module
from hailstack.cli.main import app
from hailstack.pulumi import stack as stack_module

runner = CliRunner()

EXPECTED_CREATE_RESOURCE_COUNT = 22


class ResourceRecord(TypedDict):
    """Store a mocked Pulumi resource registration."""

    type: str
    name: str
    inputs: dict[str, object]


class MockResourceArgsView(Protocol):
    """Provide a typed view of Pulumi mock resource inputs."""

    inputs: Mapping[str, object]


class MockCallArgsView(Protocol):
    """Provide a typed view of Pulumi mock invoke args."""

    args: Mapping[str, object]


@dataclass(frozen=True)
class FlavorDetails:
    """Represent fake flavour details for create pre-flight validation."""

    vcpus: int
    ram_mb: int


@dataclass(frozen=True)
class ComputeQuota:
    """Represent fake available compute quota."""

    instances_available: int = 10
    cores_available: int = 100
    ram_mb_available: int = 512000


@dataclass(frozen=True)
class VolumeQuota:
    """Represent fake available volume quota."""

    gigabytes_available: int = 1000


@dataclass(frozen=True)
class FakePreviewResult:
    """Represent the subset of a Pulumi preview result used by the runner."""

    stdout: str


@dataclass(frozen=True)
class FakeOutputValue:
    """Represent one output value returned by a fake Pulumi update."""

    value: object


@dataclass(frozen=True)
class FakeUpResult:
    """Represent the subset of a Pulumi up result used by the runner."""

    stdout: str
    outputs: dict[str, FakeOutputValue]


@dataclass(frozen=True)
class ProgramSnapshot:
    """Capture the mocked resources and exports from one Pulumi program run."""

    resources: list[ResourceRecord]
    exports: dict[str, object]


class FakeOpenStackClient:
    """Provide deterministic OpenStack pre-flight responses for create."""

    def __init__(self) -> None:
        """Initialise the fake resource lookup state."""
        self.images = {"hailstack-hail-0.2.137-gnomad-3.0.4-r2"}
        self.flavours = {
            "m2.2xlarge": FlavorDetails(vcpus=8, ram_mb=32768),
            "m2.xlarge": FlavorDetails(vcpus=4, ram_mb=16384),
        }
        self.networks = {"private-net"}
        self.compute_quota = ComputeQuota()
        self.volume_quota = VolumeQuota()

    def get_image(self, name: str) -> object | None:
        """Return a truthy image record when the image exists."""
        return {"name": name} if name in self.images else None

    def get_flavour(self, name: str) -> FlavorDetails | None:
        """Return flavour details when the flavour exists."""
        return self.flavours.get(name)

    def get_network(self, name: str) -> object | None:
        """Return a truthy network record when the network exists."""
        return {"name": name} if name in self.networks else None

    def floating_ip_is_available(self, address: str) -> bool:
        """Report whether a configured floating IP is available."""
        del address
        return True

    def volume_exists(self, volume_id: str) -> bool:
        """Report whether a referenced volume exists."""
        del volume_id
        return True

    def volume_is_available(self, volume_id: str) -> bool:
        """Report whether a referenced volume can be attached."""
        del volume_id
        return True

    def get_compute_quota(self) -> ComputeQuota:
        """Return available compute quota."""
        return self.compute_quota

    def get_volume_quota(self) -> VolumeQuota:
        """Return available volume quota."""
        return self.volume_quota


class RecordingMocks(pulumi.runtime.Mocks):
    """Record mocked Pulumi resources and synthesize stable outputs."""

    def __init__(self) -> None:
        """Initialise resource and invoke recording state."""
        self.resources: list[ResourceRecord] = []
        self._next_floating_ip_octet = 10

    def new_resource(
        self,
        args: pulumi.runtime.MockResourceArgs,
    ) -> tuple[str | None, dict[str, object]]:
        """Capture resource registrations and add deterministic outputs."""
        typed_args = cast(MockResourceArgsView, args)
        inputs = {str(key): value for key, value in typed_args.inputs.items()}
        state = dict(inputs)
        state.setdefault("name", inputs.get("name", args.name))
        state.setdefault("tags", [])

        if args.typ == "openstack:networking/port:Port":
            state.setdefault("all_fixed_ips", [self._ip_for_name(args.name)])

        if args.typ == "openstack:networking/floatingIp:FloatingIp":
            state.setdefault("address", f"203.0.113.{self._next_floating_ip_octet}")
            self._next_floating_ip_octet += 1

        self.resources.append(
            {
                "type": args.typ,
                "name": args.name,
                "inputs": inputs,
            }
        )
        return f"{args.name}-id", state

    def call(
        self,
        args: pulumi.runtime.MockCallArgs,
    ) -> tuple[dict[str, object], list[tuple[str, str]]]:
        """Mock the OpenStack network lookup data source."""
        typed_args = cast(MockCallArgsView, args)
        invoke_args = {str(key): value for key, value in typed_args.args.items()}
        if args.token == "openstack:networking/getNetwork:getNetwork":
            network_name = str(invoke_args["name"])
            return {"id": f"{network_name}-id", "name": network_name}, []
        return {}, []

    @staticmethod
    def _ip_for_name(name: str) -> str:
        """Return a stable private IP for a mocked port name."""
        if name.endswith("master-port"):
            return "10.0.0.10"
        suffix = int(name.rsplit("-", 1)[1])
        return f"10.0.0.{suffix + 10}"


class FakeAutoStack:
    """Implement the Pulumi automation stack interface needed by the runner."""

    def __init__(
        self,
        environment: FakeAutomationEnvironment,
        stack_name: str,
        program: Callable[[], None],
    ) -> None:
        """Initialise a fake stack with the current Pulumi program."""
        self._environment = environment
        self._stack_name = stack_name
        self._program = program
        self.created_snapshot: ProgramSnapshot | None = None
        self.workspace = environment

    def set_program(self, program: Callable[[], None]) -> None:
        """Update the currently selected Pulumi program for this stack."""
        self._program = program

    def preview(
        self,
        on_output: Callable[[str], None] | None = None,
    ) -> FakePreviewResult:
        """Render either a create or destroy preview from mocked state."""
        snapshot = self._environment.run_program(self._program)
        if snapshot.resources:
            stdout = (
                "Previewing update\n"
                "Resources:\n"
                f"  + {len(snapshot.resources)} to create\n"
            )
        else:
            destroy_count = 0
            if self.created_snapshot is not None:
                destroy_count = len(self.created_snapshot.resources)
            stdout = f"Previewing destroy\nResources:\n  - {destroy_count} to delete\n"
        if on_output is not None:
            on_output(stdout)
        return FakePreviewResult(stdout=stdout)

    def preview_destroy(
        self,
        on_output: Callable[[str], None] | None = None,
    ) -> FakePreviewResult:
        """Render a destroy preview from the currently created mocked state."""
        destroy_count = 0
        if self.created_snapshot is not None:
            destroy_count = len(self.created_snapshot.resources)
        stdout = f"Previewing destroy\nResources:\n  - {destroy_count} to delete\n"
        if on_output is not None:
            on_output(stdout)
        return FakePreviewResult(stdout=stdout)

    def up(self, on_output: Callable[[str], None] | None = None) -> FakeUpResult:
        """Run the current Pulumi program and persist the created stack state."""
        if self._stack_name in self._environment.fail_up_for_stacks:
            raise RuntimeError(f"update failed for {self._stack_name}")
        snapshot = self._environment.run_program(self._program)
        self.created_snapshot = snapshot
        stdout = (
            "Updating\n"
            "Outputs:\n"
            + "\n".join(
                f"  {name}: {value}" for name, value in sorted(snapshot.exports.items())
            )
            + "\n"
        )
        if on_output is not None:
            on_output(stdout)
        return FakeUpResult(
            stdout=stdout,
            outputs={
                name: FakeOutputValue(value=value)
                for name, value in snapshot.exports.items()
            },
        )

    def destroy(self, remove: bool = False) -> None:
        """Destroy the created stack state and optionally remove the stack."""
        self.created_snapshot = None
        if remove:
            self._environment.stacks.pop(self._stack_name, None)
            self._environment.removed_stacks.add(self._stack_name)


class FakeAutomationEnvironment:
    """Provide a fake Pulumi automation backend for lifecycle tests."""

    def __init__(self) -> None:
        """Initialise fake automation stack storage."""
        self.stacks: dict[str, FakeAutoStack] = {}
        self.fail_up_for_stacks: set[str] = set()
        self.removed_stacks: set[str] = set()

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Patch the automation and backend login seams used by the runner."""

        def fake_create_or_select_stack(
            *,
            stack_name: str,
            project_name: str,
            program: Callable[[], None],
            opts: object,
        ) -> FakeAutoStack:
            del project_name, opts
            stack = self.stacks.get(stack_name)
            if stack is None:
                stack = FakeAutoStack(self, stack_name, program)
                self.stacks[stack_name] = stack
            else:
                stack.set_program(program)
            return stack

        def fake_create_stack(
            *,
            stack_name: str,
            project_name: str,
            program: Callable[[], None],
            opts: object,
        ) -> FakeAutoStack:
            del project_name, opts
            if stack_name in self.stacks:
                raise RuntimeError(f"stack {stack_name} already exists")
            stack = FakeAutoStack(self, stack_name, program)
            self.stacks[stack_name] = stack
            return stack

        def fake_select_stack(
            *,
            stack_name: str,
            project_name: str,
            program: Callable[[], None],
            opts: object,
        ) -> FakeAutoStack:
            del project_name, opts
            stack = self.stacks.get(stack_name)
            if stack is None:
                raise RuntimeError(f"no stack named {stack_name}")
            stack.set_program(program)
            return stack

        def fake_subprocess_run(*args: object, **kwargs: object) -> object:
            del args, kwargs

            @dataclass(frozen=True)
            class Result:
                returncode: int = 0
                stdout: str = ""
                stderr: str = ""

            return Result()

        monkeypatch.setattr(
            stack_module.auto,
            "create_or_select_stack",
            fake_create_or_select_stack,
        )
        monkeypatch.setattr(
            stack_module.auto,
            "create_stack",
            fake_create_stack,
        )
        monkeypatch.setattr(
            stack_module.auto,
            "select_stack",
            fake_select_stack,
        )
        monkeypatch.setattr(stack_module.subprocess, "run", fake_subprocess_run)

    def run_program(self, program: Callable[[], None]) -> ProgramSnapshot:
        """Execute one Pulumi program with provider mocks and resolve exports."""
        mocks = RecordingMocks()
        exported: dict[str, object] = {}
        loop = asyncio.new_event_loop()
        original_export = pulumi.export
        asyncio.set_event_loop(loop)

        def capture_export(name: str, value: object) -> None:
            exported[name] = value

        try:
            pulumi.runtime.set_mocks(mocks, project="hailstack", stack="integration")
            pulumi.export = capture_export
            program()
            _drain_resource_registrations(loop, mocks)
            resolved_exports = {
                name: (
                    value
                    if not isinstance(value, pulumi.Output)
                    else _resolve_output(loop, cast(pulumi.Output[object], value))
                )
                for name, value in exported.items()
            }
            return ProgramSnapshot(
                resources=list(mocks.resources),
                exports=resolved_exports,
            )
        finally:
            pulumi.export = original_export
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()
            asyncio.set_event_loop(None)

    def has_stack(self, stack_name: str) -> bool:
        """Return whether the fake backend still retains the stack."""
        return stack_name in self.stacks

    def snapshot(self, stack_name: str) -> ProgramSnapshot:
        """Return the last created snapshot for a retained stack."""
        stack = self.stacks[stack_name]
        assert stack.created_snapshot is not None
        return stack.created_snapshot

    def remove_stack(self, stack_name: str) -> None:
        """Remove a stack from the fake backend."""
        self.stacks.pop(stack_name, None)
        self.removed_stacks.add(stack_name)


def _resolve_output(
    loop: asyncio.AbstractEventLoop,
    output: pulumi.Output[object],
) -> object:
    """Resolve a Pulumi output during the mocked stack run."""
    resolved = loop.run_until_complete(output.future())
    assert resolved is not None
    return resolved


def _drain_resource_registrations(
    loop: asyncio.AbstractEventLoop,
    mocks: RecordingMocks,
) -> None:
    """Wait until mock resource registrations stop increasing."""
    stable_iterations = 0
    previous_count = -1
    for _ in range(50):
        loop.run_until_complete(asyncio.sleep(0.01))
        current_count = len(mocks.resources)
        pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
        if current_count == previous_count and not pending_tasks:
            stable_iterations += 1
            if stable_iterations >= 2:
                return
            continue
        previous_count = current_count
        stable_iterations = 0


def _write_bundles(path: Path) -> Path:
    """Write a temporary compatibility matrix for lifecycle tests."""
    path.write_text(
        """
[default]
bundle = "hail-0.2.137-gnomad-3.0.4-r2"

[bundle."hail-0.2.137-gnomad-3.0.4-r2"]
hail = "0.2.137"
spark = "3.5.6"
hadoop = "3.4.1"
java = "11"
python = "3.12"
scala = "2.12.18"
gnomad = "3.0.4"
status = "latest"
""".strip(),
        encoding="utf-8",
    )
    return path


def _write_config(path: Path) -> Path:
    """Write a minimal valid config file for create and destroy lifecycle tests."""
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            'bundle = "hail-0.2.137-gnomad-3.0.4-r2"\n'
            "num_workers = 2\n"
            'master_flavour = "m2.2xlarge"\n'
            'worker_flavour = "m2.xlarge"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n\n'
            "[ceph_s3]\n"
            'endpoint = "https://ceph.example.invalid"\n'
            'bucket = "hailstack-state"\n'
            'access_key = "state-access"\n'
            'secret_key = "state-secret"\n\n'
            "[ssh_keys]\n"
            'public_keys = ["ssh-ed25519 AAAA primary@test"]\n'
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def lifecycle_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, FakeAutomationEnvironment]:
    """Install fake automation and command dependencies for lifecycle tests."""
    matrix_path = _write_bundles(tmp_path / "bundles.toml")
    config_path = _write_config(tmp_path / "cluster.toml")
    environment = FakeAutomationEnvironment()
    environment.install(monkeypatch)
    monkeypatch.setattr(create_module, "DEFAULT_COMPATIBILITY_MATRIX_PATH", matrix_path)
    monkeypatch.setattr(
        create_module,
        "create_openstack_preflight_client",
        lambda: FakeOpenStackClient(),
    )
    monkeypatch.setenv("HAILSTACK_WEB_PASSWORD", "web-secret")
    return config_path, environment


def test_create_dry_run_reports_expected_resource_count(
    lifecycle_environment: tuple[Path, FakeAutomationEnvironment],
) -> None:
    """Preview the mocked create and report the exact resource count."""
    config_path, environment = lifecycle_environment

    result = runner.invoke(app, ["create", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert result.stdout == (
        "Previewing update\n"
        "Resources:\n"
        f"  + {EXPECTED_CREATE_RESOURCE_COUNT} to create\n"
    )
    assert not environment.has_stack("hailstack-test-cluster")


def test_create_failed_update_keeps_existing_stack(
    lifecycle_environment: tuple[Path, FakeAutomationEnvironment],
) -> None:
    """Do not destroy an existing stack when a subsequent update fails."""
    config_path, environment = lifecycle_environment

    create_result = runner.invoke(app, ["create", "--config", str(config_path)])
    assert create_result.exit_code == 0
    original_snapshot = environment.snapshot("hailstack-test-cluster")

    environment.fail_up_for_stacks.add("hailstack-test-cluster")
    retry_result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert retry_result.exit_code == 1
    assert environment.has_stack("hailstack-test-cluster")
    assert environment.snapshot("hailstack-test-cluster") == original_snapshot


def test_create_apply_exports_expected_stack_outputs(
    lifecycle_environment: tuple[Path, FakeAutomationEnvironment],
) -> None:
    """Apply the mocked create and persist the documented outputs."""
    config_path, environment = lifecycle_environment

    result = runner.invoke(app, ["create", "--config", str(config_path)])

    assert result.exit_code == 0
    snapshot = environment.snapshot("hailstack-test-cluster")
    assert snapshot.exports == {
        "bundle_id": "hail-0.2.137-gnomad-3.0.4-r2",
        "cluster_name": "test-cluster",
        "master_private_ip": "10.0.0.10",
        "master_flavour": "m2.2xlarge",
        "master_public_ip": "203.0.113.10",
        "managed_volume_size_gb": 0,
        "monitoring_enabled": True,
        "num_workers": 2,
        "worker_flavour": "m2.xlarge",
        "worker_names": ["test-cluster-worker-01", "test-cluster-worker-02"],
        "worker_private_ips": ["10.0.0.11", "10.0.0.12"],
    }


def test_destroy_removes_created_stack_after_confirmation(
    lifecycle_environment: tuple[Path, FakeAutomationEnvironment],
) -> None:
    """Destroy the created mocked stack and remove it from the backend."""
    config_path, environment = lifecycle_environment

    create_result = runner.invoke(app, ["create", "--config", str(config_path)])
    assert create_result.exit_code == 0

    destroy_result = runner.invoke(
        app,
        ["destroy", "--config", str(config_path)],
        input="test-cluster\n",
    )

    assert destroy_result.exit_code == 0
    assert not environment.has_stack("hailstack-test-cluster")
    assert "hailstack-test-cluster" in environment.removed_stacks
