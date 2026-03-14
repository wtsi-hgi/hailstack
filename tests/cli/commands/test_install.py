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

"""Acceptance tests for the G1 install CLI command."""

import json
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pytest
from pip._vendor.packaging.requirements import Requirement
from typer.testing import CliRunner

from hailstack.ansible import runner as ansible_runner_module
from hailstack.cli.commands import install as install_module
from hailstack.cli.main import app
from hailstack.storage import rollout as rollout_module

runner = CliRunner()


@dataclass(frozen=True)
class FakeNodeResult:
    """Represent a fake per-node install result."""

    node_name: str
    host: str
    success: bool
    system_packages: tuple[str, ...] = ()
    python_packages: tuple[str, ...] = ()
    smoke_test: str | None = None
    verification: dict[str, object] | None = None
    error: str = ""
    changed: bool = False
    attempts: int = 1
    software_state_updated: bool = True


class FakeStackOutputsRunner:
    """Return deterministic Pulumi stack outputs for install tests."""

    def __init__(self) -> None:
        """Initialise fake outputs for one master and three workers."""
        self.calls = 0

    def get_install_outputs(self, config: object) -> dict[str, object]:
        """Return a stable install inventory payload."""
        del config
        self.calls += 1
        return {
            "cluster_name": "test-cluster",
            "master_public_ip": "198.51.100.10",
            "master_private_ip": "10.0.0.10",
            "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
            "worker_names": [
                "test-cluster-worker-01",
                "test-cluster-worker-02",
                "test-cluster-worker-03",
            ],
        }


class FakePlaybookRunner:
    """Capture install-playbook invocations and emit fake node results."""

    def __init__(
        self,
        *,
        responses: Sequence[Sequence[FakeNodeResult]] | None = None,
    ) -> None:
        """Initialise per-attempt responses for selected nodes."""
        self.responses = list(responses or [])
        self.calls: list[dict[str, object]] = []

    def __call__(self, command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        """Record each install attempt and write one fake results file."""
        command_list = list(command)
        vars_path = Path(
            command_list[command_list.index("-e") + 1].removeprefix("@"))
        payload = json.loads(vars_path.read_text(encoding="utf-8"))
        inventory_path = Path(command_list[command_list.index("-i") + 1])
        inventory_payload = json.loads(
            inventory_path.read_text(encoding="utf-8"))
        self.calls.append(
            {
                "inventory": inventory_payload,
                "system_packages": list(payload["system_packages"]),
                "python_packages": list(payload["python_packages"]),
                "ssh_username": command_list[command_list.index("-u") + 1],
                "ssh_key_path": (
                    Path(command_list[command_list.index("--private-key") + 1])
                    if "--private-key" in command_list
                    else None
                ),
                "smoke_test": payload["smoke_test"],
            }
        )
        if self.responses:
            response = self.responses.pop(0)
        else:
            response = [
                FakeNodeResult(
                    node_name=str(host),
                    host=str(host),
                    success=True,
                    system_packages=tuple(payload["system_packages"]),
                    python_packages=tuple(payload["python_packages"]),
                    smoke_test=payload["smoke_test"],
                )
                for host in inventory_payload["all"]["hosts"]
            ]
        result_path = Path(payload["hailstack_result_path"])
        lines = [
            json.dumps(
                {
                    "hostname": result.host,
                    "success": result.success,
                    "system_installed": _verified_system_packages(
                        requested_packages=payload["system_packages"],
                        installed_packages=result.system_packages,
                    ),
                    "python_installed": _verified_python_packages(
                        requested_packages=payload["python_packages"],
                        installed_packages=result.python_packages,
                    ),
                    "errors": [result.error] if result.error else [],
                    "verification": _verification_payload(
                        result=result,
                        requested_system_packages=payload["system_packages"],
                        requested_python_packages=payload["python_packages"],
                        smoke_test=payload["smoke_test"],
                    ),
                    "changed": result.changed,
                },
                sort_keys=True,
            )
            for result in response
        ]
        result_path.write_text("\n".join(lines), encoding="utf-8")
        return subprocess.CompletedProcess(command_list, 0, stdout="", stderr="")


class RecordingUploader:
    """Capture rollout uploads performed by the real storage module."""

    def __init__(self) -> None:
        """Initialise recorder state."""
        self.objects: dict[str, bytes] = {}

    def put_object(self, *, key: str, body: bytes, content_type: str) -> None:
        """Record one uploaded S3 object."""
        assert content_type == "application/json"
        self.objects[key] = body


def _write_config(path: Path) -> Path:
    """Write a minimal valid install configuration file."""
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            'bundle = "obsolete-bundle-id"\n'
            "num_workers = 3\n"
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


def _write_package_file(path: Path, *, system: list[str], python: list[str]) -> Path:
    """Write a package list TOML file for install tests."""
    system_list = ", ".join(f'"{package}"' for package in system)
    python_list = ", ".join(f'"{package}"' for package in python)
    path.write_text(
        (
            "[system]\n"
            f"packages = [{system_list}]\n\n"
            "[python]\n"
            f"packages = [{python_list}]\n"
        ),
        encoding="utf-8",
    )
    return path


def _distribution_name(requirement: str) -> str:
    """Return the normalized distribution name for a requested package."""
    return Requirement(requirement).name.lower().replace("_", "-").replace(".", "-")


def _distribution_version(requirement: str) -> str:
    """Choose an installed version that satisfies the requested requirement."""
    parsed_requirement = Requirement(requirement)
    candidates = [
        specifier.version for specifier in parsed_requirement.specifier]
    candidates.extend(["9999", "1.0", "0"])
    for candidate in candidates:
        if not parsed_requirement.specifier or parsed_requirement.specifier.contains(
            candidate,
            prereleases=True,
        ):
            return candidate
    raise AssertionError(f"No satisfying test version found for {requirement}")


def _installed_python_versions(packages: Sequence[str]) -> dict[str, str]:
    """Return installed Python versions keyed by normalized distribution name."""
    return {
        _distribution_name(package): _distribution_version(package)
        for package in packages
    }


def _verification_payload(
    *,
    result: FakeNodeResult,
    requested_system_packages: Sequence[str],
    requested_python_packages: Sequence[str],
    smoke_test: str | None,
) -> dict[str, object]:
    """Build realistic verification metadata from simulated node state."""
    if result.verification is not None:
        return result.verification

    installed_system_packages = set(result.system_packages)
    installed_python_versions = _installed_python_versions(
        result.python_packages)
    system_status = {
        package: package in installed_system_packages
        for package in requested_system_packages
    }
    python_status = {
        package: _distribution_name(package) in installed_python_versions
        for package in requested_python_packages
    }
    version_status = {
        package: (
            _distribution_name(package) in installed_python_versions
            and Requirement(package).specifier.contains(
                installed_python_versions[_distribution_name(package)],
                prereleases=True,
            )
            if Requirement(package).specifier
            else _distribution_name(package) in installed_python_versions
        )
        for package in requested_python_packages
    }
    import_status = {
        package: _distribution_name(package) in installed_python_versions
        for package in requested_python_packages
    }
    return {
        "system": system_status,
        "python": python_status,
        "imports": import_status,
        "versions": version_status,
        "smoke_test": smoke_test is None or result.success,
        "software_state_updated": result.software_state_updated,
    }


def _verified_system_packages(
    *,
    requested_packages: Sequence[str],
    installed_packages: Sequence[str],
) -> list[str]:
    """Return requested system packages that are present on the fake node."""
    installed = set(installed_packages)
    return [package for package in requested_packages if package in installed]


def _verified_python_packages(
    *,
    requested_packages: Sequence[str],
    installed_packages: Sequence[str],
) -> list[str]:
    """Return requested Python packages that are present on the fake node."""
    installed_versions = _installed_python_versions(installed_packages)
    return [
        package
        for package in requested_packages
        if _distribution_name(package) in installed_versions
    ]


def _result_for_all_nodes(
    *,
    success: bool = True,
    system_packages: Sequence[str] = (),
    python_packages: Sequence[str] = (),
    smoke_test: str | None = None,
    changed: bool = False,
) -> list[FakeNodeResult]:
    """Return node results for the default fake cluster."""
    return [
        FakeNodeResult(
            node_name=node_name,
            host=host,
            success=success,
            system_packages=tuple(system_packages),
            python_packages=tuple(python_packages),
            smoke_test=smoke_test,
            changed=changed,
        )
        for node_name, host in (
            ("test-cluster-master", "198.51.100.10"),
            ("test-cluster-worker-01", "10.0.0.21"),
            ("test-cluster-worker-02", "10.0.0.22"),
            ("test-cluster-worker-03", "10.0.0.23"),
        )
    ]


def _install_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stack_runner: FakeStackOutputsRunner | None = None,
    playbook_runner: FakePlaybookRunner | None = None,
    uploader: RecordingUploader | None = None,
    sleep_calls: list[float] | None = None,
) -> tuple[FakeStackOutputsRunner, FakePlaybookRunner, RecordingUploader]:
    """Install fake dependencies into the install module."""
    fake_stack_runner = stack_runner or FakeStackOutputsRunner()
    fake_playbook_runner = playbook_runner or FakePlaybookRunner()
    fake_uploader = uploader or RecordingUploader()
    monkeypatch.setattr(
        install_module.PulumiInstallStackRunner,
        "get_install_outputs",
        lambda self, config: fake_stack_runner.get_install_outputs(config),
    )
    monkeypatch.setattr(
        ansible_runner_module,
        "_run_playbook_command",
        fake_playbook_runner,
    )
    monkeypatch.setattr(
        rollout_module,
        "create_rollout_uploader",
        lambda config: fake_uploader,
    )
    if sleep_calls is not None:
        monkeypatch.setattr(
            install_module,
            "sleep",
            lambda seconds: sleep_calls.append(seconds),
        )
    return fake_stack_runner, fake_playbook_runner, fake_uploader


def _uploaded_manifest(uploader: RecordingUploader) -> dict[str, object]:
    """Return the uploaded manifest JSON payload."""
    manifest_keys = [
        key for key in uploader.objects if key.endswith("/manifest.json")]
    assert len(manifest_keys) == 1
    return json.loads(uploader.objects[manifest_keys[0]].decode("utf-8"))


def _uploaded_nodes(uploader: RecordingUploader) -> list[dict[str, object]]:
    """Return the uploaded per-node result payloads."""
    node_keys = sorted(key for key in uploader.objects if "/nodes/" in key)
    return [json.loads(uploader.objects[key].decode("utf-8")) for key in node_keys]


def test_install_system_package_verification_is_recorded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist requested system packages in each uploaded per-node result."""
    config_path = _write_config(tmp_path / "install.toml")
    playbook_runner = FakePlaybookRunner(
        responses=[_result_for_all_nodes(
            success=True, system_packages=["libpq-dev"])]
    )
    _, fake_playbook_runner, fake_uploader = _install_fakes(
        monkeypatch,
        playbook_runner=playbook_runner,
    )

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--system", "libpq-dev"],
    )

    assert result.exit_code == 0
    assert fake_playbook_runner.calls[0]["system_packages"] == ["libpq-dev"]
    assert all(
        node["system_installed"] == ["libpq-dev"]
        for node in _uploaded_nodes(fake_uploader)
    )


def test_install_python_package_verification_is_recorded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist requested Python packages in each uploaded per-node result."""
    config_path = _write_config(tmp_path / "install.toml")
    playbook_runner = FakePlaybookRunner(
        responses=[_result_for_all_nodes(
            success=True, python_packages=["pandas"])]
    )
    _, fake_playbook_runner, fake_uploader = _install_fakes(
        monkeypatch,
        playbook_runner=playbook_runner,
    )

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--python", "pandas"],
    )

    assert result.exit_code == 0
    assert fake_playbook_runner.calls[0]["python_packages"] == ["pandas"]
    assert all(
        node["python_installed"] == ["pandas"]
        for node in _uploaded_nodes(fake_uploader)
    )


def test_install_loads_system_and_python_packages_from_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Install both package types from the packages TOML file."""
    config_path = _write_config(tmp_path / "install.toml")
    packages_path = _write_package_file(
        tmp_path / "packages.toml",
        system=["mc"],
        python=["pandas"],
    )
    playbook_runner = FakePlaybookRunner(
        responses=[
            _result_for_all_nodes(
                success=True,
                system_packages=["mc"],
                python_packages=["pandas"],
            )
        ]
    )
    _, fake_playbook_runner, _ = _install_fakes(
        monkeypatch,
        playbook_runner=playbook_runner,
    )

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path),
         "--file", str(packages_path)],
    )

    assert result.exit_code == 0
    assert fake_playbook_runner.calls[0]["system_packages"] == ["mc"]
    assert fake_playbook_runner.calls[0]["python_packages"] == ["pandas"]


def test_install_merges_inline_and_file_packages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Merge inline package arguments with package-file contents."""
    config_path = _write_config(tmp_path / "install.toml")
    packages_path = _write_package_file(
        tmp_path / "packages.toml",
        system=["pkg2"],
        python=[],
    )
    playbook_runner = FakePlaybookRunner(
        responses=[
            _result_for_all_nodes(
                success=True,
                system_packages=["pkg1", "pkg2"],
            )
        ]
    )
    _, fake_playbook_runner, fake_uploader = _install_fakes(
        monkeypatch,
        playbook_runner=playbook_runner,
    )

    result = runner.invoke(
        app,
        [
            "install",
            "--config",
            str(config_path),
            "--system",
            "pkg1",
            "--file",
            str(packages_path),
        ],
    )

    assert result.exit_code == 0
    assert fake_playbook_runner.calls[0]["system_packages"] == ["pkg1", "pkg2"]
    assert _uploaded_manifest(fake_uploader)[
        "system_packages"] == ["pkg1", "pkg2"]


def test_install_records_smoke_test_failure_in_node_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mark the failing node unsuccessful when the smoke test exits non-zero."""
    config_path = _write_config(tmp_path / "install.toml")
    smoke_test = "python -c 'import pandas'"
    responses = [
        [
            *_result_for_all_nodes(
                success=True,
                python_packages=["pandas"],
                smoke_test=smoke_test,
            )[:-1],
            FakeNodeResult(
                node_name="test-cluster-worker-03",
                host="10.0.0.23",
                success=False,
                python_packages=("pandas",),
                smoke_test=smoke_test,
                error="smoke test failed",
            ),
        ],
        [
            FakeNodeResult(
                node_name="test-cluster-worker-03",
                host="10.0.0.23",
                success=False,
                python_packages=("pandas",),
                smoke_test=smoke_test,
                error="smoke test failed",
                attempts=2,
            )
        ],
        [
            FakeNodeResult(
                node_name="test-cluster-worker-03",
                host="10.0.0.23",
                success=False,
                python_packages=("pandas",),
                smoke_test=smoke_test,
                error="smoke test failed",
                attempts=3,
            )
        ],
        [
            FakeNodeResult(
                node_name="test-cluster-worker-03",
                host="10.0.0.23",
                success=False,
                python_packages=("pandas",),
                smoke_test=smoke_test,
                error="smoke test failed",
                attempts=4,
            )
        ],
    ]
    playbook_runner = FakePlaybookRunner(responses=responses)
    _, _, fake_uploader = _install_fakes(
        monkeypatch,
        playbook_runner=playbook_runner,
        sleep_calls=[],
    )

    result = runner.invoke(
        app,
        [
            "install",
            "--config",
            str(config_path),
            "--python",
            "pandas",
            "--smoke-test",
            smoke_test,
        ],
    )

    assert result.exit_code == 1
    manifest = _uploaded_manifest(fake_uploader)
    failed_nodes = [
        node for node in _uploaded_nodes(fake_uploader) if not node["success"]
    ]
    assert manifest["success_count"] == 3
    assert manifest["failure_count"] == 1
    assert [node["hostname"]
            for node in failed_nodes] == ["test-cluster-worker-03"]
    assert failed_nodes[0]["errors"] == ["smoke test failed"]


def test_install_retains_version_constraint_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain constrained Python requirement strings in rollout storage."""
    config_path = _write_config(tmp_path / "install.toml")
    package = "pandas>=2.0"
    playbook_runner = FakePlaybookRunner(
        responses=[_result_for_all_nodes(
            success=True, python_packages=[package])]
    )
    _, _, fake_uploader = _install_fakes(
        monkeypatch, playbook_runner=playbook_runner)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--python", package],
    )

    assert result.exit_code == 0
    manifest = _uploaded_manifest(fake_uploader)
    assert manifest["python_packages"] == [package]
    assert all(
        node["python_installed"] == [package] for node in _uploaded_nodes(fake_uploader)
    )


def test_install_rollout_manifest_includes_sha256(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist a manifest with a populated SHA-256 digest field."""
    config_path = _write_config(tmp_path / "install.toml")
    _, _, fake_uploader = _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--system", "mc"],
    )

    assert result.exit_code == 0
    manifest = _uploaded_manifest(fake_uploader)
    assert len(manifest["sha256"]) == 64


def test_install_rollout_records_per_node_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write per-node result entries including package lists and success state."""
    config_path = _write_config(tmp_path / "install.toml")
    playbook_runner = FakePlaybookRunner(
        responses=[
            _result_for_all_nodes(
                success=True,
                system_packages=["mc"],
                python_packages=["pandas"],
            )
        ]
    )
    _, _, fake_uploader = _install_fakes(
        monkeypatch, playbook_runner=playbook_runner)

    result = runner.invoke(
        app,
        [
            "install",
            "--config",
            str(config_path),
            "--system",
            "mc",
            "--python",
            "pandas",
        ],
    )

    assert result.exit_code == 0
    node_results = {node["hostname"]                    : node for node in _uploaded_nodes(fake_uploader)}
    node_result = node_results["test-cluster-worker-01"]
    assert node_result["success"] is True
    assert node_result["system_installed"] == ["mc"]
    assert node_result["python_installed"] == ["pandas"]
    assert node_result["errors"] == []


def test_install_marks_node_local_state_as_updated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Summarise a fully successful rollout when all nodes update cleanly."""
    config_path = _write_config(tmp_path / "install.toml")
    _, _, fake_uploader = _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--system", "mc"],
    )

    assert result.exit_code == 0
    manifest = _uploaded_manifest(fake_uploader)
    assert manifest["success_count"] == 4
    assert manifest["failure_count"] == 0
    assert all(node["success"]
               is True for node in _uploaded_nodes(fake_uploader))


def test_install_records_python_import_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist imported Python packages as installed per-node results."""
    config_path = _write_config(tmp_path / "install.toml")
    playbook_runner = FakePlaybookRunner(
        responses=[_result_for_all_nodes(
            success=True, python_packages=["pandas"])]
    )
    _, _, fake_uploader = _install_fakes(
        monkeypatch, playbook_runner=playbook_runner)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--python", "pandas"],
    )

    assert result.exit_code == 0
    assert all(
        node["python_installed"] == ["pandas"]
        for node in _uploaded_nodes(fake_uploader)
    )


def test_install_routes_workers_via_master_proxyjump(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write ProxyJump inventory settings so worker installs can traverse master."""
    config_path = _write_config(tmp_path / "install.toml")
    fake_playbook_runner = FakePlaybookRunner()
    _install_fakes(monkeypatch, playbook_runner=fake_playbook_runner)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--system", "mc"],
    )

    assert result.exit_code == 0
    worker_hosts = fake_playbook_runner.calls[0]["inventory"]["all"]["children"][
        "worker"
    ]["hosts"]
    assert all(
        host_vars["ansible_ssh_common_args"]
        == "-o ProxyJump=ubuntu@198.51.100.10 -o StrictHostKeyChecking=no "
        "-o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null"
        for host_vars in worker_hosts.values()
    )


def test_install_uses_non_persistent_host_key_options_for_master(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply the same non-persistent host-key handling to master installs."""
    config_path = _write_config(tmp_path / "install.toml")
    fake_playbook_runner = FakePlaybookRunner()
    _install_fakes(monkeypatch, playbook_runner=fake_playbook_runner)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--system", "mc"],
    )

    assert result.exit_code == 0
    master_hosts = fake_playbook_runner.calls[0]["inventory"]["all"]["children"][
        "master"
    ]["hosts"]
    assert all(
        host_vars["ansible_ssh_common_args"]
        == "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        "-o GlobalKnownHostsFile=/dev/null"
        for host_vars in master_hosts.values()
    )


def test_required_import_status_reads_package_keyed_verification() -> None:
    """Treat package-keyed import verification as success for requested packages."""
    assert install_module._required_import_status(
        ["beautifulsoup4", "scikit-learn>=1.5"],
        {"beautifulsoup4": True, "scikit-learn>=1.5": True},
    ) == {"beautifulsoup4": True, "scikit-learn>=1.5": True}
    assert install_module._required_import_status(
        ["pillow", "opencv-python"],
        {"pillow": True, "opencv-python": True},
    ) == {"pillow": True, "opencv-python": True}


def test_install_missing_verification_metadata_marks_node_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed when the runner omits verification metadata for a request."""
    config_path = _write_config(tmp_path / "install.toml")
    playbook_runner = FakePlaybookRunner(
        responses=[
            [
                *(_result_for_all_nodes(success=True,
                  python_packages=["pandas"])[:-1]),
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=True,
                    python_packages=("pandas",),
                    verification={},
                ),
            ],
            [
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=True,
                    python_packages=("pandas",),
                    verification={},
                    attempts=2,
                )
            ],
            [
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=True,
                    python_packages=("pandas",),
                    verification={},
                    attempts=3,
                )
            ],
            [
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=True,
                    python_packages=("pandas",),
                    verification={},
                    attempts=4,
                )
            ],
        ]
    )
    _, _, fake_uploader = _install_fakes(
        monkeypatch, playbook_runner=playbook_runner)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--python", "pandas"],
    )

    assert result.exit_code == 1
    manifest = _uploaded_manifest(fake_uploader)
    failed_nodes = [
        node for node in _uploaded_nodes(fake_uploader) if not node["success"]
    ]
    assert manifest["success_count"] == 3
    assert manifest["failure_count"] == 1
    assert [node["hostname"]
            for node in failed_nodes] == ["test-cluster-worker-03"]
    assert failed_nodes[0]["errors"] == [
        "python package verification failed: pandas"]


def test_install_retries_failed_nodes_with_exponential_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry only failed nodes with 1/2/4 second backoff before partial failure."""
    config_path = _write_config(tmp_path / "install.toml")
    sleep_calls: list[float] = []
    playbook_runner = FakePlaybookRunner(
        responses=[
            [
                *_result_for_all_nodes(success=True,
                                       python_packages=["pandas"])[:-1],
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=False,
                    python_packages=("pandas",),
                    verification={"python": {"pandas": False}},
                    error="host unreachable",
                ),
            ],
            [
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=False,
                    python_packages=("pandas",),
                    verification={"python": {"pandas": False}},
                    error="host unreachable",
                    attempts=2,
                )
            ],
            [
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=False,
                    python_packages=("pandas",),
                    verification={"python": {"pandas": False}},
                    error="host unreachable",
                    attempts=3,
                )
            ],
            [
                FakeNodeResult(
                    node_name="test-cluster-worker-03",
                    host="10.0.0.23",
                    success=False,
                    python_packages=("pandas",),
                    verification={"python": {"pandas": False}},
                    error="host unreachable",
                    attempts=4,
                )
            ],
        ]
    )
    _, fake_playbook_runner, fake_uploader = _install_fakes(
        monkeypatch,
        playbook_runner=playbook_runner,
        sleep_calls=sleep_calls,
    )

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--python", "pandas"],
    )

    assert result.exit_code == 1
    assert [
        len(call["inventory"]["all"]["hosts"]) for call in fake_playbook_runner.calls
    ] == [4, 1, 1, 1]
    assert sleep_calls == [1.0, 2.0, 4.0]
    manifest = _uploaded_manifest(fake_uploader)
    assert manifest["success_count"] == 3
    assert manifest["failure_count"] == 1


def test_install_is_idempotent_when_re_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow repeated installs with no changes and no failures."""
    config_path = _write_config(tmp_path / "install.toml")
    playbook_runner = FakePlaybookRunner(
        responses=[
            _result_for_all_nodes(
                success=True,
                python_packages=["pandas"],
                changed=False,
            ),
            _result_for_all_nodes(
                success=True,
                python_packages=["pandas"],
                changed=False,
            ),
        ]
    )
    _, fake_playbook_runner, _ = _install_fakes(
        monkeypatch,
        playbook_runner=playbook_runner,
    )

    first = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--python", "pandas"],
    )
    second = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--python", "pandas"],
    )

    assert first.exit_code == 0
    assert second.exit_code == 0
    assert fake_playbook_runner.calls[0]["python_packages"] == ["pandas"]
    assert fake_playbook_runner.calls[1]["python_packages"] == ["pandas"]


def test_install_uses_explicit_ssh_key_when_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass the configured SSH private key path to the install executor."""
    config_path = _write_config(tmp_path / "install.toml")
    ssh_key_path = tmp_path / "cluster-key"
    ssh_key_path.write_text("PRIVATE KEY", encoding="utf-8")
    _, fake_playbook_runner, _ = _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        [
            "install",
            "--config",
            str(config_path),
            "--system",
            "mc",
            "--ssh-key",
            str(ssh_key_path),
        ],
    )

    assert result.exit_code == 0
    assert fake_playbook_runner.calls[0]["ssh_key_path"] == ssh_key_path


def test_install_logs_progress_stages_to_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Log the documented install progress stages to stderr."""
    config_path = _write_config(tmp_path / "install.toml")
    _install_fakes(monkeypatch)

    result = runner.invoke(
        app,
        ["install", "--config", str(config_path), "--system", "mc"],
    )

    assert result.exit_code == 0
    assert "config loaded" in result.stderr
    assert "resolving nodes" in result.stderr
    assert "running ansible" in result.stderr
    assert "verifying packages" in result.stderr
    assert "uploading rollout manifest" in result.stderr
