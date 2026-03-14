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

"""Acceptance tests for the H1 status CLI command."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from hailstack.cli.commands import status as status_module
from hailstack.cli.main import app
from hailstack.errors import PulumiError

runner = CliRunner()


class FakeStatusStackRunner:
    """Return deterministic Pulumi stack outputs for status tests."""

    def __init__(
        self,
        *,
        outputs: dict[str, object] | None = None,
        error: Exception | None = None,
    ) -> None:
        """Initialise fake status responses."""
        self.outputs = outputs or {
            "cluster_name": "test-cluster",
            "bundle_id": "bundle-from-stack",
            "master_public_ip": "198.51.100.10",
            "master_private_ip": "10.0.0.10",
            "master_flavour": "m2.2xlarge",
            "monitoring_enabled": True,
            "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
            "worker_names": [
                "test-cluster-worker-01",
                "test-cluster-worker-02",
                "test-cluster-worker-03",
            ],
            "attached_volume_name": "my-data-vol",
            "managed_volume_size_gb": 500,
        }
        self.error = error
        self.calls = 0

    def get_status_outputs(self, config: object) -> dict[str, object]:
        """Return a stable stack output payload or raise an error."""
        del config
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.outputs


class FakeStatusProbe:
    """Capture detailed status probes and return fake probe output."""

    def __init__(
        self,
        *,
        detailed_status: object | None = None,
    ) -> None:
        """Initialise fake detailed probe responses."""
        self.detailed_status = detailed_status
        self.calls: list[dict[str, object]] = []

    def probe(
        self,
        inventory: object,
        *,
        ssh_username: str,
        ssh_key_path: Path | None = None,
    ) -> object:
        """Record the requested inventory and return the configured snapshot."""
        self.calls.append(
            {
                "inventory": inventory,
                "ssh_username": ssh_username,
                "ssh_key_path": ssh_key_path,
            }
        )
        return self.detailed_status


def _write_config(path: Path) -> Path:
    """Write a minimal valid status configuration file."""
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            'bundle = "bundle-from-config"\n'
            "num_workers = 3\n"
            'master_flavour = "m2.2xlarge"\n'
            'worker_flavour = "m2.xlarge"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n\n'
            "[volumes]\n"
            "create = true\n"
            'name = "my-data-vol"\n'
            "size_gb = 500\n\n"
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


def _detailed_status(*, unreachable_worker: bool = False) -> object:
    """Return a reusable fake detailed status snapshot."""
    services = [
        status_module.ServiceStatus(
            name="spark-master",
            status="active",
            node="master",
        ),
        status_module.ServiceStatus(
            name="hdfs-namenode",
            status="active",
            node="master",
        ),
        status_module.ServiceStatus(
            name="spark-worker",
            status="active",
            node="worker-01",
        ),
        status_module.ServiceStatus(
            name="spark-worker",
            status="active",
            node="worker-03",
        ),
        status_module.ServiceStatus(
            name="yarn-nm",
            status="active",
            node="worker-01",
        ),
        status_module.ServiceStatus(
            name="yarn-nm",
            status="active",
            node="worker-03",
        ),
    ]
    resources = [
        status_module.NodeResources(
            node="master",
            status="ok",
            cpu_percent=23.0,
            memory_percent=45.0,
            disk_percent=12.0,
        ),
        status_module.NodeResources(
            node="worker-01",
            status="ok",
            cpu_percent=67.0,
            memory_percent=78.0,
            disk_percent=8.0,
        ),
        status_module.NodeResources(
            node="worker-03",
            status="ok",
            cpu_percent=12.0,
            memory_percent=34.0,
            disk_percent=8.0,
        ),
    ]
    if unreachable_worker:
        services.extend(
            [
                status_module.ServiceStatus(
                    name="spark-worker",
                    status="unreachable",
                    node="worker-02",
                ),
                status_module.ServiceStatus(
                    name="yarn-nm",
                    status="unreachable",
                    node="worker-02",
                ),
            ]
        )
        resources.append(
            status_module.NodeResources(
                node="worker-02",
                status="unreachable",
                cpu_percent=None,
                memory_percent=None,
                disk_percent=None,
            )
        )
    else:
        services.extend(
            [
                status_module.ServiceStatus(
                    name="spark-worker",
                    status="active",
                    node="worker-02",
                ),
                status_module.ServiceStatus(
                    name="yarn-nm",
                    status="active",
                    node="worker-02",
                ),
            ]
        )
        resources.append(
            status_module.NodeResources(
                node="worker-02",
                status="ok",
                cpu_percent=55.0,
                memory_percent=62.0,
                disk_percent=8.0,
            )
        )
    return status_module.DetailedClusterStatus(services=services, resources=resources)


def _install_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stack_runner: FakeStatusStackRunner | None = None,
    probe: FakeStatusProbe | None = None,
) -> tuple[FakeStatusStackRunner, FakeStatusProbe]:
    """Install fake status dependencies into the command module."""
    fake_stack_runner = stack_runner or FakeStatusStackRunner()
    fake_probe = probe or FakeStatusProbe(detailed_status=_detailed_status())
    monkeypatch.setattr(
        status_module,
        "create_status_stack_runner",
        lambda logger: fake_stack_runner,
    )
    monkeypatch.setattr(
        status_module,
        "create_status_probe",
        lambda: fake_probe,
    )
    return fake_stack_runner, fake_probe


def test_status_default_output_uses_pulumi_stack_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render the documented summary from Pulumi outputs without probing SSH."""
    config_path = _write_config(tmp_path / "status.toml")
    fake_stack_runner, fake_probe = _install_fakes(monkeypatch)

    result = runner.invoke(app, ["status", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Cluster: test-cluster" in result.stdout
    assert "Bundle:  bundle-from-stack" in result.stdout
    assert "Master:  198.51.100.10 (m2.2xlarge)" in result.stdout
    assert "Workers: 3" in result.stdout
    assert "  worker-01: 10.0.0.21" in result.stdout
    assert "  worker-02: 10.0.0.22" in result.stdout
    assert "  worker-03: 10.0.0.23" in result.stdout
    assert "Volume:  my-data-vol (500GB)" in result.stdout
    assert fake_stack_runner.calls == 1
    assert fake_probe.calls == []


def test_status_detailed_human_output_includes_service_statuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Add grouped systemd service probe results when --detailed is requested."""
    config_path = _write_config(tmp_path / "status.toml")
    _, fake_probe = _install_fakes(
        monkeypatch,
        probe=FakeStatusProbe(detailed_status=_detailed_status()),
    )

    result = runner.invoke(
        app, ["status", "--config", str(config_path), "--detailed"])

    assert result.exit_code == 0
    assert "Services:" in result.stdout
    assert "spark-master:" in result.stdout
    assert "active (master)" in result.stdout
    assert "spark-worker:" in result.stdout
    assert "worker-01, worker-02, worker-03" in result.stdout
    assert fake_probe.calls[0]["ssh_username"] == "ubuntu"
    worker_targets = [
        node for node in fake_probe.calls[0]["inventory"] if node.role == "worker"
    ]
    assert [node.jump_host for node in worker_targets] == [
        "198.51.100.10",
        "198.51.100.10",
        "198.51.100.10",
    ]


def test_status_detailed_human_output_includes_resource_usage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Add per-node CPU, memory, and disk usage when --detailed is requested."""
    config_path = _write_config(tmp_path / "status.toml")
    _install_fakes(
        monkeypatch,
        probe=FakeStatusProbe(detailed_status=_detailed_status()),
    )

    result = runner.invoke(
        app, ["status", "--config", str(config_path), "--detailed"])

    assert result.exit_code == 0
    assert "Resources:" in result.stdout
    assert "master:    CPU 23%  MEM 45%  DISK 12%" in result.stdout
    assert "worker-01: CPU 67%  MEM 78%  DISK 8%" in result.stdout
    assert "worker-02: CPU 55%  MEM 62%  DISK 8%" in result.stdout
    assert "worker-03: CPU 12%  MEM 34%  DISK 8%" in result.stdout


def test_status_detailed_passes_explicit_ssh_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thread --ssh-key through the detailed SSH status probe."""
    config_path = _write_config(tmp_path / "status.toml")
    ssh_key_path = tmp_path / "cluster-key"
    ssh_key_path.write_text("PRIVATE KEY", encoding="utf-8")
    _, fake_probe = _install_fakes(
        monkeypatch,
        probe=FakeStatusProbe(detailed_status=_detailed_status()),
    )

    result = runner.invoke(
        app,
        [
            "status",
            "--config",
            str(config_path),
            "--detailed",
            "--ssh-key",
            str(ssh_key_path),
        ],
    )

    assert result.exit_code == 0
    assert fake_probe.calls[0]["ssh_key_path"] == ssh_key_path


def test_status_json_output_is_machine_readable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit the documented summary payload as valid JSON when --json is used."""
    config_path = _write_config(tmp_path / "status.toml")
    _, fake_probe = _install_fakes(monkeypatch)

    result = runner.invoke(
        app, ["status", "--config", str(config_path), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == {
        "bundle": "bundle-from-stack",
        "cluster_name": "test-cluster",
        "master": {
            "flavour": "m2.2xlarge",
            "ip": "198.51.100.10",
            "name": "master",
        },
        "volume": {"name": "my-data-vol", "size_gb": 500},
        "worker_count": 3,
        "workers": [
            {"ip": "10.0.0.21", "name": "worker-01"},
            {"ip": "10.0.0.22", "name": "worker-02"},
            {"ip": "10.0.0.23", "name": "worker-03"},
        ],
    }
    assert fake_probe.calls == []


def test_status_json_detailed_output_includes_services_and_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Include detailed service and resource sections in JSON when requested."""
    config_path = _write_config(tmp_path / "status.toml")
    _install_fakes(
        monkeypatch,
        probe=FakeStatusProbe(detailed_status=_detailed_status()),
    )

    result = runner.invoke(
        app,
        ["status", "--config", str(config_path), "--json", "--detailed"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["services"][0] == {
        "name": "hdfs-namenode",
        "nodes": ["master"],
        "status": "active",
    }
    assert payload["resources"][0] == {
        "cpu_percent": 23.0,
        "disk_percent": 12.0,
        "memory_percent": 45.0,
        "node": "master",
        "status": "ok",
    }
    assert any(service["name"] ==
               "spark-worker" for service in payload["services"])
    assert any(resource["node"] ==
               "worker-02" for resource in payload["resources"])


def test_status_prefers_deployed_outputs_over_drifted_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render deployed flavour and volume details from stack outputs."""
    config_path = _write_config(tmp_path / "status.toml")
    _install_fakes(
        monkeypatch,
        stack_runner=FakeStatusStackRunner(
            outputs={
                "cluster_name": "test-cluster",
                "bundle_id": "bundle-from-stack",
                "master_public_ip": "198.51.100.10",
                "master_private_ip": "10.0.0.10",
                "master_flavour": "m2.4xlarge",
                "monitoring_enabled": True,
                "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
                "worker_names": [
                    "test-cluster-worker-01",
                    "test-cluster-worker-02",
                    "test-cluster-worker-03",
                ],
                "attached_volume_name": "deployed-data-vol",
                "managed_volume_size_gb": 750,
            }
        ),
    )

    result = runner.invoke(app, ["status", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Master:  198.51.100.10 (m2.4xlarge)" in result.stdout
    assert "Volume:  deployed-data-vol (750GB)" in result.stdout


def test_status_uses_public_ip_without_requiring_private_ip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow older stack outputs that only export the public master address."""
    config_path = _write_config(tmp_path / "status.toml")
    _install_fakes(
        monkeypatch,
        stack_runner=FakeStatusStackRunner(
            outputs={
                "cluster_name": "test-cluster",
                "bundle_id": "bundle-from-stack",
                "master_public_ip": "198.51.100.10",
                "master_flavour": "m2.2xlarge",
                "monitoring_enabled": True,
                "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
                "worker_names": [
                    "test-cluster-worker-01",
                    "test-cluster-worker-02",
                    "test-cluster-worker-03",
                ],
            }
        ),
    )

    result = runner.invoke(app, ["status", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Master:  198.51.100.10 (m2.2xlarge)" in result.stdout


def test_status_detailed_prefers_deployed_service_metadata_over_drifted_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Probe services from deployed metadata rather than drifted local config."""
    config_path = tmp_path / "status.toml"
    config_path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            'bundle = "bundle-from-config"\n'
            "num_workers = 3\n"
            'master_flavour = "m2.2xlarge"\n'
            'worker_flavour = "m2.xlarge"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n'
            'monitoring = "none"\n\n'
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
    _, fake_probe = _install_fakes(
        monkeypatch,
        stack_runner=FakeStatusStackRunner(
            outputs={
                "cluster_name": "test-cluster",
                "bundle_id": "bundle-from-stack",
                "master_public_ip": "198.51.100.10",
                "master_private_ip": "10.0.0.10",
                "master_flavour": "m2.2xlarge",
                "monitoring_enabled": True,
                "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
                "worker_names": [
                    "test-cluster-worker-01",
                    "test-cluster-worker-02",
                    "test-cluster-worker-03",
                ],
                "attached_volume_name": "deployed-data-vol",
                "managed_volume_size_gb": 750,
            }
        ),
        probe=FakeStatusProbe(detailed_status=_detailed_status()),
    )

    result = runner.invoke(
        app, ["status", "--config", str(config_path), "--detailed"])

    assert result.exit_code == 0
    inventory = fake_probe.calls[0]["inventory"]
    master_node = next(node for node in inventory if node.role == "master")
    worker_node = next(node for node in inventory if node.role == "worker")
    assert "netdata" in master_node.services
    assert "nfs-server" in master_node.services
    assert "netdata" in worker_node.services


def test_status_suppresses_drifted_volume_when_stack_outputs_report_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat deployed volume metadata as authoritative when no volume is attached."""
    config_path = _write_config(tmp_path / "status.toml")
    _install_fakes(
        monkeypatch,
        stack_runner=FakeStatusStackRunner(
            outputs={
                "cluster_name": "test-cluster",
                "bundle_id": "bundle-from-stack",
                "master_public_ip": "198.51.100.10",
                "master_private_ip": "10.0.0.10",
                "master_flavour": "m2.4xlarge",
                "monitoring_enabled": True,
                "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
                "worker_names": [
                    "test-cluster-worker-01",
                    "test-cluster-worker-02",
                    "test-cluster-worker-03",
                ],
                "managed_volume_size_gb": 0,
            }
        ),
    )

    result = runner.invoke(app, ["status", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Volume:" not in result.stdout


def test_status_detailed_skips_nfs_probe_when_stack_outputs_report_no_volume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid probing NFS when deployed outputs explicitly report no volume."""
    config_path = _write_config(tmp_path / "status.toml")
    _, fake_probe = _install_fakes(
        monkeypatch,
        stack_runner=FakeStatusStackRunner(
            outputs={
                "cluster_name": "test-cluster",
                "bundle_id": "bundle-from-stack",
                "master_public_ip": "198.51.100.10",
                "master_private_ip": "10.0.0.10",
                "master_flavour": "m2.2xlarge",
                "monitoring_enabled": True,
                "worker_private_ips": ["10.0.0.21", "10.0.0.22", "10.0.0.23"],
                "worker_names": [
                    "test-cluster-worker-01",
                    "test-cluster-worker-02",
                    "test-cluster-worker-03",
                ],
                "managed_volume_size_gb": 0,
            }
        ),
        probe=FakeStatusProbe(detailed_status=_detailed_status()),
    )

    result = runner.invoke(
        app,
        ["status", "--config", str(config_path), "--detailed"],
    )

    assert result.exit_code == 0
    inventory = fake_probe.calls[0]["inventory"]
    master_node = next(node for node in inventory if node.role == "master")
    assert "nfs-server" not in master_node.services


def test_status_cluster_not_found_raises_documented_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise the documented cluster-not-found error when Pulumi state is absent."""
    config_path = _write_config(tmp_path / "status.toml")
    _install_fakes(
        monkeypatch,
        stack_runner=FakeStatusStackRunner(
            error=PulumiError("Cluster not found")),
    )

    result = runner.invoke(app, ["status", "--config", str(config_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, PulumiError)
    assert str(result.exception) == "Cluster not found"


def test_status_detailed_marks_unreachable_workers_without_failing_others(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Show unreachable worker nodes while preserving successful results for others."""
    config_path = _write_config(tmp_path / "status.toml")
    _install_fakes(
        monkeypatch,
        probe=FakeStatusProbe(
            detailed_status=_detailed_status(unreachable_worker=True)
        ),
    )

    result = runner.invoke(
        app, ["status", "--config", str(config_path), "--detailed"])

    assert result.exit_code == 0
    assert "spark-worker:" in result.stdout
    assert "unreachable (worker-02)" in result.stdout
    assert "worker-01, worker-03" in result.stdout
    assert "worker-02: unreachable" in result.stdout


def test_status_detailed_surfaces_ssh_auth_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise credential errors instead of rendering them as unreachable nodes."""

    async def fake_check_service_health_targets(
        *,
        hosts: object,
        ssh_username: str,
        services: object,
        ssh_key_path: Path | None = None,
    ) -> list[object]:
        del hosts, ssh_username, services, ssh_key_path
        return [status_module.SSHError("worker-01: Permission denied (publickey)")]

    async def fake_gather_resource_usage_targets(
        *,
        hosts: object,
        ssh_username: str,
        ssh_key_path: Path | None = None,
    ) -> list[object]:
        del hosts, ssh_username, ssh_key_path
        return [status_module.SSHError("worker-01: Permission denied (publickey)")]

    monkeypatch.setattr(
        status_module,
        "check_service_health_targets",
        fake_check_service_health_targets,
    )
    monkeypatch.setattr(
        status_module,
        "gather_resource_usage_targets",
        fake_gather_resource_usage_targets,
    )

    probe = status_module.SSHStatusProbe()
    inventory = [
        status_module.StatusNode(
            name="worker-01",
            host="10.0.0.21",
            role="worker",
            services=("spark-worker",),
            jump_host="198.51.100.10",
        )
    ]

    with pytest.raises(status_module.SSHError, match="Permission denied"):
        probe.probe(inventory, ssh_username="ubuntu")


def test_status_probe_treats_legacy_jupyter_service_name_as_jupyter_lab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Collapse legacy hailstack-jupyterlab probes into the JupyterLab status row."""

    async def fake_check_service_health_targets(
        *,
        hosts: object,
        ssh_username: str,
        services: object,
        ssh_key_path: Path | None = None,
    ) -> list[object]:
        del hosts, ssh_username, ssh_key_path
        assert services == {"master": ("jupyter-lab", "hailstack-jupyterlab")}
        return [
            SimpleNamespace(name="jupyter-lab", active=False, node="master"),
            SimpleNamespace(name="hailstack-jupyterlab",
                            active=True, node="master"),
        ]

    async def fake_gather_resource_usage_targets(
        *,
        hosts: object,
        ssh_username: str,
        ssh_key_path: Path | None = None,
    ) -> list[object]:
        del hosts, ssh_username, ssh_key_path
        return [
            SimpleNamespace(
                hostname="master",
                cpu_percent=23.0,
                memory_percent=45.0,
                disk_percent=12.0,
            )
        ]

    monkeypatch.setattr(
        status_module,
        "check_service_health_targets",
        fake_check_service_health_targets,
    )
    monkeypatch.setattr(
        status_module,
        "gather_resource_usage_targets",
        fake_gather_resource_usage_targets,
    )

    probe = status_module.SSHStatusProbe()
    result = probe.probe(
        [
            status_module.StatusNode(
                name="master",
                host="198.51.100.10",
                role="master",
                services=("jupyter-lab", "hailstack-jupyterlab"),
            )
        ],
        ssh_username="ubuntu",
    )

    assert result.services == [
        status_module.ServiceStatus(
            name="jupyter-lab",
            status="active",
            node="master",
        )
    ]
