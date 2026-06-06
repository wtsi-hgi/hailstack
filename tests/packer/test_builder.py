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

"""Acceptance tests for packer image building."""

import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID

import pytest

import hailstack.packer.builder as packer_builder
from hailstack.config.compatibility import Bundle
from hailstack.config.parser import load_config
from hailstack.errors import PackerError
from hailstack.packer.builder import (
    PACKER_ROOT_PATH,
    PACKER_SCRIPTS_PATH,
    PACKER_TEMPLATE_PATH,
    REQUIRED_PACKER_SCRIPT_PATHS,
    _packer_vars,
    _resolve_openstack_network_id,
    _run_packer,
    build_image,
)

NETWORK_UUID = "11111111-2222-3333-4444-555555555555"
LUSTRE_NETWORK_UUID = "33333333-4444-5555-6666-777777777777"
RESOLVED_NETWORK_UUID = "22222222-3333-4444-5555-666666666666"
RESOLVED_LUSTRE_NETWORK_UUID = "44444444-5555-6666-7777-888888888888"
MANAGEMENT_PORT_UUID = "55555555-6666-7777-8888-999999999999"
PACKER_APT_HELPER_RELATIVE_PATH = Path("scripts/apt-locks.sh")
PACKER_APT_HELPER_PATH = PACKER_ROOT_PATH / PACKER_APT_HELPER_RELATIVE_PATH
PACKER_REMOTE_APT_HELPER_PATH = "/tmp/hailstack-packer-apt-locks.sh"
PACKER_APT_SCRIPT_RELATIVE_PATHS = (
    Path("scripts/base.sh"),
    Path("scripts/ubuntu/packages.sh"),
    Path("scripts/ubuntu/netdata.sh"),
)


def _write_config(
    path: Path,
    *,
    network_name: str = NETWORK_UUID,
    lustre_network: str = "",
    cluster_floating_ip_pool: str = "",
    packer_floating_ip_pool: str = "",
    gnomad_methods_version: str = "",
) -> Path:
    """Write a minimal build-image config file."""
    lustre_network_line = (
        f'lustre_network = "{lustre_network}"\n' if lustre_network != "" else ""
    )
    cluster_pool_line = (
        f'floating_ip_pool = "{cluster_floating_ip_pool}"\n'
        if cluster_floating_ip_pool
        else ""
    )
    packer_pool_line = (
        f'floating_ip_pool = "{packer_floating_ip_pool}"\n'
        if packer_floating_ip_pool
        else ""
    )
    gnomad_methods_line = (
        f'gnomad_methods_version = "{gnomad_methods_version}"\n'
        if gnomad_methods_version
        else ""
    )
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            'master_flavour = "m2.medium"\n'
            f'network_name = "{network_name}"\n'
            f"{lustre_network_line}"
            f"{cluster_pool_line}"
            'ssh_username = "ubuntu"\n\n'
            "[packer]\n"
            'base_image = "ubuntu-22.04"\n'
            'flavour = "m2.large"\n'
            f"{packer_pool_line}"
            f"{gnomad_methods_line}\n"
            "[ssh_keys]\n"
            'public_keys = ["ssh-rsa AAAA"]\n\n'
            "[s3]\n"
            'access_key = "secret-access"\n'
            'secret_key = "secret-secret"\n'
        ),
        encoding="utf-8",
    )
    return path


def _bundle() -> Bundle:
    """Return a representative bundle fixture."""
    return Bundle(
        id="hail-0.2.137-gnomad-3.0.4-r2",
        hail="0.2.137",
        spark="3.5.6",
        hadoop="3.4.1",
        java="11",
        python="3.12",
        scala="2.12.18",
        gnomad="3.0.4",
        status="latest",
    )


def _result(
    stdout: str,
    stderr: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    """Build a text-mode CompletedProcess for runner fakes."""
    return subprocess.CompletedProcess(
        args=["packer", "build"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


class _RecordingSecurityGroupManager:
    """Record temporary security-group lifecycle calls for builder tests."""

    def __init__(self, name: str = "hailstack-packer-ssh-test") -> None:
        self.name = name
        self.events: list[str] = []

    def create(self) -> str:
        """Create a fake temporary security group."""
        self.events.append("create")
        return self.name

    def cleanup(self, security_group_name: str) -> None:
        """Delete a fake temporary security group."""
        self.events.append(f"cleanup:{security_group_name}")


class _SharedRecordingSecurityGroupManager:
    """Record security-group lifecycle calls into a shared event list."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.name = "hailstack-packer-ssh-test"

    def create(self) -> str:
        """Create a fake temporary security group."""
        self.events.append("sg:create")
        return self.name

    def cleanup(self, security_group_name: str) -> None:
        """Delete a fake temporary security group."""
        self.events.append(f"sg:cleanup:{security_group_name}")


class _RecordingPortManager:
    """Record temporary Packer port lifecycle calls for builder tests."""

    def __init__(
        self,
        events: list[str],
        *,
        fail_create: str = "",
    ) -> None:
        self.events = events
        self.fail_create = fail_create

    def create_management_port(
        self,
        *,
        network_id: str,
        security_group_name: str,
    ) -> str:
        """Create a fake management port with SSH security groups."""
        if self.fail_create == "management":
            raise PackerError(
                "Could not create temporary Packer management port before "
                "launching Packer. Check OpenStack port/security group "
                "quota/permissions and credentials."
            )
        self.events.append(f"port:create-management:{network_id}:{security_group_name}")
        return MANAGEMENT_PORT_UUID

    def cleanup(self, port_id: str) -> None:
        """Delete a fake temporary port."""
        self.events.append(f"port:cleanup:{port_id}")


def test_default_packer_runner_stops_on_ssh_no_route_debug_log(
    tmp_path: Path,
) -> None:
    """Stop waiting when Packer's SSH debug log proves the fixed IP is unreachable."""
    script = (
        "import os, pathlib, time\n"
        "log_path = pathlib.Path(os.environ['PACKER_LOG_PATH'])\n"
        "log_path.write_text("
        "'2026/06/05 TCP connection to SSH ip/port failed: "
        "dial tcp 192.168.252.82:22: connect: no route to host\\n', "
        "encoding='utf-8'"
        ")\n"
        "time.sleep(2)\n"
    )

    start_time = time.monotonic()
    result = _run_packer([sys.executable, "-c", script], cwd=tmp_path)
    elapsed_seconds = time.monotonic() - start_time

    assert result.returncode != 0
    assert elapsed_seconds < 1.5
    assert "dial tcp 192.168.252.82:22: connect: no route to host" in result.stderr


def _write_template_assets(
    tmp_path: Path,
    *,
    include_apt_helper: bool = True,
) -> Path:
    """Create a minimal template tree for unit tests that stub out the runner."""
    template_path = tmp_path / "hailstack.pkr.hcl"
    template_path.write_text("build {}\n", encoding="utf-8")

    for script_path in REQUIRED_PACKER_SCRIPT_PATHS:
        relative_path = script_path.relative_to(PACKER_SCRIPTS_PATH.parent)
        target_path = tmp_path / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\n",
            encoding="utf-8",
        )
        target_path.chmod(0o755)

    if include_apt_helper:
        target_path = tmp_path / PACKER_APT_HELPER_RELATIVE_PATH
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\n",
            encoding="utf-8",
        )
        target_path.chmod(0o755)

    return template_path


def test_build_image_resolves_network_name_to_uuid_before_packer(
    tmp_path: Path,
) -> None:
    """Pass Packer the UUID for a configured OpenStack network name."""
    config = load_config(
        _write_config(tmp_path / "cluster.toml", network_name="cloudforms_network")
    )
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []
    requested_networks: list[str] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    def fake_network_resolver(network_name: str) -> str:
        requested_networks.append(network_name)
        return RESOLVED_NETWORK_UUID

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        network_resolver=fake_network_resolver,
    )

    command = recorded_commands[0]
    assert requested_networks == ["cloudforms_network"]
    assert f"network={RESOLVED_NETWORK_UUID}" in command
    assert "network=cloudforms_network" not in command


def test_build_image_resolves_lustre_network_name_to_uuid_before_packer(
    tmp_path: Path,
) -> None:
    """Attach the configured Lustre network to temporary build instances."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            network_name="cloudforms_network",
            lustre_network="lustre-hgi01",
        )
    )
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []
    requested_networks: list[str] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    def fake_network_resolver(network_name: str) -> str:
        requested_networks.append(network_name)
        return {
            "cloudforms_network": RESOLVED_NETWORK_UUID,
            "lustre-hgi01": RESOLVED_LUSTRE_NETWORK_UUID,
        }[network_name]

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        network_resolver=fake_network_resolver,
    )

    command = recorded_commands[0]
    assert requested_networks == ["cloudforms_network", "lustre-hgi01"]
    assert f"network={RESOLVED_NETWORK_UUID}" in command
    assert f"lustre_network={RESOLVED_LUSTRE_NETWORK_UUID}" in command
    assert "network=cloudforms_network" not in command
    assert "lustre_network=lustre-hgi01" not in command


def test_build_image_fails_before_runner_when_lustre_network_cannot_resolve(
    tmp_path: Path,
) -> None:
    """Raise a clear PackerError before launching Packer with a bad Lustre net."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            lustre_network="missing-lustre",
        )
    )
    template_path = _write_template_assets(tmp_path)
    runner_called = False

    def fail_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner should not be called")

    def fail_network_resolver(network_name: str) -> str:
        raise PackerError(
            f"Could not resolve OpenStack network '{network_name}' to the UUID "
            "Packer requires. Run `openstack network list` and check "
            "OpenStack credentials."
        )

    with pytest.raises(PackerError) as raised:
        build_image(
            config,
            _bundle(),
            runner=fail_runner,
            template_path=template_path,
            network_resolver=fail_network_resolver,
        )

    message = str(raised.value)
    assert "missing-lustre" in message
    assert "openstack network list" in message
    assert "credentials" in message
    assert not runner_called


def test_build_image_keeps_single_network_when_lustre_network_is_blank(
    tmp_path: Path,
) -> None:
    """Treat blank Lustre network config as unset for Packer builds."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            lustre_network="   ",
        )
    )
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    command = recorded_commands[0]
    assert f"network={NETWORK_UUID}" in command
    assert "lustre_network=" in command
    assert "lustre_network=   " not in command


def test_build_image_passes_lustre_network_uuid_without_resolver_call(
    tmp_path: Path,
) -> None:
    """Keep UUID-based Lustre network configs working without OpenStack lookup."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            lustre_network=LUSTRE_NETWORK_UUID,
        )
    )
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    def fail_network_resolver(network_name: str) -> str:
        raise AssertionError(f"resolver should not be called for {network_name}")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        network_resolver=fail_network_resolver,
    )

    command = recorded_commands[0]
    assert f"network={NETWORK_UUID}" in command
    assert f"lustre_network={LUSTRE_NETWORK_UUID}" in command


def test_build_image_fails_before_runner_when_network_name_cannot_resolve(
    tmp_path: Path,
) -> None:
    """Raise a clear PackerError before Packer launches with a bad network."""
    config = load_config(
        _write_config(tmp_path / "cluster.toml", network_name="cloudforms_network")
    )
    template_path = _write_template_assets(tmp_path)
    runner_called = False

    def fail_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner should not be called")

    def fail_network_resolver(network_name: str) -> str:
        raise PackerError(
            f"Could not resolve OpenStack network '{network_name}' to the UUID "
            "Packer requires. Run `openstack network list` and check "
            "OpenStack credentials."
        )

    with pytest.raises(PackerError) as raised:
        build_image(
            config,
            _bundle(),
            runner=fail_runner,
            template_path=template_path,
            network_resolver=fail_network_resolver,
        )

    message = str(raised.value)
    assert "cloudforms_network" in message
    assert "openstack network list" in message
    assert "credentials" in message
    assert not runner_called


def test_build_image_passes_configured_network_uuid_without_resolver_call(
    tmp_path: Path,
) -> None:
    """Keep existing UUID-based cluster.network_name configs working."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    def fail_network_resolver(network_name: str) -> str:
        raise AssertionError(f"resolver should not be called for {network_name}")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        network_resolver=fail_network_resolver,
    )

    assert f"network={NETWORK_UUID}" in recorded_commands[0]


def test_openstack_network_resolver_returns_cli_network_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve a friendly network name through the OpenStack CLI JSON output."""
    recorded_commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output
        assert text
        assert not check
        recorded_commands.append(command)
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=f'{{"id": "{RESOLVED_NETWORK_UUID}"}}\n',
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert _resolve_openstack_network_id("cloudforms_network") == RESOLVED_NETWORK_UUID
    assert recorded_commands == [
        ["openstack", "network", "show", "cloudforms_network", "-f", "json"]
    ]


def test_openstack_network_resolver_failure_names_network_and_suggests_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explain network lookup failures before Packer reaches Nova."""

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, check
        return subprocess.CompletedProcess(
            args=command,
            returncode=1,
            stdout="",
            stderr="No Network found for cloudforms_network",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(PackerError) as raised:
        _resolve_openstack_network_id("cloudforms_network")

    message = str(raised.value)
    assert "cloudforms_network" in message
    assert "openstack network list" in message
    assert "credentials" in message


def _repo_template_script_entries() -> set[str]:
    """Return shell provisioner script entries from the checked-in template."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    scripts_block = re.search(r"scripts\s*=\s*\[(?P<body>.*?)\]", template, re.S)
    assert scripts_block is not None

    return set(re.findall(r'"([^"]+)"', scripts_block.group("body")))


def _repo_shell_provisioner_block() -> str:
    """Return the checked-in shell provisioner body."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    provisioner_block = re.search(
        r'provisioner\s+"shell"\s*\{(?P<body>.*?)\n\s*\}',
        template,
        re.S,
    )
    assert provisioner_block is not None

    return provisioner_block["body"]


def _repo_openstack_source_block() -> str:
    """Return the checked-in OpenStack source body."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    source_block = re.search(
        r'source\s+"openstack"\s+"hailstack"\s*\{(?P<body>.*?)\n\}',
        template,
        re.S,
    )
    assert source_block is not None

    return source_block["body"]


def test_checked_in_openstack_builder_uses_config_drive() -> None:
    """Deliver Packer's temporary SSH key through Nova config drive metadata."""
    source_body = _repo_openstack_source_block()

    assert re.search(r"^\s*config_drive\s*=\s*true\s*$", source_body, re.M)


def test_checked_in_openstack_builder_targets_floating_ip_management_net() -> None:
    """Associate Packer floating IPs with the management/cloudforms network."""
    source_body = _repo_openstack_source_block()

    assert re.search(
        r"^\s*instance_floating_ip_net\s*=\s*var\.network\s*$",
        source_body,
        re.M,
    )


def test_checked_in_openstack_builder_attaches_configured_lustre_network() -> None:
    """Attach the optional Lustre network without losing the management network."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    source_body = _repo_openstack_source_block()

    assert 'variable "lustre_network"' in template
    assert 'var.lustre_network == ""' in template
    assert "[var.network]" in template
    assert "[var.network, var.lustre_network]" in template
    expected_networks_local = (
        'packer_networks         = var.ports == "" ? '
        '(var.lustre_network == "" ? [var.network] : '
        "[var.network, var.lustre_network]) : "
        '(var.lustre_network == "" ? null : [var.lustre_network])'
    )
    assert expected_networks_local in template
    assert re.search(
        r"^\s*networks\s*=\s*local\.packer_networks\s*$",
        source_body,
        re.M,
    )


def test_checked_in_openstack_builder_uses_ports_without_serverwide_sg() -> None:
    """Switch to explicit ports without server-wide security groups."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    source_body = _repo_openstack_source_block()

    assert 'variable "ports"' in template
    assert 'var.ports == "" ? null : split(",", var.ports)' in template
    assert 'var.ports == "" ? ["default"] : null' in template
    assert re.search(
        r"^\s*ports\s*=\s*local\.packer_ports\s*$",
        source_body,
        re.M,
    )
    assert re.search(
        r"^\s*security_groups\s*=\s*local\.packer_security_groups\s*$",
        source_body,
        re.M,
    )


def test_build_image_runs_packer_with_expected_variable_values(tmp_path: Path) -> None:
    """Pass the documented base, SSH, network, and bundle vars to Packer."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    command = recorded_commands[0]
    assert "bundle_id=hail-0.2.137-gnomad-3.0.4-r2" in command
    assert "base_image=ubuntu-22.04" in command
    assert "ssh_username=ubuntu" in command
    assert "flavor=m2.large" in command
    assert f"network={NETWORK_UUID}" in command
    assert "lustre_network=" in command
    assert "floating_ip_pool=" in command
    assert "ports=" in command
    assert not any(argument.startswith("image_name=") for argument in command)


def test_build_image_reuses_cluster_floating_ip_pool_for_packer_when_unset(
    tmp_path: Path,
) -> None:
    """Default Packer SSH reachability to the cluster floating IP pool."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            cluster_floating_ip_pool="public",
            packer_floating_ip_pool="",
        )
    )
    template_path = _write_template_assets(tmp_path)
    security_groups = _RecordingSecurityGroupManager()
    ports = _RecordingPortManager([])
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        security_group_manager=security_groups,
        port_manager=ports,
    )

    assert "floating_ip_pool=public" in recorded_commands[0]
    assert f"ports={MANAGEMENT_PORT_UUID}" in recorded_commands[0]
    assert security_groups.events == [
        "create",
        "cleanup:hailstack-packer-ssh-test",
    ]


def test_build_image_packer_floating_ip_pool_overrides_cluster_pool(
    tmp_path: Path,
) -> None:
    """Allow the legacy Packer pool to override the cluster default."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            cluster_floating_ip_pool="cluster-public",
            packer_floating_ip_pool="build-public",
        )
    )
    template_path = _write_template_assets(tmp_path)
    security_groups = _RecordingSecurityGroupManager()
    ports = _RecordingPortManager([])
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        security_group_manager=security_groups,
        port_manager=ports,
    )

    command = recorded_commands[0]
    assert "floating_ip_pool=build-public" in command
    assert "floating_ip_pool=cluster-public" not in command
    assert f"ports={MANAGEMENT_PORT_UUID}" in command


def test_build_image_uses_temporary_ports_for_floating_ip_build(
    tmp_path: Path,
) -> None:
    """Use one explicit management port while still attaching Lustre by network."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            network_name="cloudforms_network",
            lustre_network="lustre-hgi01",
            cluster_floating_ip_pool="public",
        )
    )
    template_path = _write_template_assets(tmp_path)
    events: list[str] = []
    security_groups = _SharedRecordingSecurityGroupManager(events)
    ports = _RecordingPortManager(events)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        events.append("runner")
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    def fake_network_resolver(network_name: str) -> str:
        return {
            "cloudforms_network": RESOLVED_NETWORK_UUID,
            "lustre-hgi01": RESOLVED_LUSTRE_NETWORK_UUID,
        }[network_name]

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        network_resolver=fake_network_resolver,
        security_group_manager=security_groups,
        port_manager=ports,
    )

    assert events == [
        "sg:create",
        f"port:create-management:{RESOLVED_NETWORK_UUID}:hailstack-packer-ssh-test",
        "runner",
        f"port:cleanup:{MANAGEMENT_PORT_UUID}",
        "sg:cleanup:hailstack-packer-ssh-test",
    ]
    command = recorded_commands[0]
    assert "floating_ip_pool=public" in command
    assert f"ports={MANAGEMENT_PORT_UUID}" in command
    assert f"lustre_network={RESOLVED_LUSTRE_NETWORK_UUID}" in command
    assert not any("port:create-lustre" in event for event in events)
    assert "ssh_security_group=hailstack-packer-ssh-test" not in command


def test_build_image_cleans_up_before_runner_when_temporary_port_creation_fails(
    tmp_path: Path,
) -> None:
    """Clean up partial floating-IP setup when temporary port creation fails."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            lustre_network=LUSTRE_NETWORK_UUID,
            packer_floating_ip_pool="public",
        )
    )
    template_path = _write_template_assets(tmp_path)
    events: list[str] = []
    security_groups = _SharedRecordingSecurityGroupManager(events)
    ports = _RecordingPortManager(events, fail_create="management")
    runner_called = False

    def fail_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner should not be called")

    with pytest.raises(PackerError) as raised:
        build_image(
            config,
            _bundle(),
            runner=fail_runner,
            template_path=template_path,
            security_group_manager=security_groups,
            port_manager=ports,
        )

    assert not runner_called
    assert "OpenStack port/security group quota/permissions" in str(raised.value)
    assert events == [
        "sg:create",
        "sg:cleanup:hailstack-packer-ssh-test",
    ]


def test_openstack_build_port_manager_creates_management_port_with_ssh_security(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create only the management port with SSH security for Packer ports."""
    openstack_commands: list[list[str]] = []

    monkeypatch.setattr(
        packer_builder,
        "uuid4",
        lambda: UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
    )

    def fake_openstack(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output
        assert text
        assert not check
        openstack_commands.append(command)
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=f'{{"id": "{MANAGEMENT_PORT_UUID}"}}\n',
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_openstack)

    manager = packer_builder._OpenStackBuildPortManager()
    management_port_id = manager.create_management_port(
        network_id=RESOLVED_NETWORK_UUID,
        security_group_name="hailstack-packer-ssh-test",
    )
    manager.cleanup(management_port_id)

    assert management_port_id == MANAGEMENT_PORT_UUID
    assert not any(
        "--disable-port-security" in command for command in openstack_commands
    )
    assert not any(
        "hailstack-packer-lustre-aaaaaaaa" in command for command in openstack_commands
    )
    assert openstack_commands == [
        [
            "openstack",
            "port",
            "create",
            "--network",
            RESOLVED_NETWORK_UUID,
            "--security-group",
            "default",
            "--security-group",
            "hailstack-packer-ssh-test",
            "--enable-port-security",
            "hailstack-packer-management-aaaaaaaa",
            "-f",
            "json",
        ],
        [
            "openstack",
            "port",
            "delete",
            MANAGEMENT_PORT_UUID,
        ],
    ]


def test_build_image_creates_temporary_ssh_port_for_floating_ip_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Open TCP/22 only on a temporary management port for floating IP SSH."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            lustre_network=LUSTRE_NETWORK_UUID,
            cluster_floating_ip_pool="public",
            packer_floating_ip_pool="",
        )
    )
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []
    openstack_commands: list[list[str]] = []
    events: list[str] = []
    security_group_name = "hailstack-packer-ssh-aaaaaaaa"

    monkeypatch.setattr(
        packer_builder,
        "uuid4",
        lambda: UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
    )

    def fake_openstack(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output
        assert text
        assert not check
        openstack_commands.append(command)
        if command[:3] == ["openstack", "port", "create"]:
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout=f'{{"id": "{MANAGEMENT_PORT_UUID}"}}\n',
                stderr="",
            )
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout='{"id": "sg-id"}\n',
            stderr="",
        )

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        events.append("runner")
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    monkeypatch.setattr(subprocess, "run", fake_openstack)

    build_image(config, _bundle(), runner=fake_runner, template_path=template_path)

    assert events == ["runner"]
    assert openstack_commands == [
        [
            "openstack",
            "security",
            "group",
            "create",
            "--description",
            "Temporary Hailstack Packer SSH access for image build",
            security_group_name,
            "-f",
            "json",
        ],
        [
            "openstack",
            "security",
            "group",
            "rule",
            "create",
            "--ingress",
            "--ethertype",
            "IPv4",
            "--protocol",
            "tcp",
            "--dst-port",
            "22",
            "--remote-ip",
            "0.0.0.0/0",
            security_group_name,
            "-f",
            "json",
        ],
        [
            "openstack",
            "port",
            "create",
            "--network",
            NETWORK_UUID,
            "--security-group",
            "default",
            "--security-group",
            security_group_name,
            "--enable-port-security",
            "hailstack-packer-management-aaaaaaaa",
            "-f",
            "json",
        ],
        [
            "openstack",
            "port",
            "delete",
            MANAGEMENT_PORT_UUID,
        ],
        [
            "openstack",
            "security",
            "group",
            "delete",
            security_group_name,
        ],
    ]
    command = recorded_commands[0]
    assert "floating_ip_pool=public" in command
    assert f"ports={MANAGEMENT_PORT_UUID}" in command
    assert f"lustre_network={LUSTRE_NETWORK_UUID}" in command
    assert f"ssh_security_group={security_group_name}" not in command
    assert not any("cloudforms_ssh_in" in argument for argument in command)


def test_build_image_skips_temporary_ssh_security_group_without_floating_ip_pool(
    tmp_path: Path,
) -> None:
    """Keep non-floating-IP builds on the existing security-group path."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            cluster_floating_ip_pool="",
            packer_floating_ip_pool="",
        )
    )
    template_path = _write_template_assets(tmp_path)
    security_groups = _RecordingSecurityGroupManager()
    port_events: list[str] = []
    ports = _RecordingPortManager(port_events)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
        security_group_manager=security_groups,
        port_manager=ports,
    )

    assert security_groups.events == []
    assert port_events == []
    assert "floating_ip_pool=" in recorded_commands[0]
    assert "ports=" in recorded_commands[0]


def test_build_image_cleans_up_temporary_ssh_security_group_when_packer_fails(
    tmp_path: Path,
) -> None:
    """Delete temporary SSH ingress even when Packer returns a failure."""
    config = load_config(
        _write_config(tmp_path / "cluster.toml", packer_floating_ip_pool="public")
    )
    template_path = _write_template_assets(tmp_path)
    events: list[str] = []
    security_groups = _SharedRecordingSecurityGroupManager(events)
    ports = _RecordingPortManager(events)

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        events.append("runner")
        return _result("", stderr="template failed", returncode=1)

    with pytest.raises(PackerError, match="template failed"):
        build_image(
            config,
            _bundle(),
            runner=fake_runner,
            template_path=template_path,
            security_group_manager=security_groups,
            port_manager=ports,
        )

    assert events == [
        "sg:create",
        f"port:create-management:{NETWORK_UUID}:hailstack-packer-ssh-test",
        "runner",
        f"port:cleanup:{MANAGEMENT_PORT_UUID}",
        "sg:cleanup:hailstack-packer-ssh-test",
    ]


def test_build_image_logs_warning_when_temporary_ssh_security_group_cleanup_fails(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep the primary Packer result visible when SG cleanup fails."""
    config = load_config(
        _write_config(tmp_path / "cluster.toml", packer_floating_ip_pool="public")
    )
    template_path = _write_template_assets(tmp_path)

    class FailingCleanupSecurityGroupManager(_RecordingSecurityGroupManager):
        """Raise during cleanup to exercise warning-only handling."""

        def cleanup(self, security_group_name: str) -> None:
            """Fail to delete the fake temporary security group."""
            super().cleanup(security_group_name)
            raise PackerError("delete failed")

    security_groups = FailingCleanupSecurityGroupManager()
    port_events: list[str] = []
    ports = _RecordingPortManager(port_events)

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        return _result("artifact,0,id,image-123\n")

    with caplog.at_level(logging.WARNING):
        result = build_image(
            config,
            _bundle(),
            runner=fake_runner,
            template_path=template_path,
            security_group_manager=security_groups,
            port_manager=ports,
        )

    assert result == "image-123"
    assert port_events == [
        f"port:create-management:{NETWORK_UUID}:hailstack-packer-ssh-test",
        f"port:cleanup:{MANAGEMENT_PORT_UUID}",
    ]
    assert security_groups.events == [
        "create",
        "cleanup:hailstack-packer-ssh-test",
    ]
    assert "Could not delete temporary Packer SSH security group" in caplog.text
    assert "hailstack-packer-ssh-test" in caplog.text
    assert "delete failed" in caplog.text


def test_build_image_logs_warning_when_temporary_port_cleanup_fails(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep the primary Packer result visible when port cleanup fails."""
    config = load_config(
        _write_config(tmp_path / "cluster.toml", packer_floating_ip_pool="public")
    )
    template_path = _write_template_assets(tmp_path)
    security_groups = _RecordingSecurityGroupManager()

    class FailingCleanupPortManager(_RecordingPortManager):
        """Raise during port cleanup to exercise warning-only handling."""

        def cleanup(self, port_id: str) -> None:
            """Fail to delete the fake temporary port."""
            super().cleanup(port_id)
            raise PackerError("port delete failed")

    port_events: list[str] = []
    ports = FailingCleanupPortManager(port_events)

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        return _result("artifact,0,id,image-123\n")

    with caplog.at_level(logging.WARNING):
        result = build_image(
            config,
            _bundle(),
            runner=fake_runner,
            template_path=template_path,
            security_group_manager=security_groups,
            port_manager=ports,
        )

    assert result == "image-123"
    assert port_events == [
        f"port:create-management:{NETWORK_UUID}:hailstack-packer-ssh-test",
        f"port:cleanup:{MANAGEMENT_PORT_UUID}",
    ]
    assert security_groups.events == [
        "create",
        "cleanup:hailstack-packer-ssh-test",
    ]
    assert "Could not delete temporary Packer port" in caplog.text
    assert MANAGEMENT_PORT_UUID in caplog.text
    assert "port delete failed" in caplog.text


@pytest.mark.parametrize(
    ("failing_command", "expected_context"),
    [
        pytest.param("create", "create temporary Packer SSH security group", id="sg"),
        pytest.param("rule", "create temporary Packer SSH ingress rule", id="rule"),
    ],
)
def test_build_image_fails_before_runner_when_temporary_ssh_security_group_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_command: str,
    expected_context: str,
) -> None:
    """Fail before Packer when OpenStack cannot create SSH ingress."""
    config = load_config(
        _write_config(tmp_path / "cluster.toml", packer_floating_ip_pool="public")
    )
    template_path = _write_template_assets(tmp_path)
    runner_called = False
    openstack_commands: list[list[str]] = []

    def fake_openstack(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, check
        openstack_commands.append(command)
        is_group_create_command = command[:4] == [
            "openstack",
            "security",
            "group",
            "create",
        ]
        is_rule_command = command[:5] == [
            "openstack",
            "security",
            "group",
            "rule",
            "create",
        ]
        if failing_command == "create" and is_group_create_command:
            return subprocess.CompletedProcess(
                args=command,
                returncode=1,
                stdout="",
                stderr="Quota exceeded",
            )
        if failing_command == "rule" and is_rule_command:
            return subprocess.CompletedProcess(
                args=command,
                returncode=1,
                stdout="",
                stderr="Forbidden",
            )
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout="",
            stderr="",
        )

    def fail_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner should not be called")

    monkeypatch.setattr(subprocess, "run", fake_openstack)

    with pytest.raises(PackerError) as raised:
        build_image(config, _bundle(), runner=fail_runner, template_path=template_path)

    message = str(raised.value)
    assert expected_context in message
    assert "security group quota/permissions" in message
    assert ("Quota exceeded" in message) or ("Forbidden" in message)
    assert not runner_called
    if failing_command == "rule":
        assert openstack_commands[-1][:4] == [
            "openstack",
            "security",
            "group",
            "delete",
        ]


def test_build_image_runs_packer_from_template_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run Packer where relative provisioner script paths are resolvable."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_dir = tmp_path / "venv" / "lib" / "python3.14" / "site-packages"
    template_dir = template_dir / "hailstack" / "_data" / "packer"
    template_dir.mkdir(parents=True)
    template_path = _write_template_assets(template_dir)
    caller_dir = tmp_path / "caller"
    caller_dir.mkdir()
    monkeypatch.chdir(caller_dir)
    recorded_commands: list[list[str]] = []
    recorded_cwds: list[Path] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        recorded_cwds.append(cwd)
        assert Path.cwd() == caller_dir
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert recorded_commands[0][-1] == str(template_path)
    assert recorded_cwds == [template_path.parent]


def test_build_image_resolves_relative_packer_log_path_from_caller_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep user-supplied relative Packer log paths rooted at the caller cwd."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_dir = tmp_path / "venv" / "lib" / "python3.14" / "site-packages"
    template_dir = template_dir / "hailstack" / "_data" / "packer"
    template_dir.mkdir(parents=True)
    template_path = _write_template_assets(template_dir)
    caller_dir = tmp_path / "caller"
    caller_dir.mkdir()
    monkeypatch.chdir(caller_dir)
    monkeypatch.setenv("PACKER_LOG_PATH", "packer-debug.log")
    recorded_log_paths: list[str] = []
    recorded_cwds: list[Path] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command
        recorded_log_paths.append(os.environ["PACKER_LOG_PATH"])
        recorded_cwds.append(cwd)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert recorded_log_paths == [str(caller_dir / "packer-debug.log")]
    assert recorded_cwds == [template_path.parent]


def test_build_image_preserves_absolute_packer_log_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave absolute Packer log paths unchanged."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_dir = tmp_path / "template"
    template_dir.mkdir()
    template_path = _write_template_assets(template_dir)
    caller_dir = tmp_path / "caller"
    caller_dir.mkdir()
    absolute_log_path = tmp_path / "logs" / "packer-debug.log"
    monkeypatch.chdir(caller_dir)
    monkeypatch.setenv("PACKER_LOG_PATH", str(absolute_log_path))
    recorded_log_paths: list[str] = []
    recorded_cwds: list[Path] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command
        recorded_log_paths.append(os.environ["PACKER_LOG_PATH"])
        recorded_cwds.append(cwd)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert recorded_log_paths == [str(absolute_log_path)]
    assert recorded_cwds == [template_path.parent]


def test_builder_vars_match_checked_in_template_contract(tmp_path: Path) -> None:
    """Keep the builder var set aligned with the checked-in HCL declarations."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    declared_vars = set(re.findall(r'variable "([^"]+)"', template))
    builder_vars = set(
        _packer_vars(
            config,
            _bundle(),
            network_id=NETWORK_UUID,
            lustre_network_id="",
        )
    )

    assert builder_vars == declared_vars
    assert 'image_name       = "hailstack-${var.bundle_id}"' in template


def test_build_image_raises_packer_error_with_stderr_output_on_failure(
    tmp_path: Path,
) -> None:
    """Surface packer stderr when the build command fails."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command
        return _result("", stderr="template failed", returncode=1)

    with pytest.raises(PackerError, match="template failed"):
        build_image(
            config,
            _bundle(),
            runner=fake_runner,
            template_path=template_path,
        )


def test_build_image_failure_summarizes_machine_readable_packer_output(
    tmp_path: Path,
) -> None:
    """Translate Packer machine-readable failures into user-facing diagnostics."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    packer_output = "\n".join(
        (
            "1780585788,,ui,say,==> openstack.hailstack: Waiting for SSH to "
            "become available...",
            "1780586088,,ui,error,==> openstack.hailstack: Timeout waiting for SSH.",
            "1780586093,openstack.hailstack,error,Timeout waiting for SSH.",
            "1780586093,,ui,say,\\n==> Builds finished but no artifacts were created.",
        )
    )

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command
        return _result(packer_output, returncode=1)

    with pytest.raises(PackerError) as raised:
        build_image(
            config,
            _bundle(),
            runner=fake_runner,
            template_path=template_path,
        )

    message = str(raised.value)
    assert "Packer build failed." in message
    assert "Packer diagnostics:" in message
    assert "openstack.hailstack: Timeout waiting for SSH." in message
    assert "Raw Packer output:" in message
    assert "1780586088,,ui,error" in message


def test_build_image_no_route_failure_explains_floating_ip_fix(
    tmp_path: Path,
) -> None:
    """Explain how to make remote fixed-IP SSH reachable instead of timing out."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            network_name="cloudforms_network",
            packer_floating_ip_pool="",
        )
    )
    template_path = _write_template_assets(tmp_path)
    packer_debug_output = "\n".join(
        (
            "2026/06/05 10:00:00 packer-plugin-openstack: Floating IP not required",
            "2026/06/05 10:00:01 packer-plugin-openstack: "
            "Using SSH communicator to connect: 192.168.252.82",
            "2026/06/05 10:00:02 packer-plugin-openstack: "
            "TCP connection to SSH ip/port failed: dial tcp 192.168.252.82:22: "
            "connect: no route to host",
        )
    )

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        return _result("", stderr=packer_debug_output, returncode=1)

    with pytest.raises(PackerError) as raised:
        build_image(
            config,
            _bundle(),
            runner=fake_runner,
            template_path=template_path,
            network_resolver=lambda network_name: RESOLVED_NETWORK_UUID,
        )

    message = str(raised.value)
    assert (
        "could not SSH to the temporary build instance fixed IP `192.168.252.82`"
        in message
    )
    assert "no route to host" in message
    assert "`cluster.floating_ip_pool`" in message
    assert "`[packer].floating_ip_pool`" in message
    assert "`cluster.network_name` (`cloudforms_network`)" in message


def test_build_image_maps_hadoop_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle Hadoop version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "hadoop_version=3.4.1" in recorded_commands[0]


def test_build_image_maps_spark_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle Spark version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "spark_version=3.5.6" in recorded_commands[0]


def test_build_image_maps_hail_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle Hail version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "hail_version=0.2.137" in recorded_commands[0]


def test_build_image_maps_java_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle Java version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "java_version=11" in recorded_commands[0]


def test_build_image_maps_python_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle Python version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "python_version=3.12" in recorded_commands[0]


def test_build_image_maps_scala_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle Scala version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "scala_version=2.12.18" in recorded_commands[0]


def test_build_image_maps_gnomad_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle gnomAD version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "gnomad_version=3.0.4" in recorded_commands[0]


def test_build_image_maps_default_gnomad_methods_version_to_packer_vars(
    tmp_path: Path,
) -> None:
    """Provide the default gnomAD methods package version to the template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    command = recorded_commands[0]
    assert "gnomad_version=3.0.4" in command
    assert "gnomad_methods_version=0.8.2" in command


def test_build_image_maps_configured_gnomad_methods_version_to_packer_vars(
    tmp_path: Path,
) -> None:
    """Allow build-image configs to override the gnomAD methods package version."""
    config = load_config(
        _write_config(
            tmp_path / "cluster.toml",
            gnomad_methods_version="0.8.1",
        )
    )
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "gnomad_methods_version=0.8.1" in recorded_commands[0]
    assert "gnomad_methods_version=0.8.2" not in recorded_commands[0]


def test_build_image_returns_uploaded_image_id(tmp_path: Path) -> None:
    """Return the Packer-reported image ID from the build output."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command
        return _result("1700000000,,artifact,0,id,image-123\n")

    result = build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert result == "image-123"


def test_build_image_does_not_pass_secrets_or_cluster_specific_config(
    tmp_path: Path,
) -> None:
    """Exclude SSH keys, S3 secrets, and cluster name from Packer vars."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    rendered = "\n".join(recorded_commands[0])
    assert "secret-access" not in rendered
    assert "secret-secret" not in rendered
    assert "ssh-rsa AAAA" not in rendered
    assert "test-cluster" not in rendered


def test_build_image_fails_before_runner_when_template_assets_missing(
    tmp_path: Path,
) -> None:
    """Reject missing checked-in template assets before invoking packer."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = tmp_path / "hailstack.pkr.hcl"
    template_path.write_text('source "null" "noop" {}\n', encoding="utf-8")

    def fail_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command
        raise AssertionError("runner should not be called")

    with pytest.raises(PackerError, match="Missing required Packer assets"):
        build_image(
            config,
            _bundle(),
            runner=fail_runner,
            template_path=template_path,
        )


def test_build_image_fails_before_runner_when_apt_lock_helper_missing(
    tmp_path: Path,
) -> None:
    """Reject template trees missing the apt/dpkg lock helper."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path, include_apt_helper=False)

    def fail_runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd
        raise AssertionError("runner should not be called")

    with pytest.raises(PackerError) as raised:
        build_image(
            config,
            _bundle(),
            runner=fail_runner,
            template_path=template_path,
        )

    assert "scripts/apt-locks.sh" in str(raised.value)


def test_repo_packer_template_declares_expected_scripts_and_env_vars() -> None:
    """Check the checked-in template wires all provisioner scripts and bundle vars."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")

    assert 'image_name       = "hailstack-${var.bundle_id}"' in template
    assert 'ssh_timeout      = "30m"' in template
    for variable_name in (
        "bundle_id",
        "hail_version",
        "spark_version",
        "hadoop_version",
        "java_version",
        "python_version",
        "scala_version",
        "gnomad_version",
        "gnomad_methods_version",
        "base_image",
        "ssh_username",
        "flavor",
        "network",
        "lustre_network",
        "floating_ip_pool",
        "ports",
    ):
        assert f'variable "{variable_name}"' in template

    for script_path in REQUIRED_PACKER_SCRIPT_PATHS:
        relative_path = script_path.relative_to(PACKER_SCRIPTS_PATH.parent)
        assert f'"${{path.root}}/{relative_path.as_posix()}"' in template

    for env_name in (
        "HADOOP_VERSION",
        "SPARK_VERSION",
        "HAIL_VERSION",
        "JAVA_VERSION",
        "PYTHON_VERSION",
        "SCALA_VERSION",
        "GNOMAD_VERSION",
        "GNOMAD_METHODS_VERSION",
    ):
        assert f'"{env_name}=${{var.' in template


def test_repo_packer_template_uploads_apt_lock_helper_before_scripts() -> None:
    """Upload the apt/dpkg helper before any shell provisioner uses it."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    file_block = re.search(
        r'provisioner\s+"file"\s*\{(?P<body>.*?)\n\s*\}',
        template,
        re.S,
    )

    assert file_block is not None
    assert re.search(
        r'^\s*source\s*=\s*"\${path\.root}/scripts/apt-locks\.sh"\s*$',
        file_block["body"],
        re.M,
    )
    assert re.search(
        rf'^\s*destination\s*=\s*"{PACKER_REMOTE_APT_HELPER_PATH}"\s*$',
        file_block["body"],
        re.M,
    )
    assert template.index('provisioner "file"') < template.index('provisioner "shell"')


def test_repo_packer_shell_provisioner_runs_as_root_and_preserves_env() -> None:
    """Run provisioner scripts through passwordless sudo with bundle env vars."""
    provisioner_body = _repo_shell_provisioner_block()
    execute_command = re.search(
        r'^\s*execute_command\s*=\s*"(?P<command>[^"]+)"\s*$',
        provisioner_body,
        re.M,
    )
    assert execute_command is not None

    command = execute_command["command"]
    assert "chmod +x {{ .Path }}" in command
    assert "{{ .Vars }}" in command
    assert "sudo -E {{ .Path }}" in command
    assert command.index("{{ .Vars }}") < command.index("sudo -E")


def test_repo_packer_template_roots_scripts_at_template_directory() -> None:
    """Resolve shell provisioner scripts from the template root, not process cwd."""
    expected_entries = {
        f"${{path.root}}/{script_path.relative_to(PACKER_SCRIPTS_PATH.parent).as_posix()}"
        for script_path in REQUIRED_PACKER_SCRIPT_PATHS
    }
    actual_entries = _repo_template_script_entries()

    assert actual_entries == expected_entries
    assert not any(entry.startswith("scripts/") for entry in actual_entries)


def test_repo_packer_apt_helper_waits_bounded_for_unattended_dpkg_locks() -> None:
    """Keep the apt/dpkg helper bounded and aware of unattended apt activity."""
    content = PACKER_APT_HELPER_PATH.read_text(encoding="utf-8")

    assert "HAILSTACK_APT_LOCK_TIMEOUT_SECONDS" in content
    assert "HAILSTACK_APT_LOCK_POLL_SECONDS" in content
    for lock_path in (
        "/var/lib/dpkg/lock-frontend",
        "/var/lib/dpkg/lock",
        "/var/lib/apt/lists/lock",
        "/var/cache/apt/archives/lock",
    ):
        assert lock_path in content
    for unit_name in (
        "apt-daily.timer",
        "apt-daily-upgrade.timer",
        "apt-daily.service",
        "apt-daily-upgrade.service",
        "unattended-upgrades.service",
    ):
        assert unit_name in content

    assert "fuser" in content or "lsof" in content
    assert "DPkg::Lock::Timeout" in content
    assert "sleep" in content
    assert "return 1" in content


def test_repo_packer_apt_scripts_call_helper_before_apt_commands() -> None:
    """Require Packer apt scripts to go through the lock-aware helper."""
    direct_command_pattern = re.compile(r"^\s*(apt-get|apt|dpkg)\b", re.M)
    offenders: list[str] = []

    for relative_path in PACKER_APT_SCRIPT_RELATIVE_PATHS:
        script_path = PACKER_ROOT_PATH / relative_path
        content = script_path.read_text(encoding="utf-8")
        assert PACKER_REMOTE_APT_HELPER_PATH in content
        assert "hailstack_apt_get" in content

        if relative_path == Path("scripts/base.sh"):
            assert "hailstack_add_apt_repository -y ppa:deadsnakes/ppa" in content

    for script_path in REQUIRED_PACKER_SCRIPT_PATHS:
        content = script_path.read_text(encoding="utf-8")
        for match in direct_command_pattern.finditer(content):
            offenders.append(f"{script_path.relative_to(PACKER_ROOT_PATH)}: {match[0]}")

    assert offenders == []


def test_repo_packer_scripts_are_executable_and_embed_version_checks() -> None:
    """Keep verification hooks in the checked-in script tree."""
    expected_checks = {
        "base.sh": [
            "/opt/hailstack/base-venv",
            "/opt/hailstack/overlay-venv",
            "cryptsetup",
            "jupyter-lab.service",
            "netcat-openbsd",
            "nginx.service",
            "nfs-common",
            "nfs-kernel-server",
        ],
        "ubuntu/packages.sh": [
            'hailstack_verify_version Java "$JAVA_VERSION"',
            'hailstack_verify_version Python "$PYTHON_VERSION"',
            'hailstack_verify_version Scala "$SCALA_VERSION"',
        ],
        "ubuntu/hadoop.sh": [
            'grep -F "$HADOOP_VERSION"',
            "hdfs-namenode.service",
            "hdfs-datanode.service",
            "yarn-rm.service",
            "yarn-nm.service",
            "mapred-history.service",
        ],
        "ubuntu/spark.sh": [
            'grep -F "$SPARK_VERSION"',
            "spark-master.service",
            "spark-history-server.service",
            "spark-worker.service",
        ],
        "ubuntu/hail.sh": ['grep -F "$HAIL_VERSION"'],
        "ubuntu/jupyter.sh": [
            'grep -F "$JUPYTER_VERSION"',
            "jupyter-lab.service",
        ],
        "ubuntu/gnomad.sh": ['grep -F "$GNOMAD_VERSION"'],
        "ubuntu/uv.sh": ['grep -F "$UV_VERSION"'],
        "ubuntu/netdata.sh": [
            'grep -F "$NETDATA_VERSION"',
            "netdata.service",
            "systemctl enable netdata.service",
        ],
    }

    for relative_path, tokens in expected_checks.items():
        script_path = PACKER_SCRIPTS_PATH / relative_path
        content = script_path.read_text(encoding="utf-8")

        assert script_path.exists()
        assert script_path.stat().st_mode & 0o111
        for token in tokens:
            assert token in content


def test_repo_packer_scripts_do_not_embed_cluster_specific_or_secret_values() -> None:
    """Keep checked-in image scripts free of SSH keys, S3 credentials, and names."""
    forbidden_tokens = (
        "ssh-rsa",
        "secret-access",
        "secret-secret",
        "test-cluster",
    )

    for script_path in REQUIRED_PACKER_SCRIPT_PATHS:
        content = script_path.read_text(encoding="utf-8")
        for forbidden_token in forbidden_tokens:
            assert forbidden_token not in content


def test_e2_packer_template_validates_with_packer_cli() -> None:
    """Validate the checked-in HCL template with the local Packer CLI."""
    if shutil.which("packer") is None:
        pytest.skip("packer CLI not installed")

    result = subprocess.run(
        ["packer", "validate", "-syntax-only", str(PACKER_TEMPLATE_PATH)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout


@pytest.mark.parametrize(
    ("bundle", "expected_pairs"),
    [
        pytest.param(
            Bundle(
                id="hail-0.2.137-gnomad-3.0.4-r2",
                hail="0.2.137",
                spark="3.5.6",
                hadoop="3.4.1",
                java="11",
                python="3.12",
                scala="2.12.18",
                gnomad="3.0.4",
                status="latest",
            ),
            {
                "HADOOP_VERSION": "3.4.1",
                "SPARK_VERSION": "3.5.6",
                "HAIL_VERSION": "0.2.137",
                "JAVA_VERSION": "11",
                "PYTHON_VERSION": "3.12",
                "SCALA_VERSION": "2.12.18",
                "GNOMAD_VERSION": "3.0.4",
                "GNOMAD_METHODS_VERSION": "0.8.2",
            },
            id="latest-bundle",
        ),
        pytest.param(
            Bundle(
                id="hail-0.2.136-gnomad-3.0.4-r1",
                hail="0.2.136",
                spark="3.5.4",
                hadoop="3.4.0",
                java="11",
                python="3.12",
                scala="2.12.18",
                gnomad="3.0.4",
                status="supported",
            ),
            {
                "HADOOP_VERSION": "3.4.0",
                "SPARK_VERSION": "3.5.4",
                "HAIL_VERSION": "0.2.136",
                "JAVA_VERSION": "11",
                "PYTHON_VERSION": "3.12",
                "SCALA_VERSION": "2.12.18",
                "GNOMAD_VERSION": "3.0.4",
                "GNOMAD_METHODS_VERSION": "0.8.2",
            },
            id="supported-bundle",
        ),
    ],
)
def test_e2_bundle_versions_flow_into_provisioner_environment_vars(
    tmp_path: Path,
    bundle: Bundle,
    expected_pairs: dict[str, str],
) -> None:
    """Map bundle versions into the template contract used by shell provisioners."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    variables = _packer_vars(
        config,
        bundle,
        network_id=NETWORK_UUID,
        lustre_network_id="",
    )
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")

    for env_name, expected_value in expected_pairs.items():
        template_reference = f'"{env_name}=${{var.{env_name.lower()}}}"'
        variable_reference = env_name.lower()

        assert template_reference in template
        assert variables[variable_reference] == expected_value


def test_e2_all_required_provisioner_scripts_exist_and_are_executable() -> None:
    """Keep the full checked-in provisioner script set present and executable."""
    assert len(REQUIRED_PACKER_SCRIPT_PATHS) == 9

    for script_path in REQUIRED_PACKER_SCRIPT_PATHS:
        assert script_path.is_file()
        assert script_path.stat().st_mode & 0o111


def test_e2_jupyter_provisioner_repairs_base_venv_after_gnomad() -> None:
    """Run Jupyter dependency repair after gnomAD has installed its dependencies."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    gnomad_reference = '"${path.root}/scripts/ubuntu/gnomad.sh"'
    jupyter_reference = '"${path.root}/scripts/ubuntu/jupyter.sh"'

    assert template.index(gnomad_reference) < template.index(jupyter_reference)


def test_e2_base_venv_preinstalls_are_declared_via_uv() -> None:
    """Declare the base venv and its preinstalled Python tools directly in scripts."""
    expected_tokens = {
        PACKER_SCRIPTS_PATH / "base.sh": [
            'PYTHON_BIN="python${PYTHON_VERSION}"',
            'PYTHON_VENV_PACKAGE="${PYTHON_BIN}-venv"',
            "hailstack_add_apt_repository -y ppa:deadsnakes/ppa",
            '"${PYTHON_BIN}" -m venv /opt/hailstack/base-venv',
            "/opt/hailstack/base-venv/bin/python -m pip install --upgrade pip uv",
            "/opt/hailstack/base-venv/bin/python -m venv --system-site-packages "
            "/opt/hailstack/overlay-venv",
            "printf '%s\\n' \"${BASE_PURELIB}\" > "
            '"${OVERLAY_PURELIB}/hailstack-base-venv.pth"',
            "test -d /opt/hailstack/base-venv",
            "test -d /opt/hailstack/overlay-venv",
            "ExecStart=/opt/hailstack/overlay-venv/bin/python -m jupyterlab "
            "--ip=0.0.0.0 --port=8888 --no-browser --allow-root",
        ],
        PACKER_SCRIPTS_PATH / "ubuntu/hail.sh": [
            "test -d /opt/hailstack/base-venv",
            "/opt/hailstack/base-venv/bin/uv pip install",
            '"hail==${HAIL_VERSION}"',
            '"pyspark==${SPARK_VERSION}"',
        ],
        PACKER_SCRIPTS_PATH / "ubuntu/jupyter.sh": [
            "test -d /opt/hailstack/base-venv",
            'JUPYTER_VERSION="${JUPYTER_VERSION:-3.5.3}"',
            'JUPYTER_SERVER_VERSION="${JUPYTER_SERVER_VERSION:-2.10.0}"',
            'JUPYTERLAB_SERVER_VERSION="${JUPYTERLAB_SERVER_VERSION:-2.16.6}"',
            'JUPYTER_EVENTS_VERSION="${JUPYTER_EVENTS_VERSION:-0.6.3}"',
            'JSONSCHEMA_VERSION="${JSONSCHEMA_VERSION:-3.2.0}"',
            'DECORATOR_VERSION="${DECORATOR_VERSION:-4.4.2}"',
            'IPYTHON_VERSION="${IPYTHON_VERSION:-8.39.0}"',
            'PYTHON_JSON_LOGGER_VERSION="${PYTHON_JSON_LOGGER_VERSION:-2.0.7}"',
            "/opt/hailstack/base-venv/bin/uv pip install",
            '"jupyterlab==${JUPYTER_VERSION}"',
            '"jupyter-server==${JUPYTER_SERVER_VERSION}"',
            '"jupyterlab-server==${JUPYTERLAB_SERVER_VERSION}"',
            '"jupyter-events==${JUPYTER_EVENTS_VERSION}"',
            '"jsonschema==${JSONSCHEMA_VERSION}"',
            '"decorator==${DECORATOR_VERSION}"',
            '"ipython==${IPYTHON_VERSION}"',
            '"python-json-logger==${PYTHON_JSON_LOGGER_VERSION}"',
            "/opt/hailstack/base-venv/bin/python -m pip check",
            "jupyterlab.labapp",
            "jupyter_server.serverapp",
        ],
        PACKER_SCRIPTS_PATH / "ubuntu/gnomad.sh": [
            "test -d /opt/hailstack/base-venv",
            "/opt/hailstack/base-venv/bin/uv pip install",
            'GNOMAD_METHODS_VERSION="${GNOMAD_METHODS_VERSION:-0.8.2}"',
            '"gnomad==${GNOMAD_METHODS_VERSION}"',
            "importlib.metadata.version",
        ],
        PACKER_SCRIPTS_PATH / "ubuntu/uv.sh": [
            "test -d /opt/hailstack/base-venv",
            "/opt/hailstack/base-venv/bin/python -m pip install --upgrade uv",
            "test -x /opt/hailstack/base-venv/bin/uv",
        ],
    }

    for script_path, tokens in expected_tokens.items():
        content = script_path.read_text(encoding="utf-8")
        for token in tokens:
            assert token in content
