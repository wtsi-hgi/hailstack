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

import re
import subprocess
from pathlib import Path

import pytest

from hailstack.config.compatibility import Bundle
from hailstack.config.parser import load_config
from hailstack.errors import PackerError
from hailstack.packer.builder import (
    PACKER_SCRIPTS_PATH,
    PACKER_TEMPLATE_PATH,
    REQUIRED_PACKER_SCRIPT_PATHS,
    _packer_vars,
    build_image,
)


def _write_config(path: Path) -> Path:
    """Write a minimal build-image config file."""
    path.write_text(
        (
            "[cluster]\n"
            'name = "test-cluster"\n'
            'master_flavour = "m2.medium"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n\n'
            "[packer]\n"
            'base_image = "ubuntu-22.04"\n'
            'flavour = "m2.large"\n'
            'floating_ip_pool = "public"\n\n'
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


def _write_template_assets(tmp_path: Path) -> Path:
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

    return template_path


def test_build_image_runs_packer_with_expected_variable_values(tmp_path: Path) -> None:
    """Pass the documented base, SSH, network, and bundle vars to Packer."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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
    assert "network=private-net" in command
    assert "floating_ip_pool=public" in command
    assert not any(argument.startswith("image_name=") for argument in command)


def test_builder_vars_match_checked_in_template_contract(tmp_path: Path) -> None:
    """Keep the builder var set aligned with the checked-in HCL declarations."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
    declared_vars = set(re.findall(r'variable "([^"]+)"', template))
    builder_vars = set(_packer_vars(config, _bundle()))

    assert builder_vars == declared_vars
    assert 'image_name       = "hailstack-${var.bundle_id}"' in template


def test_build_image_raises_packer_error_with_stderr_output_on_failure(
    tmp_path: Path,
) -> None:
    """Surface packer stderr when the build command fails."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        del command
        return _result("", stderr="template failed", returncode=1)

    with pytest.raises(PackerError, match="template failed"):
        build_image(
            config,
            _bundle(),
            runner=fake_runner,
            template_path=template_path,
        )


def test_build_image_maps_hadoop_version_to_packer_vars(tmp_path: Path) -> None:
    """Provide the bundle Hadoop version to the packer template."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)
    recorded_commands: list[list[str]] = []

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        recorded_commands.append(command)
        return _result("artifact,0,id,image-123\n")

    build_image(
        config,
        _bundle(),
        runner=fake_runner,
        template_path=template_path,
    )

    assert "gnomad_version=3.0.4" in recorded_commands[0]


def test_build_image_returns_uploaded_image_id(tmp_path: Path) -> None:
    """Return the Packer-reported image ID from the build output."""
    config = load_config(_write_config(tmp_path / "cluster.toml"))
    template_path = _write_template_assets(tmp_path)

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
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

    def fail_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        del command
        raise AssertionError("runner should not be called")

    with pytest.raises(PackerError, match="Missing required Packer assets"):
        build_image(
            config,
            _bundle(),
            runner=fail_runner,
            template_path=template_path,
        )


def test_repo_packer_template_declares_expected_scripts_and_env_vars() -> None:
    """Check the checked-in template wires all provisioner scripts and bundle vars."""
    template = PACKER_TEMPLATE_PATH.read_text(encoding="utf-8")

    assert 'image_name       = "hailstack-${var.bundle_id}"' in template
    for variable_name in (
        "bundle_id",
        "hail_version",
        "spark_version",
        "hadoop_version",
        "java_version",
        "python_version",
        "scala_version",
        "gnomad_version",
        "base_image",
        "ssh_username",
        "flavor",
        "network",
        "floating_ip_pool",
    ):
        assert f'variable "{variable_name}"' in template

    for script_path in REQUIRED_PACKER_SCRIPT_PATHS:
        relative_path = script_path.relative_to(PACKER_SCRIPTS_PATH.parent)
        assert f'"{relative_path.as_posix()}"' in template

    for env_name in (
        "HADOOP_VERSION",
        "SPARK_VERSION",
        "HAIL_VERSION",
        "JAVA_VERSION",
        "PYTHON_VERSION",
        "SCALA_VERSION",
        "GNOMAD_VERSION",
    ):
        assert f'"{env_name}=${{var.' in template


def test_repo_packer_scripts_are_executable_and_embed_version_checks() -> None:
    """Keep verification hooks in the checked-in script tree."""
    expected_checks = {
        "base.sh": [
            "/opt/hailstack/base-venv",
            "hailstack-jupyterlab.service",
            "nginx.service",
        ],
        "ubuntu/packages.sh": [
            'grep -F "$JAVA_VERSION"',
            'grep -F "$PYTHON_VERSION"',
            'grep -F "$SCALA_VERSION"',
        ],
        "ubuntu/hadoop.sh": [
            'grep -F "$HADOOP_VERSION"',
            "hadoop-namenode.service",
            "hadoop-datanode.service",
        ],
        "ubuntu/spark.sh": [
            'grep -F "$SPARK_VERSION"',
            "spark-master.service",
            "spark-worker.service",
        ],
        "ubuntu/hail.sh": ['grep -F "$HAIL_VERSION"'],
        "ubuntu/jupyter.sh": [
            'grep -F "$JUPYTER_VERSION"',
            "hailstack-jupyterlab.service",
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
    variables = _packer_vars(config, bundle)
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


def test_e2_base_venv_preinstalls_are_declared_via_uv() -> None:
    """Declare the base venv and its preinstalled Python tools directly in scripts."""
    expected_tokens = {
        PACKER_SCRIPTS_PATH / "base.sh": [
            "python3 -m venv /opt/hailstack/base-venv",
            "/opt/hailstack/base-venv/bin/python -m pip install --upgrade pip uv",
            "test -d /opt/hailstack/base-venv",
        ],
        PACKER_SCRIPTS_PATH / "ubuntu/hail.sh": [
            "test -d /opt/hailstack/base-venv",
            "/opt/hailstack/base-venv/bin/uv pip install",
            '"hail==${HAIL_VERSION}"',
            '"pyspark==${SPARK_VERSION}"',
        ],
        PACKER_SCRIPTS_PATH / "ubuntu/jupyter.sh": [
            "test -d /opt/hailstack/base-venv",
            "/opt/hailstack/base-venv/bin/uv pip install",
            "jupyterlab",
        ],
        PACKER_SCRIPTS_PATH / "ubuntu/gnomad.sh": [
            "test -d /opt/hailstack/base-venv",
            "/opt/hailstack/base-venv/bin/uv pip install",
            '"gnomad==${GNOMAD_VERSION}"',
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
