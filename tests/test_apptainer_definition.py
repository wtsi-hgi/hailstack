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

"""Acceptance tests for the M1 Apptainer definition."""

import re
import tomllib
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
APPTAINER_DEFINITION_PATH = REPOSITORY_ROOT / "Apptainer.def"
PYPROJECT_PATH = REPOSITORY_ROOT / "pyproject.toml"
CLI_MAIN_PATH = REPOSITORY_ROOT / "src" / "hailstack" / "cli" / "main.py"
VERSION_MODULE_PATH = REPOSITORY_ROOT / "src" / "hailstack" / "version.py"


def _read_definition() -> str:
    """Return the checked-in Apptainer definition content."""
    return APPTAINER_DEFINITION_PATH.read_text(encoding="utf-8")


def _read_pyproject() -> dict[str, object]:
    """Return the parsed pyproject metadata used by the container install."""
    with PYPROJECT_PATH.open("rb") as handle:
        return tomllib.load(handle)


def _read_cli_main() -> str:
    """Return the hailstack CLI entrypoint source."""
    return CLI_MAIN_PATH.read_text(encoding="utf-8")


def _read_version_module() -> str:
    """Return the project version module source."""
    return VERSION_MODULE_PATH.read_text(encoding="utf-8")


def _section(document: str, name: str) -> str:
    """Return a named Apptainer section body without executing anything."""
    match = re.search(
        rf"^%{name}\n(?P<body>.*?)(?=^%[a-z]+\n|\Z)",
        document,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None
    return match.group("body")


def test_m1_acceptance_1_sif_contains_hailstack_executable() -> None:
    """Stage and install the project so the container exposes `hailstack`."""
    definition = _read_definition()
    files_section = _section(definition, "files")
    post_section = _section(definition, "post")
    runscript_section = _section(definition, "runscript")
    pyproject = _read_pyproject()
    project_table = pyproject["project"]
    assert isinstance(project_table, dict)
    scripts_table = project_table["scripts"]
    assert isinstance(scripts_table, dict)

    assert APPTAINER_DEFINITION_PATH.is_file()
    assert "pyproject.toml /opt/hailstack/pyproject.toml" in files_section
    assert "src /opt/hailstack/src" in files_section
    assert "bundles.toml /opt/hailstack/bundles.toml" in files_section
    assert "README.md /opt/hailstack/README.md" in files_section
    assert "example-config.toml /opt/hailstack/example-config.toml" in files_section
    assert "ansible /opt/hailstack/ansible" in files_section
    assert "packer /opt/hailstack/packer" in files_section
    assert scripts_table["hailstack"] == "hailstack.cli.main:app"
    assert (
        "python3.14 -m pip install --no-cache-dir --editable /opt/hailstack"
        in post_section
    )
    assert 'exec hailstack "$@"' in runscript_section


def test_m1_acceptance_2_apptainer_run_version_prints_project_version() -> None:
    """Delegate `apptainer run ... --version` to the real CLI version callback."""
    definition = _read_definition()
    post_section = _section(definition, "post")
    runscript_section = _section(definition, "runscript")
    cli_main_source = _read_cli_main()
    version_module_source = _read_version_module()
    pyproject = _read_pyproject()
    project_table = pyproject["project"]
    assert isinstance(project_table, dict)

    assert 'exec hailstack "$@"' in runscript_section
    assert (
        "python3.14 -m pip install --no-cache-dir --editable /opt/hailstack"
        in post_section
    )
    assert 'typer.echo(f"hailstack {__version__}")' in cli_main_source
    assert 'help="Show version and exit."' in cli_main_source
    assert (
        re.search(
            r'^__version__ = "(?P<version>[^\"]+)"$',
            version_module_source,
            flags=re.MULTILINE,
        )
        is not None
    )
    assert project_table["version"] == "0.1.0"


def test_m1_acceptance_3_sif_contains_pulumi_packer_and_ansible_executables() -> None:
    """Install each companion tool in a location the container can execute."""
    definition = _read_definition()
    environment_section = _section(definition, "environment")
    post_section = _section(definition, "post")

    assert 'export PATH="/root/.pulumi/bin:$PATH"' in environment_section
    assert "curl -fsSL https://get.pulumi.com | sh" in post_section
    assert (
        "https://releases.hashicorp.com/packer/1.11.2/packer_1.11.2_linux_amd64.zip"
        in post_section
    )
    assert "unzip -o /tmp/packer.zip -d /usr/local/bin" in post_section
    assert "python3.14 -m pip install --no-cache-dir ansible" in post_section


def test_m1_acceptance_4_definition_encodes_sub_500mb_size_contract() -> None:
    """Use the documented slim base and cleanup steps for the size contract."""
    definition = _read_definition()
    post_section = _section(definition, "post")

    assert "Bootstrap: docker" in definition
    assert "From: python:3.14-slim" in definition
    assert "apt-get update && apt-get install -y" in post_section
    assert "apt-get clean" in post_section
    assert "rm -rf /var/lib/apt/lists/*" in post_section
    assert "--no-cache-dir" in post_section
