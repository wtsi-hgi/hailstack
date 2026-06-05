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

"""Integration tests for Packer shell provisioner scripts."""

import os
import re
import subprocess
from pathlib import Path
from typing import Final

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
BASE_SCRIPT_PATH = REPOSITORY_ROOT / "packer" / "scripts" / "base.sh"
HADOOP_SCRIPT_PATH = REPOSITORY_ROOT / "packer" / "scripts" / "ubuntu" / "hadoop.sh"
PACKAGES_SCRIPT_PATH = REPOSITORY_ROOT / "packer" / "scripts" / "ubuntu" / "packages.sh"
GNOMAD_SCRIPT_PATH = REPOSITORY_ROOT / "packer" / "scripts" / "ubuntu" / "gnomad.sh"
UBUNTU_SCRIPTS_PATH = REPOSITORY_ROOT / "packer" / "scripts" / "ubuntu"
VERSION_CHECK_PATTERN = re.compile(
    r'(grep -F "\$\{?[A-Z0-9_]+_VERSION\}?"|'
    r"hailstack_verify_version .*\$\{?[A-Z0-9_]+_VERSION\}?)"
)
MOCK_VERSION_ENV: Final[dict[str, str]] = {
    "GNOMAD_VERSION": "3.0.4",
    "HADOOP_VERSION": "3.4.1",
    "HAIL_VERSION": "0.2.137",
    "JAVA_VERSION": "11",
    "PYTHON_VERSION": "3.12",
    "SCALA_VERSION": "2.12.18",
    "SPARK_VERSION": "3.5.6",
}


def _script_paths(directory: Path) -> list[Path]:
    """Return sorted shell script paths from the target directory."""
    return sorted(directory.glob("*.sh"))


def _last_command(path: Path) -> str:
    """Return the last non-empty, non-comment line from a shell script."""
    lines = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert lines
    return lines[-1]


def _write_stub_command(path: Path, body: str) -> None:
    """Create an executable command stub used by the hermetic base.sh test."""
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def _stub_environment(tmp_path: Path) -> tuple[Path, Path]:
    """Return a temporary bin directory and command log for script stubs."""
    bin_dir = tmp_path / "bin"
    command_log = tmp_path / "commands.log"
    bin_dir.mkdir()
    return bin_dir, command_log


def _python_stub_body() -> str:
    """Return a python stub that can create a minimal virtual environment."""
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'if [[ "${1:-}" == "-m" && "${2:-}" == "venv" ]]; then\n'
        '  if [[ "${3:-}" == "--system-site-packages" ]]; then\n'
        "    target=$4\n"
        "  else\n"
        "    target=$3\n"
        "  fi\n"
        '  mkdir -p "$target/bin"\n'
        '  mkdir -p "$target/lib/python/site-packages"\n'
        "  cat >\"$target/bin/python\" <<'EOF'\n"
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'if [[ "${1:-}" == "-m" && "${2:-}" == "venv" ]]; then\n'
        '  if [[ "${3:-}" == "--system-site-packages" ]]; then\n'
        "    target=$4\n"
        "  else\n"
        "    target=$3\n"
        "  fi\n"
        '  mkdir -p "$target/bin"\n'
        '  mkdir -p "$target/lib/python/site-packages"\n'
        '  cp "$0" "$target/bin/python"\n'
        '  chmod +x "$target/bin/python"\n'
        "  exit 0\n"
        "fi\n"
        'if [[ "${1:-}" == "-m" && "${2:-}" == "pip" ]]; then\n'
        "  exit 0\n"
        "fi\n"
        'if [[ "${1:-}" == "-c" && "${2:-}" == '
        "*\"sysconfig.get_path('purelib')\"* ]]; then\n"
        '  script_dir=$(cd "$(dirname "$0")" && pwd)\n'
        '  printf "%s\\n" "${script_dir%/bin}/lib/python/site-packages"\n'
        "  exit 0\n"
        "fi\n"
        "exit 0\n"
        "EOF\n"
        '  chmod +x "$target/bin/python"\n'
        "  exit 0\n"
        "fi\n"
        "exit 0\n"
    )


def _rewrite_base_script(path: Path, temp_root: Path) -> Path:
    """Copy base.sh into a temporary tree and rewrite absolute system paths."""
    rewritten = path.read_text(encoding="utf-8")
    replacements = {
        "/opt/hailstack": str(temp_root / "opt" / "hailstack"),
        "/etc/systemd/system": str(temp_root / "etc" / "systemd" / "system"),
        "/lib/systemd/system": str(temp_root / "lib" / "systemd" / "system"),
    }
    for original, replacement in replacements.items():
        rewritten = rewritten.replace(original, replacement)

    rewritten_path = temp_root / "base.sh"
    rewritten_path.write_text(rewritten, encoding="utf-8")
    rewritten_path.chmod(0o755)
    return rewritten_path


def _rewrite_hadoop_script(path: Path, temp_root: Path) -> Path:
    """Copy hadoop.sh into a temporary tree and rewrite absolute system paths."""
    rewritten = path.read_text(encoding="utf-8")
    replacements = (
        ("/tmp/", f"{temp_root}/tmp/"),
        ("/opt", str(temp_root / "opt")),
        ("/etc/systemd/system", str(temp_root / "etc" / "systemd" / "system")),
    )
    for original, replacement in replacements:
        rewritten = rewritten.replace(original, replacement)

    rewritten_path = temp_root / "hadoop.sh"
    rewritten_path.write_text(rewritten, encoding="utf-8")
    rewritten_path.chmod(0o755)
    return rewritten_path


def _rewrite_gnomad_script(path: Path, temp_root: Path) -> Path:
    """Copy gnomad.sh into a temporary tree and rewrite absolute system paths."""
    rewritten = path.read_text(encoding="utf-8").replace(
        "/opt/hailstack",
        str(temp_root / "opt" / "hailstack"),
    )

    rewritten_path = temp_root / "gnomad.sh"
    rewritten_path.write_text(rewritten, encoding="utf-8")
    rewritten_path.chmod(0o755)
    return rewritten_path


def test_o2_each_ubuntu_provisioner_script_ends_with_version_check_command() -> None:
    """Require every Ubuntu provisioner script to end with a version-check command."""
    offenders = [
        f"{path.name}: {_last_command(path)}"
        for path in _script_paths(UBUNTU_SCRIPTS_PATH)
        if VERSION_CHECK_PATTERN.search(_last_command(path)) is None
    ]

    assert offenders == []


def test_o2_base_script_exits_zero_with_mock_version_environment(
    tmp_path: Path,
) -> None:
    """Run a hermetic temp copy of base.sh with mocked version vars and tools."""
    temp_root = tmp_path / "root"
    bin_dir, _command_log = _stub_environment(tmp_path)
    (temp_root / "etc" / "systemd" / "system").mkdir(parents=True)
    (temp_root / "lib" / "systemd" / "system").mkdir(parents=True)
    (temp_root / "lib" / "systemd" / "system" / "nginx.service").write_text(
        "[Unit]\nDescription=nginx\n",
        encoding="utf-8",
    )

    _write_stub_command(
        bin_dir / "apt-get",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
    )
    _write_stub_command(
        bin_dir / "apt-cache",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
    )
    _write_stub_command(
        bin_dir / "add-apt-repository",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
    )
    _write_stub_command(
        bin_dir / "fuser",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 1\n",
    )
    _write_stub_command(
        bin_dir / "systemctl",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
    )
    _write_stub_command(
        bin_dir / "install",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'if [[ "${1:-}" == "-d" ]]; then\n'
        "  shift\n"
        "  mode=755\n"
        '  if [[ "${1:-}" == "-m" ]]; then\n'
        "    mode=$2\n"
        "    shift 2\n"
        "  fi\n"
        '  mkdir -p "$1"\n'
        '  chmod "$mode" "$1"\n'
        "  exit 0\n"
        "fi\n"
        "exit 0\n",
    )
    _write_stub_command(
        bin_dir / "python3",
        _python_stub_body(),
    )
    _write_stub_command(
        bin_dir / "python3.12",
        _python_stub_body(),
    )

    script_path = _rewrite_base_script(BASE_SCRIPT_PATH, temp_root)
    env = dict(os.environ)
    env.update(MOCK_VERSION_ENV)
    env["HAILSTACK_PACKER_APT_HELPER"] = str(
        REPOSITORY_ROOT / "packer" / "scripts" / "apt-locks.sh"
    )
    env["PATH"] = str(bin_dir) + os.pathsep + env.get("PATH", "")

    result = subprocess.run(
        ["bash", str(script_path)],
        capture_output=True,
        check=False,
        cwd=REPOSITORY_ROOT,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_o2_hadoop_script_discovers_java_home_from_installed_java(
    tmp_path: Path,
) -> None:
    """Discover JAVA_HOME for Hadoop when sudo -E does not provide it."""
    temp_root = tmp_path / "root"
    bin_dir, command_log = _stub_environment(tmp_path)
    (temp_root / "tmp").mkdir(parents=True)
    (temp_root / "opt").mkdir(parents=True)
    (temp_root / "etc" / "systemd" / "system").mkdir(parents=True)
    java_home = temp_root / "usr" / "lib" / "jvm" / "java-11-openjdk-amd64"
    (java_home / "bin").mkdir(parents=True)
    (java_home / "bin" / "java").write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
        encoding="utf-8",
    )
    (java_home / "bin" / "java").chmod(0o755)
    (bin_dir / "java").symlink_to(java_home / "bin" / "java")

    _write_stub_command(
        bin_dir / "curl",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'output_path=""\n'
        'url=""\n'
        "while (($# > 0)); do\n"
        '  if [[ "$1" == "-o" ]]; then\n'
        "    output_path=$2\n"
        "    shift 2\n"
        "    continue\n"
        "  fi\n"
        '  if [[ "$1" == http* ]]; then\n'
        "    url=$1\n"
        "  fi\n"
        "  shift\n"
        "done\n"
        'expected="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"\n'
        'printf "curl %s -> %s\\n" "$url" "$output_path" >>"${HAILSTACK_COMMAND_LOG}"\n'
        '[[ "$url" == "$expected" ]]\n'
        '[[ -n "$output_path" ]]\n'
        'printf "hadoop archive\\n" >"$output_path"\n',
    )
    _write_stub_command(
        bin_dir / "tar",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'printf "tar %s\\n" "$*" >>"${HAILSTACK_COMMAND_LOG}"\n'
        'target=""\n'
        "while (($# > 0)); do\n"
        '  if [[ "$1" == "-C" ]]; then\n'
        "    target=$2\n"
        "    shift 2\n"
        "    continue\n"
        "  fi\n"
        "  shift\n"
        "done\n"
        '[[ -n "$target" ]]\n'
        'install_dir="$target/hadoop-${HADOOP_VERSION}"\n'
        'mkdir -p "$install_dir/bin" "$install_dir/etc/hadoop"\n'
        'printf "# Hadoop env\\n" >"$install_dir/etc/hadoop/hadoop-env.sh"\n'
        "cat >\"$install_dir/bin/hadoop\" <<'EOF'\n"
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'install_dir="$(cd "$(dirname "$0")/.." && pwd)"\n'
        'env_file="${install_dir}/etc/hadoop/hadoop-env.sh"\n'
        'if [[ -r "${env_file}" ]]; then\n'
        '  source "${env_file}"\n'
        "fi\n"
        'if [[ -z "${JAVA_HOME:-}" ]]; then\n'
        '  printf "ERROR: JAVA_HOME is not set and could not be found.\\n" >&2\n'
        "  exit 1\n"
        "fi\n"
        'printf "hadoop version JAVA_HOME=%s\\n" "${JAVA_HOME}" '
        '>>"${HAILSTACK_COMMAND_LOG}"\n'
        'printf "Hadoop %s\\n" "${HADOOP_VERSION}"\n'
        "EOF\n"
        'chmod +x "$install_dir/bin/hadoop"\n',
    )
    _write_stub_command(
        bin_dir / "systemctl",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
    )

    script_path = _rewrite_hadoop_script(HADOOP_SCRIPT_PATH, temp_root)
    env = dict(os.environ)
    env.update(MOCK_VERSION_ENV)
    env.pop("JAVA_HOME", None)
    env["HAILSTACK_COMMAND_LOG"] = str(command_log)
    env["PATH"] = str(bin_dir) + os.pathsep + env.get("PATH", "")

    result = subprocess.run(
        ["bash", str(script_path)],
        capture_output=True,
        check=False,
        cwd=REPOSITORY_ROOT,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    hadoop_env = temp_root / "opt" / "hadoop-3.4.1" / "etc" / "hadoop" / "hadoop-env.sh"

    assert f"export JAVA_HOME={java_home}" in hadoop_env.read_text(encoding="utf-8")
    assert f"hadoop version JAVA_HOME={java_home}" in command_log.read_text(
        encoding="utf-8"
    )


def test_o2_packages_script_installs_configured_scala_version(
    tmp_path: Path,
) -> None:
    """Install the configured Scala deb instead of Ubuntu's generic scala package."""
    bin_dir, command_log = _stub_environment(tmp_path)
    scala_state = tmp_path / "scala-version.txt"

    _write_stub_command(
        bin_dir / "apt-get",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'printf "apt-get %s\\n" "$*" >>"${HAILSTACK_COMMAND_LOG}"\n'
        'for arg in "$@"; do\n'
        '  if [[ "$arg" == "scala" ]]; then\n'
        '    printf "generic Ubuntu scala package was requested\\n" >&2\n'
        "    exit 42\n"
        "  fi\n"
        '  if [[ "$arg" == */scala-"${SCALA_VERSION}".deb ]]; then\n'
        '    printf "%s\\n" "${SCALA_VERSION}" >"${HAILSTACK_SCALA_STATE}"\n'
        "  fi\n"
        "done\n"
        "exit 0\n",
    )
    _write_stub_command(
        bin_dir / "curl",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'expected="https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb"\n'
        'output_path=""\n'
        'url=""\n'
        "while (($# > 0)); do\n"
        '  if [[ "$1" == "-o" ]]; then\n'
        "    output_path=$2\n"
        "    shift 2\n"
        "    continue\n"
        "  fi\n"
        '  if [[ "$1" == http* ]]; then\n'
        "    url=$1\n"
        "  fi\n"
        "  shift\n"
        "done\n"
        'printf "curl %s -> %s\\n" "$url" "$output_path" >>"${HAILSTACK_COMMAND_LOG}"\n'
        '[[ "$url" == "$expected" ]]\n'
        '[[ -n "$output_path" ]]\n'
        'printf "scala deb\\n" >"$output_path"\n',
    )
    _write_stub_command(
        bin_dir / "java",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'if [[ "${1:-}" == "-version" ]]; then\n'
        '  printf "openjdk version \\"11.0.31\\"\\n" >&2\n'
        "  exit 0\n"
        "fi\n"
        "exit 1\n",
    )
    _write_stub_command(
        bin_dir / "python3.12",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'if [[ "${1:-}" == "--version" ]]; then\n'
        '  printf "Python 3.12.13\\n"\n'
        "  exit 0\n"
        "fi\n"
        "exit 1\n",
    )
    _write_stub_command(
        bin_dir / "scala",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'version="2.11.12"\n'
        'if [[ -r "${HAILSTACK_SCALA_STATE}" ]]; then\n'
        '  version="$(cat "${HAILSTACK_SCALA_STATE}")"\n'
        "fi\n"
        'printf "Scala code runner version %s -- Lightbend\\n" "$version" >&2\n',
    )
    _write_stub_command(
        bin_dir / "fuser",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 1\n",
    )
    _write_stub_command(
        bin_dir / "systemctl",
        "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
    )

    env = dict(os.environ)
    env.update(MOCK_VERSION_ENV)
    env["HAILSTACK_COMMAND_LOG"] = str(command_log)
    env["HAILSTACK_PACKER_APT_HELPER"] = str(
        REPOSITORY_ROOT / "packer" / "scripts" / "apt-locks.sh"
    )
    env["HAILSTACK_SCALA_STATE"] = str(scala_state)
    env["PATH"] = str(bin_dir) + os.pathsep + env.get("PATH", "")
    env["TMPDIR"] = str(tmp_path)

    result = subprocess.run(
        ["bash", str(PACKAGES_SCRIPT_PATH)],
        capture_output=True,
        check=False,
        cwd=REPOSITORY_ROOT,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    commands = command_log.read_text(encoding="utf-8").splitlines()
    install_lines = [line for line in commands if " install " in line]

    assert any(
        "https://downloads.lightbend.com/scala/2.12.18/scala-2.12.18.deb" in line
        for line in commands
    )
    assert any("scala-2.12.18.deb" in line for line in install_lines)
    assert not any(re.search(r"(^| )scala( |$)", line) for line in install_lines)


def test_o2_gnomad_script_keeps_data_release_separate_from_package_pin(
    tmp_path: Path,
) -> None:
    """Install gnomAD methods without using the data release as a package version."""
    temp_root = tmp_path / "root"
    base_venv = temp_root / "opt" / "hailstack" / "base-venv"
    bin_dir = base_venv / "bin"
    command_log = tmp_path / "commands.log"
    bin_dir.mkdir(parents=True)

    _write_stub_command(
        bin_dir / "uv",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'printf "uv %s\\n" "$*" >>"${HAILSTACK_COMMAND_LOG}"\n'
        'for arg in "$@"; do\n'
        '  if [[ "$arg" == "gnomad==${GNOMAD_VERSION}" ]]; then\n'
        '    printf "data release used as package pin\\n" >&2\n'
        "    exit 42\n"
        "  fi\n"
        "done\n"
        '[[ "$*" == *"gnomad=="* ]]\n',
    )
    _write_stub_command(
        bin_dir / "python",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'printf "python %s\\n" "$*" >>"${HAILSTACK_COMMAND_LOG}"\n'
        '[[ "${1:-}" == "-c" ]]\n'
        '[[ "$*" == *"import gnomad"* ]]\n'
        '[[ "$*" == *"importlib.metadata.version"* ]]\n'
        'printf "gnomAD data release %s via gnomad package 0.8.2\\n" '
        '"$GNOMAD_VERSION"\n',
    )

    script_path = _rewrite_gnomad_script(GNOMAD_SCRIPT_PATH, temp_root)
    env = dict(os.environ)
    env.update(MOCK_VERSION_ENV)
    env["GNOMAD_METHODS_VERSION"] = "0.8.2"
    env["HAILSTACK_COMMAND_LOG"] = str(command_log)

    result = subprocess.run(
        ["bash", str(script_path)],
        capture_output=True,
        check=False,
        cwd=REPOSITORY_ROOT,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    commands = command_log.read_text(encoding="utf-8").splitlines()

    assert any("gnomad==0.8.2" in line for line in commands)
    assert not any("gnomad==3.0.4" in line for line in commands)
