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
UBUNTU_SCRIPTS_PATH = REPOSITORY_ROOT / "packer" / "scripts" / "ubuntu"
VERSION_CHECK_PATTERN = re.compile(r'grep -F "\$\{?[A-Z0-9_]+_VERSION\}?"')
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
    bin_dir = tmp_path / "bin"
    (temp_root / "etc" / "systemd" / "system").mkdir(parents=True)
    (temp_root / "lib" / "systemd" / "system").mkdir(parents=True)
    (temp_root / "lib" / "systemd" / "system" / "nginx.service").write_text(
        "[Unit]\nDescription=nginx\n",
        encoding="utf-8",
    )
    bin_dir.mkdir()

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
