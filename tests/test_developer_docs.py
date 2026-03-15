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

"""Validation tests for developer-facing repository documentation."""

import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONTRIBUTING_PATH = REPOSITORY_ROOT / "CONTRIBUTING.md"
AI_PATH = REPOSITORY_ROOT / "AI.md"
README_PATH = REPOSITORY_ROOT / "README.md"
ERRORS_PATH = REPOSITORY_ROOT / "src" / "hailstack" / "errors.py"

CONTRIBUTING_SECTIONS = (
    "Project Structure",
    "Development Setup",
    "Testing",
    "Building Apptainer Image",
    "Adding a New Bundle",
    "Architecture Decisions",
)

AI_SECTIONS = (
    "Coding Conventions",
    "Commands",
    "Error Hierarchy",
    "Key Modules",
    "Common Tasks",
)

SHARED_COMMANDS = (
    "uv run ruff check src/ tests/",
    "uv run ruff format src/ tests/",
    "uv run pyright",
    "uv run pytest tests/ -v",
    "uv run pytest tests/ -v -k <pattern>",
)

CONTRIBUTING_REQUIRED_SNIPPETS = (
    "pytest",
    "fixtures",
    "parametrization",
    "hailstack build-image --config <config.toml> --bundle <bundle-id>",
    "not in editable mode",
    "Submit a PR",
    "Pulumi is used instead of Terraform",
    "Ceph S3",
    "fat and pre-baked",
)

AI_REQUIRED_SNIPPETS = (
    "Pydantic v2",
    "Typer patterns",
    "Update a Packer script",
    "Add a test",
)


def _read(path: Path) -> str:
    """Return the UTF-8 text for one documentation file."""
    return path.read_text(encoding="utf-8")


def _heading_titles(document: str) -> list[str]:
    """Return all second-level Markdown heading titles."""
    return re.findall(r"^## (.+)$", document, flags=re.MULTILINE)


def _shared_command_lines(document: str) -> list[str]:
    """Extract the shared lint, type-check, and test commands from one doc."""
    lines = [line.strip() for line in document.splitlines()]
    return list(dict.fromkeys(line for line in lines if line in SHARED_COMMANDS))


def _error_class_names() -> list[str]:
    """Return every error class declared in src/hailstack/errors.py."""
    return re.findall(r"^class ([A-Za-z0-9_]+)\(", _read(ERRORS_PATH), re.MULTILINE)


def test_contributing_has_required_sections() -> None:
    """Require the six documented CONTRIBUTING.md sections."""
    headings = _heading_titles(_read(CONTRIBUTING_PATH))

    assert list(CONTRIBUTING_SECTIONS) == headings


def test_ai_has_required_sections() -> None:
    """Require the five documented AI.md sections."""
    headings = _heading_titles(_read(AI_PATH))

    assert list(AI_SECTIONS) == headings


def test_contributing_and_ai_share_the_same_core_commands() -> None:
    """Keep the canonical lint, type-check, and test commands identical."""
    contributing_commands = _shared_command_lines(_read(CONTRIBUTING_PATH))
    ai_commands = _shared_command_lines(_read(AI_PATH))

    assert contributing_commands == list(SHARED_COMMANDS)
    assert ai_commands == list(SHARED_COMMANDS)


def test_ai_lists_every_error_class_from_the_error_module() -> None:
    """Ensure AI.md stays in sync with the current exception hierarchy."""
    ai_text = _read(AI_PATH)

    for error_name in _error_class_names():
        assert re.search(rf"\b{error_name}\b", ai_text) is not None


def test_contributing_covers_required_workflows_and_architecture_rationale() -> None:
    """Require the N2-specific testing, bundle, and architecture guidance."""
    contributing_text = _read(CONTRIBUTING_PATH)

    for snippet in CONTRIBUTING_REQUIRED_SNIPPETS:
        assert snippet in contributing_text


def test_readme_reboot_section_documents_explicit_ssh_key_usage() -> None:
    """Keep README reboot docs aligned with the CLI's SSH key support."""
    readme_text = _read(README_PATH)

    assert "### `hailstack reboot`" in readme_text
    assert "| `--ssh-key PATH` | SSH private key path." in readme_text
    assert (
        "hailstack reboot --config my-cluster.toml --dotenv .env --ssh-key "
        "~/.ssh/my-cluster-key"
    ) in readme_text


def test_ai_covers_required_conventions_and_common_tasks() -> None:
    """Require the N2-specific AI guidance beyond headings and commands."""
    ai_text = _read(AI_PATH)

    for snippet in AI_REQUIRED_SNIPPETS:
        assert snippet in ai_text
