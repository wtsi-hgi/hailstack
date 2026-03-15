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

"""Acceptance tests for the M2 GitHub Actions CI workflow."""

import tomllib
from pathlib import Path
from typing import cast

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT_PATH = REPOSITORY_ROOT / "pyproject.toml"


def _require_dict(value: object, *, context: str) -> dict[str, object]:
    """Return a dictionary value with string keys."""
    assert isinstance(value, dict), f"{context} must be an object"
    raw_mapping = cast(dict[object, object], value)
    validated: dict[str, object] = {}
    for key, item in raw_mapping.items():
        assert isinstance(key, str), f"{context} keys must be strings"
        validated[key] = item
    return validated


def _require_list(value: object, *, context: str) -> list[object]:
    """Return a list value."""
    assert isinstance(value, list), f"{context} must be a list"
    return list(cast(list[object], value))


def _require_str(value: object, *, context: str) -> str:
    """Return a string value."""
    assert isinstance(value, str), f"{context} must be a string"
    return value


def _load_workflow() -> dict[str, object]:
    """Load the CI workflow YAML as a typed mapping."""
    loaded = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    return _require_dict(loaded, context="ci workflow")


def _load_pyproject() -> dict[str, object]:
    """Load pyproject.toml as a typed mapping."""
    with PYPROJECT_PATH.open("rb") as handle:
        loaded = tomllib.load(handle)
    return _require_dict(loaded, context="pyproject")


def _job_steps(job_name: str) -> list[dict[str, object]]:
    """Return the steps for one workflow job."""
    workflow = _load_workflow()
    jobs = _require_dict(workflow["jobs"], context="workflow jobs")
    job = _require_dict(jobs[job_name], context=f"{job_name} job")
    steps = _require_list(job["steps"], context=f"{job_name} job steps")
    return [_require_dict(step, context=f"{job_name} step") for step in steps]


def _step_by_name(job_name: str, step_name: str) -> dict[str, object]:
    """Return one named step from a workflow job."""
    for step in _job_steps(job_name):
        if step.get("name") == step_name:
            return step
    raise AssertionError(f"{job_name} job must define step '{step_name}'")


def test_ci_jobs_pin_packer_setup_and_install_the_requested_version() -> None:
    """Install a pinned Packer action in every job that exercises repo tooling."""
    expected_action = "hashicorp/setup-packer@1aa358be5cf73883762b302a3a03abd66e75b232"

    for job_name in ("lint", "typecheck", "test"):
        packer_step = _step_by_name(job_name, "Set up Packer")

        assert _require_str(packer_step["uses"], context="packer step uses") == (
            expected_action
        )
        with_block = _require_dict(
            packer_step["with"], context="packer step inputs")
        assert _require_str(with_block["version"], context="packer version") == (
            "1.11.2"
        )


def test_ci_workflow_runs_on_push_to_main_and_pull_requests() -> None:
    """Run CI on pushes to main and for all pull requests."""
    workflow = _load_workflow()

    trigger = _require_dict(workflow["on"], context="workflow trigger")
    push_trigger = _require_dict(trigger["push"], context="push trigger")
    branches = [
        _require_str(branch, context="push branch")
        for branch in _require_list(push_trigger["branches"], context="push branches")
    ]

    assert branches == ["main"]
    assert trigger["pull_request"] is None


def test_ci_lint_job_runs_ruff_commands_that_fail_on_lint_or_format_errors() -> None:
    """Run Ruff check plus Ruff format --check in the lint job."""
    lint_steps = _job_steps("lint")
    run_commands = {
        _require_str(step["name"], context="lint step name"): _require_str(
            step["run"], context="lint run command"
        )
        for step in lint_steps
        if "run" in step
    }

    assert run_commands["Run Ruff checks"] == "uv run ruff check src/ tests/"
    assert run_commands["Check Ruff formatting"] == (
        "uv run ruff format --check src/ tests/"
    )


def test_ci_typecheck_job_uses_repo_strict_pyright_configuration() -> None:
    """Run Pyright using the repo's strict pyproject configuration."""
    typecheck_step = _step_by_name(
        "typecheck", "Run Pyright strict type checks")
    pyproject = _load_pyproject()
    tool_config = _require_dict(pyproject["tool"], context="tool config")
    pyright_config = _require_dict(
        tool_config["pyright"], context="pyright config")

    assert _require_str(typecheck_step["run"], context="typecheck run command") == (
        "uv run pyright"
    )
    assert (
        _require_str(
            pyright_config["typeCheckingMode"],
            context="pyright typeCheckingMode",
        )
        == "strict"
    )


def test_ci_test_job_runs_full_pytest_suite_with_coverage() -> None:
    """Run all tests under tests/ and report coverage in the test job."""
    test_step = _step_by_name("test", "Run pytest with coverage")
    test_command = _require_str(test_step["run"], context="test run command")

    assert test_command == (
        "uv run --with pytest-cov pytest --cov=src/hailstack "
        "--cov-report=term-missing tests/ -v"
    )
    assert "tests/ -v" in test_command
    assert "--cov=src/hailstack" in test_command
    assert "--cov-report=term-missing" in test_command
