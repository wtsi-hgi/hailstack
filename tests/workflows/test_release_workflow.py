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

"""Acceptance tests for the M3 GitHub release workflow."""

from pathlib import Path
from typing import cast

import yaml

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] /
    ".github" / "workflows" / "release.yml"
)
ROOT_BUILD_COMMAND = (
    'sudo apptainer build "dist/hailstack-${GITHUB_REF_NAME}.sif" Apptainer.def'
)
UNPRIVILEGED_BUILD_COMMAND = (
    'apptainer build "dist/hailstack-${GITHUB_REF_NAME}.sif" Apptainer.def'
)


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


def _require_bool(value: object, *, context: str) -> bool:
    """Return a boolean value."""
    assert isinstance(value, bool), f"{context} must be a bool"
    return value


def _load_workflow() -> dict[str, object]:
    """Load the release workflow YAML as a typed mapping."""
    loaded = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    return _require_dict(loaded, context="release workflow")


def _release_step() -> dict[str, object]:
    """Return the GitHub release publishing step."""
    workflow = _load_workflow()
    jobs = _require_dict(workflow["jobs"], context="workflow jobs")
    release_job = _require_dict(jobs["release"], context="release job")
    steps = _require_list(release_job["steps"], context="release job steps")
    for raw_step in steps:
        step = _require_dict(raw_step, context="workflow step")
        uses = step.get("uses")
        if isinstance(uses, str) and uses == "softprops/action-gh-release@v2":
            return step
    raise AssertionError("release workflow must define a GitHub release step")


def _step_named(step_name: str) -> dict[str, object]:
    """Return a named workflow step."""
    workflow = _load_workflow()
    jobs = _require_dict(workflow["jobs"], context="workflow jobs")
    release_job = _require_dict(jobs["release"], context="release job")
    steps = _require_list(release_job["steps"], context="release job steps")
    for raw_step in steps:
        step = _require_dict(raw_step, context="workflow step")
        name = step.get("name")
        if isinstance(name, str) and name == step_name:
            return step
    raise AssertionError(f"release workflow must define a '{step_name}' step")


def test_release_workflow_triggers_on_version_tags() -> None:
    """Trigger the workflow only on version tag pushes matching v*."""
    workflow = _load_workflow()

    trigger = _require_dict(workflow["on"], context="workflow trigger")
    push_trigger = _require_dict(trigger["push"], context="push trigger")
    tags = [
        _require_str(tag, context="push tag")
        for tag in _require_list(push_trigger["tags"], context="push tags")
    ]

    assert tags == ["v*"]


def test_release_workflow_uploads_sif_asset_to_github_release() -> None:
    """Build a SIF from the repo root and attach it to the GitHub release."""
    build_step = _step_named("Build Apptainer SIF")
    build_script = _require_str(build_step["run"], context="build step run")
    assert ROOT_BUILD_COMMAND in build_script

    release_step = _release_step()
    release_inputs = _require_dict(
        release_step["with"], context="release step inputs")
    assert (
        _require_str(release_inputs["files"], context="release asset path")
        == "dist/hailstack-${{ github.ref_name }}.sif"
    )


def test_release_workflow_builds_with_explicit_privilege_on_github_runners() -> None:
    """Use an explicitly privileged Apptainer build on GitHub-hosted runners."""
    install_step = _step_named("Install Apptainer")
    build_step = _step_named("Build Apptainer SIF")

    install_script = _require_str(
        install_step["run"], context="install step run")
    build_script = _require_str(build_step["run"], context="build step run")

    assert "sudo apt-get install -y apptainer" in install_script
    assert ROOT_BUILD_COMMAND in build_script
    assert UNPRIVILEGED_BUILD_COMMAND not in build_script.replace(
        ROOT_BUILD_COMMAND, ""
    )


def test_release_workflow_enables_generated_release_notes() -> None:
    """Generate GitHub release notes automatically from tagged commits."""
    workflow = _load_workflow()
    permissions = _require_dict(
        workflow["permissions"], context="workflow permissions")
    release_step = _release_step()
    release_inputs = _require_dict(
        release_step["with"], context="release step inputs")

    assert (
        _require_str(permissions["contents"],
                     context="contents permission") == "write"
    )
    assert (
        _require_bool(
            release_inputs["generate_release_notes"],
            context="generate release notes",
        )
        is True
    )
