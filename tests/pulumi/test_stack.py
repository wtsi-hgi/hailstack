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

"""Acceptance tests for Pulumi automation stack selection semantics."""

from types import SimpleNamespace

import pytest

from hailstack.errors import PulumiError
from hailstack.pulumi import stack as stack_module


class FakeAutoStack:
    """Capture preview and destroy interactions from the runner."""

    def __init__(self) -> None:
        """Initialise counters for the fake stack."""
        self.preview_calls = 0
        self.destroy_calls = 0

    def preview(self, *, on_output: object) -> object:
        """Return a fake preview result."""
        del on_output
        self.preview_calls += 1
        return SimpleNamespace(stdout="preview\n")

    def destroy(self, *, remove: bool = False) -> None:
        """Record destroy requests from the runner."""
        assert remove is True
        self.destroy_calls += 1


def _config() -> object:
    """Return the subset of config the runner needs for tests."""
    return SimpleNamespace(
        cluster=SimpleNamespace(name="test-cluster"),
        ceph_s3=SimpleNamespace(
            bucket="hailstack-state",
            endpoint="https://ceph.example.invalid",
            access_key="state-access",
            secret_key="state-secret",
        ),
    )


def test_preview_destroy_selects_existing_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use select-stack for destroy previews so dry runs do not create state."""
    fake_stack = FakeAutoStack()
    calls: list[str] = []

    def fake_select_stack(**kwargs: object) -> FakeAutoStack:
        del kwargs
        calls.append("select")
        return fake_stack

    def fake_create_or_select_stack(**kwargs: object) -> FakeAutoStack:
        del kwargs
        calls.append("create_or_select")
        return fake_stack

    monkeypatch.setattr(stack_module.auto, "select_stack", fake_select_stack)
    monkeypatch.setattr(
        stack_module.auto,
        "create_or_select_stack",
        fake_create_or_select_stack,
    )

    result = stack_module.AutomationStackRunner().preview_destroy(_config())

    assert result == "preview\n"
    assert calls == ["select"]
    assert fake_stack.preview_calls == 1


def test_destroy_raises_clear_error_when_stack_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail destroy with a clear missing-stack error instead of a false success."""

    def fake_select_stack(**kwargs: object) -> FakeAutoStack:
        del kwargs
        raise RuntimeError("no stack named hailstack-test-cluster")

    monkeypatch.setattr(stack_module.auto, "select_stack", fake_select_stack)

    with pytest.raises(PulumiError, match="does not exist"):
        stack_module.AutomationStackRunner().destroy(_config())
