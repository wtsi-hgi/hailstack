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

"""Acceptance tests for the J1 destroy CLI command."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from hailstack.cli.commands import destroy as destroy_module
from hailstack.cli.main import app

runner = CliRunner()


class FakePulumiDestroyRunner:
    """Capture destroy-command Pulumi interactions."""

    def __init__(
        self,
        *,
        preview_output: str = "Plan: destroy 4 resources\n",
    ) -> None:
        """Initialise fake destroy responses."""
        self.preview_output = preview_output
        self.checked_backend = 0
        self.preview_calls = 0
        self.destroy_calls = 0

    def check_backend_access(self, config: object) -> None:
        """Record backend validation before preview or destroy."""
        del config
        self.checked_backend += 1

    def preview_destroy(self, config: object) -> str:
        """Return a preview-only Pulumi output string."""
        del config
        self.preview_calls += 1
        return self.preview_output

    def destroy(self, config: object) -> None:
        """Record destroy calls."""
        del config
        self.destroy_calls += 1


def _write_config(
    path: Path,
    *,
    cluster_name: str = "test-cluster",
    cluster_bundle: str = "removed-bundle",
    floating_ip: str = "198.51.100.20",
) -> Path:
    """Write a minimal valid destroy configuration file."""
    path.write_text(
        (
            "[cluster]\n"
            f'name = "{cluster_name}"\n'
            f'bundle = "{cluster_bundle}"\n'
            'master_flavour = "m2.2xlarge"\n'
            'worker_flavour = "m2.xlarge"\n'
            'network_name = "private-net"\n'
            f'floating_ip = "{floating_ip}"\n'
            'ssh_username = "ubuntu"\n\n'
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


def _install_fake_runner(
    monkeypatch: pytest.MonkeyPatch,
    pulumi_runner: FakePulumiDestroyRunner,
) -> None:
    """Install a fake Pulumi factory into the destroy module."""

    def fake_create_pulumi_destroy_runner(logger: object) -> FakePulumiDestroyRunner:
        del logger
        return pulumi_runner

    monkeypatch.setattr(
        destroy_module,
        "create_pulumi_destroy_runner",
        fake_create_pulumi_destroy_runner,
    )


def test_destroy_dry_run_shows_preview_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Show the destroy plan without destroying resources."""
    config_path = _write_config(tmp_path / "destroy.toml")
    fake_runner = FakePulumiDestroyRunner(preview_output="Plan: destroy 4 resources\n")
    _install_fake_runner(monkeypatch, fake_runner)

    result = runner.invoke(app, ["destroy", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert result.stdout == "Plan: destroy 4 resources\n"
    assert fake_runner.preview_calls == 1
    assert fake_runner.destroy_calls == 0


def test_destroy_with_exact_confirmation_runs_pulumi_destroy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Destroy the stack when the exact cluster name is confirmed."""
    config_path = _write_config(tmp_path / "destroy.toml")
    fake_runner = FakePulumiDestroyRunner()
    _install_fake_runner(monkeypatch, fake_runner)

    result = runner.invoke(
        app,
        ["destroy", "--config", str(config_path)],
        input="test-cluster\n",
    )

    assert result.exit_code == 0
    assert fake_runner.preview_calls == 1
    assert fake_runner.destroy_calls == 1


def test_destroy_with_incorrect_confirmation_aborts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Abort without destroying resources when confirmation does not match."""
    config_path = _write_config(tmp_path / "destroy.toml")
    fake_runner = FakePulumiDestroyRunner()
    _install_fake_runner(monkeypatch, fake_runner)

    result = runner.invoke(
        app,
        ["destroy", "--config", str(config_path)],
        input="wrong-name\n",
    )

    assert result.exit_code == 1
    assert "Aborted" in result.stdout
    assert fake_runner.preview_calls == 1
    assert fake_runner.destroy_calls == 0


def test_destroy_accepts_configured_floating_ip_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Destroy the stack successfully when the config includes a floating IP."""
    config_path = _write_config(
        tmp_path / "destroy.toml",
        floating_ip="203.0.113.44",
    )
    fake_runner = FakePulumiDestroyRunner()
    _install_fake_runner(monkeypatch, fake_runner)

    result = runner.invoke(
        app,
        ["destroy", "--config", str(config_path)],
        input="test-cluster\n",
    )

    assert result.exit_code == 0
    assert fake_runner.destroy_calls == 1


def test_destroy_logs_each_progress_stage_to_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Log the documented destroy stages to stderr."""
    config_path = _write_config(tmp_path / "destroy.toml")
    fake_runner = FakePulumiDestroyRunner()
    _install_fake_runner(monkeypatch, fake_runner)

    result = runner.invoke(
        app,
        ["destroy", "--config", str(config_path)],
        input="test-cluster\n",
    )

    assert result.exit_code == 0
    assert "config loaded" in result.stderr
    assert "previewing resources" in result.stderr
    assert "awaiting confirmation" in result.stderr
    assert "destroying infrastructure" in result.stderr
    assert "cleanup complete" in result.stderr


def test_destroy_success_outputs_final_destroyed_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Print the documented final success line after destroying the cluster."""
    config_path = _write_config(tmp_path / "destroy.toml")
    fake_runner = FakePulumiDestroyRunner()
    _install_fake_runner(monkeypatch, fake_runner)

    result = runner.invoke(
        app,
        ["destroy", "--config", str(config_path)],
        input="test-cluster\n",
    )

    assert result.exit_code == 0
    assert result.stdout.splitlines()[-1] == "Cluster 'test-cluster' destroyed."
