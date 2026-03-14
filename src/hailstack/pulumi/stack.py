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

"""Pulumi automation helpers for cluster lifecycle commands."""

import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from pulumi import automation as auto

from hailstack.config import Bundle, ClusterConfig
from hailstack.errors import PulumiError, S3Error
from hailstack.pulumi.resources import create_cluster_resources

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class CreateResult:
    """Represent the subset of Pulumi outputs needed by the create command."""

    master_public_ip: str
    stdout: str = ""


class AutomationStackRunner:
    """Wrap Pulumi automation operations in a command-friendly API."""

    def __init__(
        self,
        logger: logging.Logger | None = None,
        *,
        work_dir: Path = REPOSITORY_ROOT,
    ) -> None:
        """Initialise the runner with a repository work directory."""
        self._logger = logger or logging.getLogger(__name__)
        self._work_dir = work_dir

    def check_backend_access(self, config: ClusterConfig) -> None:
        """Validate that the configured Ceph S3 backend accepts authentication."""
        env = self._pulumi_env(config)
        backend_url = self._backend_url(config)
        try:
            result = subprocess.run(
                ["pulumi", "login", "--non-interactive", backend_url],
                capture_output=True,
                check=False,
                cwd=self._work_dir,
                env=env,
                text=True,
            )
        except FileNotFoundError as error:
            raise PulumiError("Pulumi CLI not found") from error

        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            endpoint = config.ceph_s3.endpoint.removeprefix("https://").removeprefix(
                "http://"
            )
            raise S3Error(
                f"Unable to access Ceph S3 backend at {endpoint}: {detail}")

    def preview(self, config: ClusterConfig, bundle: Bundle) -> str:
        """Run a Pulumi preview and return the rendered plan output."""
        stack = self._get_stack(config, bundle, create_if_missing=True)
        output_lines: list[str] = []
        try:
            result = stack.preview(on_output=output_lines.append)
        except Exception as error:
            raise PulumiError(f"Pulumi preview failed: {error}") from error

        return result.stdout or "".join(output_lines)

    def preview_destroy(self, config: ClusterConfig) -> str:
        """Run a Pulumi preview showing the resources that would be destroyed."""
        stack = self._get_stack(config, None, create_if_missing=False)
        output_lines: list[str] = []
        try:
            result = stack.preview(on_output=output_lines.append)
        except Exception as error:
            raise PulumiError(f"Pulumi preview failed: {error}") from error

        return result.stdout or "".join(output_lines)

    def up(self, config: ClusterConfig, bundle: Bundle) -> CreateResult:
        """Apply the Pulumi stack and return the master floating IP output."""
        stack = self._get_stack(config, bundle, create_if_missing=True)
        output_lines: list[str] = []
        try:
            result = stack.up(on_output=output_lines.append)
        except Exception as error:
            raise PulumiError(f"Pulumi up failed: {error}") from error

        master_public_ip = self._master_public_ip(result.outputs)
        stdout = result.stdout or "".join(output_lines)
        return CreateResult(master_public_ip=master_public_ip, stdout=stdout)

    def destroy(self, config: ClusterConfig, bundle: Bundle | None = None) -> None:
        """Destroy the Pulumi stack for cleanup or an explicit destroy command."""
        stack = self._get_stack(config, bundle, create_if_missing=False)
        try:
            stack.destroy(remove=True)
        except Exception as error:
            raise PulumiError(f"Pulumi destroy failed: {error}") from error

    def _get_stack(
        self,
        config: ClusterConfig,
        bundle: Bundle | None,
        *,
        create_if_missing: bool,
    ) -> auto.Stack:
        """Select the cluster stack and optionally create it when missing."""

        def pulumi_program() -> None:
            if bundle is not None:
                create_cluster_resources(config, bundle)

        workspace_options = auto.LocalWorkspaceOptions(
            env_vars=self._pulumi_env(config),
            project_settings=auto.ProjectSettings(
                name="hailstack",
                runtime="python",
                backend=auto.ProjectBackend(url=self._backend_url(config)),
            ),
            work_dir=str(self._work_dir),
        )

        try:
            stack_name = f"hailstack-{config.cluster.name}"
            if create_if_missing:
                return auto.create_or_select_stack(
                    stack_name=stack_name,
                    project_name="hailstack",
                    program=pulumi_program,
                    opts=workspace_options,
                )

            return auto.select_stack(
                stack_name=stack_name,
                project_name="hailstack",
                program=pulumi_program,
                opts=workspace_options,
            )
        except Exception as error:
            if not create_if_missing and _is_missing_stack_error(error):
                raise PulumiError(
                    f"Pulumi stack hailstack-{config.cluster.name} does not exist"
                ) from error
            raise PulumiError(
                f"Unable to initialise Pulumi stack: {error}") from error

    @staticmethod
    def _backend_url(config: ClusterConfig) -> str:
        """Render the documented Pulumi Ceph backend URL."""
        return f"s3://{config.ceph_s3.bucket}?endpoint={config.ceph_s3.endpoint}"

    @staticmethod
    def _pulumi_env(config: ClusterConfig) -> dict[str, str]:
        """Build the process environment required for Pulumi backend access."""
        env = dict(os.environ)
        env["AWS_ACCESS_KEY_ID"] = config.ceph_s3.access_key
        env["AWS_SECRET_ACCESS_KEY"] = config.ceph_s3.secret_key
        return env

    @staticmethod
    def _master_public_ip(outputs: auto.OutputMap) -> str:
        """Extract the required master_public_ip output from a Pulumi update."""
        output = outputs.get("master_public_ip")
        if output is None or not isinstance(output.value, str) or not output.value:
            raise PulumiError(
                "Pulumi create completed without a master_public_ip output"
            )
        return output.value


def _is_missing_stack_error(error: Exception) -> bool:
    """Return true when the Pulumi automation error indicates no stack exists."""
    message = str(error).lower()
    return "not found" in message or "no stack named" in message


__all__ = ["AutomationStackRunner", "CreateResult", "REPOSITORY_ROOT"]
