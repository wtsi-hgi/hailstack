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

"""Phase 6.6 end-to-end integration test skeleton."""

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(slots=True)
class OpenStackCredentials:
    """Represent the OpenStack environment required for end-to-end tests."""

    auth_url: str
    username: str
    password: str
    project_name: str
    region_name: str | None
    user_domain_name: str | None
    project_domain_name: str | None


@dataclass(slots=True)
class CleanupPlan:
    """Track whether teardown should destroy a created test cluster."""

    destroy_armed: bool = False
    config_path: Path | None = None
    destroy_command_file: Path | None = None

    def arm_destroy(self, config_path: Path) -> None:
        """Record the config that a future teardown destroy should target."""
        self.destroy_armed = True
        self.config_path = config_path

    def execute_destroy_placeholder(self) -> None:
        """Materialize the destroy command a real teardown would execute."""
        if not self.destroy_armed or self.config_path is None:
            return

        self.destroy_command_file = self.config_path.with_suffix(
            ".destroy.txt")
        self.destroy_command_file.write_text(
            (
                f'printf "%s\\n" "integration-skeleton" | '
                f'hailstack destroy --config "{self.config_path}"\n'
            ),
            encoding="utf-8",
        )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        pytest.skip(f"missing required OpenStack credential env var: {name}")
    return value


@pytest.fixture
def openstack_credentials() -> OpenStackCredentials:
    """Provide OpenStack credentials from the environment."""
    return OpenStackCredentials(
        auth_url=_require_env("OS_AUTH_URL"),
        username=_require_env("OS_USERNAME"),
        password=_require_env("OS_PASSWORD"),
        project_name=_require_env("OS_PROJECT_NAME"),
        region_name=os.environ.get("OS_REGION_NAME"),
        user_domain_name=os.environ.get("OS_USER_DOMAIN_NAME"),
        project_domain_name=os.environ.get("OS_PROJECT_DOMAIN_NAME"),
    )


@pytest.fixture
def temporary_config(
    tmp_path: Path,
    openstack_credentials: OpenStackCredentials,
) -> Path:
    """Write a temporary TOML config for future end-to-end runs."""
    del openstack_credentials
    config_path = tmp_path / "integration-e2e.toml"
    config_path.write_text(
        (
            "[cluster]\n"
            'name = "integration-skeleton"\n'
            'bundle = "example-bundle"\n'
            "num_workers = 2\n"
            'master_flavour = "m2.2xlarge"\n'
            'worker_flavour = "m2.xlarge"\n'
            'network_name = "private-net"\n'
            'ssh_username = "ubuntu"\n\n'
            "[ceph_s3]\n"
            'endpoint = "https://ceph.example.invalid"\n'
            'bucket = "hailstack-state"\n'
            'access_key = "state-access"\n'
            'secret_key = "state-secret"\n\n'
            "[ssh_keys]\n"
            'public_keys = ["ssh-ed25519 AAAA skeleton@test"]\n'
        ),
        encoding="utf-8",
    )
    return config_path


@pytest.fixture
def cleanup() -> Iterator[CleanupPlan]:
    """Track destroy-on-teardown intent for future lifecycle coverage."""
    plan = CleanupPlan()
    yield plan
    plan.execute_destroy_placeholder()


def _stub(reason: str) -> None:
    pytest.skip(reason)


@pytest.mark.integration
def test_build_image_skeleton(
    openstack_credentials: OpenStackCredentials,
    temporary_config: Path,
    cleanup: CleanupPlan,
) -> None:
    """Placeholder for the build-image lifecycle step."""
    del openstack_credentials, temporary_config, cleanup
    _stub("end-to-end skeleton only; build-image workflow not implemented")


@pytest.mark.integration
def test_create_skeleton(
    openstack_credentials: OpenStackCredentials,
    temporary_config: Path,
    cleanup: CleanupPlan,
) -> None:
    """Placeholder for the create lifecycle step."""
    del openstack_credentials
    cleanup.arm_destroy(temporary_config)
    _stub("end-to-end skeleton only; create workflow not implemented")


@pytest.mark.integration
def test_install_packages_skeleton(
    openstack_credentials: OpenStackCredentials,
    temporary_config: Path,
    cleanup: CleanupPlan,
) -> None:
    """Placeholder for the install packages lifecycle step."""
    del openstack_credentials, temporary_config, cleanup
    _stub("end-to-end skeleton only; install workflow not implemented")


@pytest.mark.integration
def test_status_default_skeleton(
    openstack_credentials: OpenStackCredentials,
    temporary_config: Path,
    cleanup: CleanupPlan,
) -> None:
    """Placeholder for the default status lifecycle step."""
    del openstack_credentials, temporary_config, cleanup
    _stub("end-to-end skeleton only; default status workflow not implemented")


@pytest.mark.integration
def test_status_detailed_skeleton(
    openstack_credentials: OpenStackCredentials,
    temporary_config: Path,
    cleanup: CleanupPlan,
) -> None:
    """Placeholder for the detailed status lifecycle step."""
    del openstack_credentials, temporary_config, cleanup
    _stub("end-to-end skeleton only; detailed status workflow not implemented")


@pytest.mark.integration
def test_reboot_workers_skeleton(
    openstack_credentials: OpenStackCredentials,
    temporary_config: Path,
    cleanup: CleanupPlan,
) -> None:
    """Placeholder for the reboot workers lifecycle step."""
    del openstack_credentials, temporary_config, cleanup
    _stub("end-to-end skeleton only; reboot workflow not implemented")


@pytest.mark.integration
def test_destroy_skeleton(
    openstack_credentials: OpenStackCredentials,
    temporary_config: Path,
    cleanup: CleanupPlan,
) -> None:
    """Placeholder for the destroy lifecycle step."""
    del openstack_credentials, temporary_config, cleanup
    _stub("end-to-end skeleton only; destroy workflow not implemented")
