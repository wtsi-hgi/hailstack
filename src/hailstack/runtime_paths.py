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

"""Resolve runtime asset paths for editable checkouts and installed wheels."""

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
CHECKOUT_ROOT = PACKAGE_ROOT.parents[1]
PACKAGED_DATA_ROOT = PACKAGE_ROOT / "_data"


def _default_runtime_work_dir() -> Path:
    """Return the default runtime work directory path without creating it."""
    if (CHECKOUT_ROOT / ".git").exists():
        return CHECKOUT_ROOT
    return Path.home() / ".hailstack" / "workspace"


def runtime_asset_path(*parts: str) -> Path:
    """Return a runtime asset path from packaged data or the source checkout."""
    packaged_path = PACKAGED_DATA_ROOT.joinpath(*parts)
    if packaged_path.exists():
        return packaged_path

    return CHECKOUT_ROOT.joinpath(*parts)


def runtime_work_dir() -> Path:
    """Return a writable working directory for Pulumi and CLI subprocesses."""
    work_dir = _default_runtime_work_dir()
    if work_dir == CHECKOUT_ROOT:
        return work_dir
    work_dir.mkdir(parents=True, exist_ok=True)

    return work_dir


BUNDLES_TOML_PATH = runtime_asset_path("bundles.toml")
ANSIBLE_ROOT = runtime_asset_path("ansible")
INSTALL_PLAYBOOK_PATH = ANSIBLE_ROOT / "install.yml"
PACKER_ROOT = runtime_asset_path("packer")
PACKER_TEMPLATE_PATH = PACKER_ROOT / "hailstack.pkr.hcl"
PACKER_SCRIPTS_PATH = PACKER_ROOT / "scripts"
RUNTIME_WORK_DIR = _default_runtime_work_dir()


__all__ = [
    "ANSIBLE_ROOT",
    "BUNDLES_TOML_PATH",
    "CHECKOUT_ROOT",
    "INSTALL_PLAYBOOK_PATH",
    "PACKAGE_ROOT",
    "PACKAGED_DATA_ROOT",
    "PACKER_ROOT",
    "PACKER_SCRIPTS_PATH",
    "PACKER_TEMPLATE_PATH",
    "RUNTIME_WORK_DIR",
    "runtime_asset_path",
    "runtime_work_dir",
]
