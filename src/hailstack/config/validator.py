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

"""Command-time bundle validation helpers."""

from hailstack.config.compatibility import Bundle, CompatibilityMatrix
from hailstack.config.schema import ClusterConfig

BUNDLE_VALIDATION_COMMANDS = frozenset({"build-image", "create"})


def command_requires_bundle_validation(command: str) -> bool:
    """Report whether the given command must resolve the configured bundle."""
    return command in BUNDLE_VALIDATION_COMMANDS


def validate_bundle(config: ClusterConfig, matrix: CompatibilityMatrix) -> Bundle:
    """Return the configured bundle, or the matrix default when unset."""
    bundle_id = config.cluster.bundle.strip()
    if bundle_id:
        return matrix.get_bundle(bundle_id)

    return matrix.get_default()


def validate_bundle_for_command(
    config: ClusterConfig,
    matrix: CompatibilityMatrix,
    command: str,
) -> Bundle | None:
    """Validate bundles only for commands that provision or build images."""
    if not command_requires_bundle_validation(command):
        return None

    return validate_bundle(config, matrix)


__all__ = [
    "BUNDLE_VALIDATION_COMMANDS",
    "command_requires_bundle_validation",
    "validate_bundle",
    "validate_bundle_for_command",
]
