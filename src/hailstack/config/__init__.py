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

"""Configuration models and parsing helpers."""

from hailstack.config.compatibility import Bundle, CompatibilityMatrix
from hailstack.config.parser import load_config
from hailstack.config.schema import (
    CephS3Config,
    ClusterConfig,
    ClusterSettings,
    DNSConfig,
    ExtrasConfig,
    PackerConfig,
    S3Config,
    SecurityGroupConfig,
    SecurityGroups,
    SSHKeysConfig,
    VolumeConfig,
)
from hailstack.config.validator import (
    BUNDLE_VALIDATION_COMMANDS,
    command_requires_bundle_validation,
    validate_bundle,
    validate_bundle_for_command,
)

__all__ = [
    "Bundle",
    "BUNDLE_VALIDATION_COMMANDS",
    "CephS3Config",
    "ClusterConfig",
    "ClusterSettings",
    "CompatibilityMatrix",
    "DNSConfig",
    "ExtrasConfig",
    "PackerConfig",
    "S3Config",
    "SSHKeysConfig",
    "SecurityGroupConfig",
    "SecurityGroups",
    "VolumeConfig",
    "command_requires_bundle_validation",
    "load_config",
    "validate_bundle",
    "validate_bundle_for_command",
]
