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

"""Project exception hierarchy."""


class HailstackError(Exception):
    """Provide a common base exception for hailstack."""


class ConfigError(HailstackError):
    """Raise when configuration input cannot be loaded."""


class ValidationError(HailstackError):
    """Raise when configuration values fail validation."""


class BundleNotFoundError(ValidationError):
    """Raise when a requested bundle ID is not present in the matrix."""


class NetworkError(HailstackError):
    """Raise when external service access fails unexpectedly."""


class PackerError(HailstackError):
    """Raise when a Packer build fails or returns invalid output."""


class PulumiError(HailstackError):
    """Raise when Pulumi resource creation cannot be completed."""


class AnsibleError(HailstackError):
    """Raise when an Ansible operation fails."""


class S3Error(HailstackError):
    """Raise when Ceph S3 state-backend access fails."""


class SSHError(HailstackError):
    """Raise when SSH connectivity or commands fail."""


class ImageNotFoundError(HailstackError):
    """Raise when a required OpenStack image cannot be found."""


class ResourceNotFoundError(HailstackError):
    """Raise when one or more requested OpenStack resources are unavailable."""


class QuotaExceededError(HailstackError):
    """Raise when requested resources exceed available project quota."""


__all__ = [
    "AnsibleError",
    "BundleNotFoundError",
    "ConfigError",
    "HailstackError",
    "ImageNotFoundError",
    "NetworkError",
    "PackerError",
    "PulumiError",
    "QuotaExceededError",
    "ResourceNotFoundError",
    "S3Error",
    "SSHError",
    "ValidationError",
]
