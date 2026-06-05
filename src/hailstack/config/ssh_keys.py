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

"""Discover create-time SSH public keys for manual cluster access."""

from collections.abc import Sequence
from pathlib import Path

from hailstack.errors import ConfigError

DEFAULT_SSH_PUBLIC_KEY_FILENAMES = (
    "id_ed25519.pub",
    "id_rsa.pub",
    "id_ecdsa.pub",
    "id_ecdsa_sk.pub",
    "id_ed25519_sk.pub",
    "id_xmss.pub",
    "id_dsa.pub",
)


def effective_create_public_keys(configured_keys: Sequence[str]) -> list[str]:
    """Return configured keys plus the runner's readable default public keys."""
    if not configured_keys:
        raise ConfigError("ssh_keys.public_keys required")

    default_public_keys = _read_runner_default_public_keys()
    if not default_public_keys:
        raise ConfigError(
            "Could not find a readable default SSH public key for this runner. "
            "Hailstack must append a key from a readable default OpenSSH "
            "public key file such as ~/.ssh/id_ed25519.pub or ~/.ssh/id_rsa.pub "
            "to every node so manual SSH with the runner's default identity "
            "works later. Create or regenerate the matching .pub file for "
            "your default SSH private key. Hailstack does not read private keys."
        )

    return _deduplicate_public_keys([*configured_keys, *default_public_keys])


def _read_runner_default_public_keys() -> list[str]:
    """Read public halves of default OpenSSH identities without private keys."""
    try:
        home = Path.home()
    except RuntimeError:
        return []

    public_keys: list[str] = []
    for filename in DEFAULT_SSH_PUBLIC_KEY_FILENAMES:
        public_key = _read_first_public_key_line(home / ".ssh" / filename)
        if public_key is not None:
            public_keys.append(public_key)
    return public_keys


def _read_first_public_key_line(path: Path) -> str | None:
    """Return the first non-empty public key line from a readable file."""
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):  # fmt: skip
        return None

    for line in content.splitlines():
        public_key = line.strip()
        if public_key:
            return public_key
    return None


def _deduplicate_public_keys(public_keys: Sequence[str]) -> list[str]:
    """Preserve order while dropping duplicate key bodies."""
    seen_identities: set[str] = set()
    deduplicated: list[str] = []
    for public_key in public_keys:
        normalized_public_key = public_key.strip()
        identity = _public_key_identity(normalized_public_key)
        if identity in seen_identities:
            continue
        seen_identities.add(identity)
        deduplicated.append(normalized_public_key)
    return deduplicated


def _public_key_identity(public_key: str) -> str:
    """Return a comment-insensitive identity for a public key line."""
    parts = public_key.split()
    if len(parts) < 2:
        return public_key
    return f"{parts[0]} {parts[1]}"


__all__ = ["DEFAULT_SSH_PUBLIC_KEY_FILENAMES", "effective_create_public_keys"]
