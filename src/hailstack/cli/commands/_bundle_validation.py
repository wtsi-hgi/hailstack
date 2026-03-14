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

"""CLI helpers for phase-1 command-time bundle validation."""

from pathlib import Path

from hailstack.config.compatibility import Bundle, CompatibilityMatrix
from hailstack.config.parser import load_config
from hailstack.config.validator import validate_bundle_for_command

DEFAULT_COMPATIBILITY_MATRIX_PATH = Path(__file__).resolve().parents[4] / "bundles.toml"


def validate_command_config_bundle(
    command: str,
    config_path: Path,
    dotenv_file: Path | None = None,
) -> Bundle | None:
    """Load config and resolve bundles for commands that validate them."""
    config = load_config(config_path, dotenv_file)
    matrix = CompatibilityMatrix(DEFAULT_COMPATIBILITY_MATRIX_PATH)

    return validate_bundle_for_command(config, matrix, command)


__all__ = [
    "DEFAULT_COMPATIBILITY_MATRIX_PATH",
    "validate_command_config_bundle",
]
