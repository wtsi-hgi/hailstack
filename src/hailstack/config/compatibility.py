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

"""Compatibility matrix parsing and bundle lookup helpers."""

import tomllib
from pathlib import Path
from typing import Literal, Never, cast

from pydantic import BaseModel, ConfigDict
from pydantic import ValidationError as PydanticValidationError

from hailstack.errors import BundleNotFoundError, ConfigError


class Bundle(BaseModel):
    """Represent a single checked-in compatibility bundle."""

    model_config = ConfigDict(extra="forbid", strict=True)

    id: str
    hail: str
    spark: str
    hadoop: str
    java: str
    python: str
    scala: str
    gnomad: str
    status: Literal["latest", "supported", "deprecated"]


class CompatibilityMatrix:
    """Load bundle definitions from a checked-in TOML compatibility matrix."""

    def __init__(self, path: Path) -> None:
        """Parse bundles.toml and index bundle definitions by explicit ID."""
        document = self._load_document(path)
        self._bundles = self._parse_bundles(document)
        self._default_bundle_id = self._parse_default_bundle_id(document)

        if not self._bundles:
            raise ConfigError(f"No bundles defined in compatibility matrix: {path}")

    def get_bundle(self, bundle_id: str) -> Bundle:
        """Return a bundle by ID or raise a bundle-specific validation error."""
        bundle = self._bundles.get(bundle_id)
        if bundle is None:
            available = ", ".join(self._bundles)
            raise BundleNotFoundError(
                f"Bundle '{bundle_id}' not found. Available bundle IDs: {available}"
            )
        return bundle

    def get_default(self) -> Bundle:
        """Return the bundle referenced by the default.bundle entry."""
        return self.get_bundle(self._default_bundle_id)

    def list_bundles(self) -> list[Bundle]:
        """Return all parsed bundles ordered by their explicit IDs."""
        return list(self._bundles.values())

    @staticmethod
    def _load_document(path: Path) -> dict[str, object]:
        """Load and validate the top-level TOML document structure."""
        try:
            with path.open("rb") as handle:
                loaded = cast(object, tomllib.load(handle))
        except FileNotFoundError as error:
            raise ConfigError(f"Compatibility matrix file not found: {path}") from error
        except tomllib.TOMLDecodeError as error:
            line_number = getattr(error, "lineno", "unknown")
            column_number = getattr(error, "colno", "unknown")
            message = getattr(error, "msg", str(error))
            raise ConfigError(
                "Invalid TOML syntax in "
                f"{path}: line {line_number}, column {column_number}: {message}"
            ) from error

        return CompatibilityMatrix._require_table(
            loaded,
            f"Compatibility matrix must contain a TOML table: {path}",
        )

    @staticmethod
    def _parse_bundles(document: dict[str, object]) -> dict[str, Bundle]:
        """Parse bundle entries into ordered Bundle models."""
        raw_bundles = document.get("bundle")
        if raw_bundles is None:
            return {}
        bundle_table = CompatibilityMatrix._require_table(
            raw_bundles,
            "Compatibility matrix bundle section must be a TOML table",
        )

        bundles: dict[str, Bundle] = {}
        for bundle_id in sorted(bundle_table):
            raw_bundle = CompatibilityMatrix._require_table(
                bundle_table[bundle_id],
                "Compatibility matrix bundle entries must be TOML tables",
            )
            bundle_data: dict[str, object] = {"id": bundle_id, **raw_bundle}

            try:
                bundles[bundle_id] = Bundle.model_validate(bundle_data)
            except PydanticValidationError as error:
                CompatibilityMatrix._raise_bundle_error(bundle_id, error)

        return bundles

    @staticmethod
    def _parse_default_bundle_id(document: dict[str, object]) -> str:
        """Extract the default bundle ID from the TOML document."""
        raw_default = CompatibilityMatrix._require_table(
            document.get("default"),
            "Compatibility matrix requires a [default] section",
        )

        raw_bundle_id = raw_default.get("bundle")
        if not isinstance(raw_bundle_id, str) or not raw_bundle_id.strip():
            raise ConfigError("Compatibility matrix requires [default].bundle")

        return raw_bundle_id

    @staticmethod
    def _require_table(value: object, message: str) -> dict[str, object]:
        """Require a TOML table with string keys and return a typed dict."""
        if not isinstance(value, dict):
            raise ConfigError(message)

        raw_table = cast(dict[object, object], value)
        table: dict[str, object] = {}
        for key, entry in raw_table.items():
            if not isinstance(key, str):
                raise ConfigError(message)
            table[key] = entry

        return table

    @staticmethod
    def _raise_bundle_error(bundle_id: str, error: PydanticValidationError) -> Never:
        """Map Pydantic bundle validation failures to ConfigError."""
        first_error = error.errors(include_url=False)[0]
        location = ".".join(str(part) for part in first_error["loc"])
        detail = first_error["msg"]
        raise ConfigError(f"Invalid bundle '{bundle_id}': {location}: {detail}")


__all__ = ["Bundle", "CompatibilityMatrix"]
