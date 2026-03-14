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

"""Persist rollout manifests and node results in Ceph S3."""

import hashlib
import hmac
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from pydantic import BaseModel, ConfigDict, Field

from hailstack.config.schema import CephS3Config
from hailstack.errors import S3Error


def _empty_str_list() -> list[str]:
    """Return a typed empty string list for pydantic defaults."""
    return []


class NodeResult(BaseModel):
    """Represent a per-node rollout record persisted to S3."""

    model_config = ConfigDict(extra="forbid", strict=True)

    hostname: str
    success: bool
    system_installed: list[str] = Field(default_factory=_empty_str_list)
    python_installed: list[str] = Field(default_factory=_empty_str_list)
    errors: list[str] = Field(default_factory=_empty_str_list)


class RolloutManifest(BaseModel):
    """Represent rollout metadata written alongside node result objects."""

    model_config = ConfigDict(extra="forbid", strict=True)

    cluster_name: str
    timestamp: str
    system_packages: list[str]
    python_packages: list[str]
    node_count: int
    success_count: int
    failure_count: int
    sha256: str


class S3ObjectUploader(Protocol):
    """Describe the object upload primitive used by rollout storage."""

    def put_object(self, *, key: str, body: bytes, content_type: str) -> None:
        """Upload one object to the configured bucket."""
        ...


class CephS3Uploader:
    """Upload rollout artifacts using SigV4-signed HTTP PUT requests."""

    def __init__(self, config: CephS3Config) -> None:
        """Initialise the uploader from the configured Ceph S3 settings."""
        self._config = config

    def put_object(self, *, key: str, body: bytes, content_type: str) -> None:
        """Upload one object to Ceph S3."""
        parsed_endpoint = urlsplit(self._config.endpoint.rstrip("/"))
        if not parsed_endpoint.scheme or not parsed_endpoint.netloc:
            raise S3Error(f"Invalid Ceph S3 endpoint: {self._config.endpoint}")

        quoted_key = quote(key, safe="/")
        object_path = f"/{self._config.bucket}/{quoted_key}"
        url = f"{parsed_endpoint.scheme}://{parsed_endpoint.netloc}{object_path}"
        request_time = datetime.now(UTC)
        payload_hash = hashlib.sha256(body).hexdigest()
        amz_date = request_time.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = request_time.strftime("%Y%m%d")
        canonical_headers = (
            f"content-type:{content_type}\n"
            f"host:{parsed_endpoint.netloc}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{amz_date}\n"
        )
        signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
        canonical_request = "\n".join(
            [
                "PUT",
                object_path,
                "",
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/us-east-1/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signing_key = _signing_key(self._config.secret_key, date_stamp)
        signature = hmac.new(
            signing_key,
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        authorization = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self._config.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        request = Request(
            url,
            data=body,
            method="PUT",
            headers={
                "Authorization": authorization,
                "Content-Type": content_type,
                "Host": parsed_endpoint.netloc,
                "X-Amz-Content-SHA256": payload_hash,
                "X-Amz-Date": amz_date,
            },
        )
        try:
            with urlopen(request):
                return
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip()
            raise S3Error(
                "Unable to upload rollout object to "
                f"{self._config.endpoint}: {detail or error.reason}"
            ) from error
        except URLError as error:
            raise S3Error(
                "Unable to upload rollout object to "
                f"{self._config.endpoint}: {error.reason}"
            ) from error


def upload_rollout(
    manifest: RolloutManifest,
    node_results: Sequence[NodeResult],
    ceph_s3_config: CephS3Config,
    cluster_name: str,
) -> str:
    """Upload rollout manifest and per-node results to Ceph S3."""
    uploader = create_rollout_uploader(ceph_s3_config)
    resolved_manifest = _with_manifest_hash(manifest)
    rollout_prefix = (
        f"hailstack/{cluster_name}/rollouts/"
        f"{_timestamp_key(resolved_manifest.timestamp)}"
    )
    manifest_key = f"{rollout_prefix}/manifest.json"
    manifest_body = _json_bytes(resolved_manifest.model_dump(mode="json"))
    uploader.put_object(
        key=manifest_key,
        body=manifest_body,
        content_type="application/json",
    )
    for node_result in node_results:
        node_key = f"{rollout_prefix}/nodes/{node_result.hostname}.json"
        uploader.put_object(
            key=node_key,
            body=_json_bytes(node_result.model_dump(mode="json")),
            content_type="application/json",
        )
    return f"s3://{ceph_s3_config.bucket}/{manifest_key}"


def create_rollout_uploader(config: CephS3Config) -> S3ObjectUploader:
    """Create the default Ceph S3 uploader for rollout artifacts."""
    return CephS3Uploader(config)


def _with_manifest_hash(manifest: RolloutManifest) -> RolloutManifest:
    """Populate the manifest hash from the content excluding the hash field."""
    payload = manifest.model_dump(mode="json")
    payload["sha256"] = ""
    digest = hashlib.sha256(_json_bytes(payload)).hexdigest()
    return manifest.model_copy(update={"sha256": digest})


def _json_bytes(payload: object) -> bytes:
    """Serialize JSON payloads in a stable format for hashing and upload."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _timestamp_key(timestamp: str) -> str:
    """Convert an ISO-8601 timestamp into the rollout S3 key segment."""
    return (
        timestamp.replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
        .replace(".", "")
    )


def _signing_key(secret_key: str, date_stamp: str) -> bytes:
    """Create the SigV4 signing key for the current request."""
    date_key = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    region_key = _hmac_sha256(date_key, "us-east-1")
    service_key = _hmac_sha256(region_key, "s3")
    return _hmac_sha256(service_key, "aws4_request")


def _hmac_sha256(key: bytes, value: str) -> bytes:
    """Return one SigV4 HMAC step."""
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()


__all__ = ["NodeResult", "RolloutManifest", "upload_rollout"]
