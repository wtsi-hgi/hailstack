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

"""Unit tests for rollout manifest storage."""

import hashlib
import json
from urllib.request import Request

from hailstack.config.schema import CephS3Config
from hailstack.storage import rollout as rollout_module


class RecordingUploader:
    """Capture uploaded rollout objects in memory."""

    def __init__(self) -> None:
        """Initialise object storage state."""
        self.objects: dict[str, bytes] = {}
        self.keys: list[str] = []

    def put_object(self, *, key: str, body: bytes, content_type: str) -> None:
        """Store uploaded objects for later assertions."""
        assert content_type == "application/json"
        self.keys.append(key)
        self.objects[key] = body


def test_upload_rollout_uploads_manifest_and_node_results(monkeypatch) -> None:
    """Upload one manifest plus one object per node result."""
    uploader = RecordingUploader()
    monkeypatch.setattr(
        rollout_module,
        "create_rollout_uploader",
        lambda config: uploader,
    )
    node_results = [
        rollout_module.NodeResult(
            hostname="test-cluster-master",
            success=True,
            system_installed=["mc"],
            python_installed=["pandas"],
            errors=[],
        ),
        rollout_module.NodeResult(
            hostname="test-cluster-worker-01",
            success=False,
            system_installed=["mc"],
            python_installed=["pandas"],
            errors=["host unreachable"],
        ),
        rollout_module.NodeResult(
            hostname="test-cluster-worker-02",
            success=True,
            system_installed=["mc"],
            python_installed=["pandas"],
            errors=[],
        ),
    ]
    manifest = rollout_module.RolloutManifest(
        cluster_name="test-cluster",
        timestamp="2026-03-13T12:00:00Z",
        system_packages=["mc"],
        python_packages=["pandas"],
        node_count=3,
        success_count=2,
        failure_count=1,
        sha256="",
    )

    path = rollout_module.upload_rollout(
        manifest=manifest,
        node_results=node_results,
        ceph_s3_config=CephS3Config(
            endpoint="https://ceph.example.invalid",
            bucket="hailstack-state",
            access_key="access",
            secret_key="secret",
        ),
        cluster_name="test-cluster",
    )

    assert path == (
        "s3://hailstack-state/"
        "hailstack/test-cluster/rollouts/20260313T120000Z/manifest.json"
    )
    assert len(uploader.objects) == 4
    uploaded_nodes = {
        key: json.loads(body.decode("utf-8"))
        for key, body in uploader.objects.items()
        if "/nodes/" in key
    }
    assert set(uploaded_nodes) == {
        "hailstack/test-cluster/rollouts/20260313T120000Z/nodes/test-cluster-master.json",
        "hailstack/test-cluster/rollouts/20260313T120000Z/nodes/test-cluster-worker-01.json",
        "hailstack/test-cluster/rollouts/20260313T120000Z/nodes/test-cluster-worker-02.json",
    }
    assert all(
        set(node_payload)
        == {
            "hostname",
            "success",
            "system_installed",
            "python_installed",
            "errors",
        }
        for node_payload in uploaded_nodes.values()
    )


def test_upload_rollout_populates_sha256_from_manifest_content(monkeypatch) -> None:
    """Hash the manifest content with sha256 excluded from the digest input."""
    uploader = RecordingUploader()
    monkeypatch.setattr(
        rollout_module, "create_rollout_uploader", lambda config: uploader
    )
    node_results = [
        rollout_module.NodeResult(
            hostname="test-cluster-master",
            success=True,
            system_installed=["mc"],
            python_installed=[],
            errors=[],
        )
    ]
    manifest = rollout_module.RolloutManifest(
        cluster_name="test-cluster",
        timestamp="2026-03-13T12:00:00Z",
        system_packages=["mc"],
        python_packages=[],
        node_count=1,
        success_count=1,
        failure_count=0,
        sha256="",
    )

    rollout_module.upload_rollout(
        manifest=manifest,
        node_results=node_results,
        ceph_s3_config=CephS3Config(
            endpoint="https://ceph.example.invalid",
            bucket="hailstack-state",
            access_key="access",
            secret_key="secret",
        ),
        cluster_name="test-cluster",
    )

    manifest_key = next(
        key for key in uploader.objects if key.endswith("/manifest.json")
    )
    uploaded_manifest = json.loads(uploader.objects[manifest_key].decode("utf-8"))
    assert set(uploaded_manifest) == {
        "cluster_name",
        "timestamp",
        "system_packages",
        "python_packages",
        "sha256",
        "node_count",
        "success_count",
        "failure_count",
    }
    hashed_payload = dict(uploaded_manifest)
    hashed_payload["sha256"] = ""
    expected_hash = hashlib.sha256(
        json.dumps(hashed_payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    assert uploaded_manifest["sha256"] == expected_hash


def test_upload_rollout_uses_documented_object_keys(monkeypatch) -> None:
    """Write the manifest and node results under the documented rollout prefix."""
    uploader = RecordingUploader()
    monkeypatch.setattr(
        rollout_module, "create_rollout_uploader", lambda config: uploader
    )
    node_results = [
        rollout_module.NodeResult(
            hostname="test-cluster-master",
            success=True,
            system_installed=["mc"],
            python_installed=[],
            errors=[],
        )
    ]
    manifest = rollout_module.RolloutManifest(
        cluster_name="test-cluster",
        timestamp="2026-03-13T12:00:00Z",
        system_packages=["mc"],
        python_packages=[],
        node_count=1,
        success_count=1,
        failure_count=0,
        sha256="",
    )

    rollout_module.upload_rollout(
        manifest=manifest,
        node_results=node_results,
        ceph_s3_config=CephS3Config(
            endpoint="https://ceph.example.invalid",
            bucket="hailstack-state",
            access_key="access",
            secret_key="secret",
        ),
        cluster_name="test-cluster",
    )

    assert sorted(uploader.objects) == [
        "hailstack/test-cluster/rollouts/20260313T120000Z/manifest.json",
        "hailstack/test-cluster/rollouts/20260313T120000Z/nodes/test-cluster-master.json",
    ]


def test_upload_rollout_publishes_manifest_after_node_results(monkeypatch) -> None:
    """Upload per-node results first so manifests cannot point at missing nodes."""
    uploader = RecordingUploader()
    monkeypatch.setattr(
        rollout_module, "create_rollout_uploader", lambda config: uploader
    )
    rollout_module.upload_rollout(
        manifest=rollout_module.RolloutManifest(
            cluster_name="test-cluster",
            timestamp="2026-03-13T12:00:00Z",
            system_packages=["mc"],
            python_packages=[],
            node_count=1,
            success_count=1,
            failure_count=0,
            sha256="",
        ),
        node_results=[
            rollout_module.NodeResult(
                hostname="test-cluster-master",
                success=True,
                system_installed=["mc"],
                python_installed=[],
                errors=[],
            )
        ],
        ceph_s3_config=CephS3Config(
            endpoint="https://ceph.example.invalid",
            bucket="hailstack-state",
            access_key="access",
            secret_key="secret",
        ),
        cluster_name="test-cluster",
    )

    assert uploader.keys == [
        "hailstack/test-cluster/rollouts/20260313T120000Z/nodes/test-cluster-master.json",
        "hailstack/test-cluster/rollouts/20260313T120000Z/manifest.json",
    ]


def test_ceph_s3_uploader_defaults_bare_hostnames_to_https(
    monkeypatch,
) -> None:
    """Treat documented bare Ceph endpoints as HTTPS URLs."""
    recorded: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    def fake_urlopen(request: Request):
        recorded["url"] = request.full_url
        return FakeResponse()

    monkeypatch.setattr(rollout_module, "urlopen", fake_urlopen)

    uploader = rollout_module.CephS3Uploader(
        CephS3Config(
            endpoint="ceph.example.invalid",
            bucket="hailstack-state",
            access_key="access",
            secret_key="secret",
        )
    )

    uploader.put_object(
        key="hailstack/test-cluster/rollouts/latest/manifest.json",
        body=b"{}",
        content_type="application/json",
    )

    assert recorded["url"] == (
        "https://ceph.example.invalid/"
        "hailstack-state/hailstack/test-cluster/rollouts/latest/manifest.json"
    )
