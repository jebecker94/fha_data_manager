"""Tests for the snapshot versioning manifest."""

from __future__ import annotations

import hashlib
from pathlib import Path

from fha_data_manager.utils.versioning import SnapshotManifest


def _write_sample(path: Path, content: bytes) -> str:
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def test_record_download_creates_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = SnapshotManifest(manifest_path=manifest_path)

    sample_file = tmp_path / "fha_sf_snapshot_20240101.xlsx"
    expected_checksum = _write_sample(sample_file, b"sample-data")

    status = manifest.record_download(sample_file, snapshot_type="sf")

    assert manifest_path.exists()
    assert status.is_downloaded is True
    assert status.raw is not None
    assert status.raw["path"] == str(sample_file)
    assert status.raw["checksum"] == expected_checksum


def test_record_processing_updates_processed_metadata(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = SnapshotManifest(manifest_path=manifest_path)

    raw_file = tmp_path / "fha_hecm_snapshot_20240301.xlsx"
    processed_file = tmp_path / "fha_hecm_snapshot_20240301.parquet"

    raw_checksum = _write_sample(raw_file, b"raw-contents")
    processed_checksum = _write_sample(processed_file, b"processed-contents")

    status = manifest.record_processing(
        raw_path=raw_file,
        processed_path=processed_file,
        snapshot_type="hecm",
    )

    assert status.is_processed is True
    assert status.processed is not None
    assert status.processed["path"] == str(processed_file)
    assert status.processed["checksum"] == processed_checksum
    assert status.raw is not None
    assert status.raw["checksum"] == raw_checksum


def test_record_processing_without_raw(tmp_path: Path) -> None:
    manifest = SnapshotManifest(manifest_path=tmp_path / "manifest.json")
    processed_file = tmp_path / "fha_sf_snapshot_20240501.parquet"
    _write_sample(processed_file, b"only-processed")

    status = manifest.record_processing(
        raw_path=None,
        processed_path=processed_file,
        snapshot_type="single_family",
    )

    assert status.is_processed is True
    assert status.raw is None
    assert status.processed is not None
