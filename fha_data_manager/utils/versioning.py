"""Version tracking helpers for FHA snapshot datasets."""

from __future__ import annotations

import datetime as _dt
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypedDict

from fha_data_manager.utils.config import DATA_DIR

logger = logging.getLogger(__name__)

SnapshotType = Literal["single_family", "hecm"]

_SCHEMA_VERSION = 1
_SNAPSHOT_PATTERN = re.compile(
    r"fha_(?P<kind>sf|hecm)_snapshot_(?P<date>\d{8})",
    re.IGNORECASE,
)


class _SnapshotComponent(TypedDict, total=False):
    """Metadata tracked for raw or processed snapshot artefacts."""

    path: str
    checksum: str
    timestamp: str


class SnapshotRecord(TypedDict):
    """Persisted manifest entry describing a monthly snapshot."""

    snapshot_type: SnapshotType
    year: int
    month: int
    raw: _SnapshotComponent | None
    processed: _SnapshotComponent | None


@dataclass(slots=True)
class SnapshotStatus:
    """Convenience representation of a manifest entry."""

    snapshot_type: SnapshotType
    year: int
    month: int
    raw: _SnapshotComponent | None
    processed: _SnapshotComponent | None

    @property
    def is_downloaded(self) -> bool:
        return self.raw is not None

    @property
    def is_processed(self) -> bool:
        return self.processed is not None


def _normalize_snapshot_type(value: str | None) -> SnapshotType:
    if value is None:
        raise ValueError("Snapshot type must be provided for version tracking.")

    normalized = value.lower()
    if normalized in {"sf", "single_family"}:
        return "single_family"
    if normalized == "hecm":
        return "hecm"
    raise ValueError(f"Unrecognised snapshot type: {value!r}")


def _compute_checksum(path: Path, *, chunk_size: int = 1 << 20) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_snapshot_filename(path: Path, *, snapshot_type: SnapshotType | None = None) -> tuple[SnapshotType, int, int]:
    name = path.name.lower()
    match = _SNAPSHOT_PATTERN.search(name)

    if match:
        inferred_kind = match.group("kind").lower()
        kind: SnapshotType = "single_family" if inferred_kind == "sf" else "hecm"
        date_str = match.group("date")
        year = int(date_str[:4])
        month = int(date_str[4:6])
    else:
        if snapshot_type is None:
            raise ValueError(
                f"Could not infer snapshot metadata from filename: {path.name!r}"
            )
        kind = snapshot_type
        digits = re.findall(r"(20\d{2})(0[1-9]|1[0-2])", name)
        if not digits:
            raise ValueError(
                "Could not determine year/month from filename and provided snapshot type."
            )
        year = int(digits[0][0])
        month = int(digits[0][1])

    if not 1 <= month <= 12:
        raise ValueError(f"Month {month} extracted from {path.name!r} is invalid")

    if snapshot_type is not None and kind != snapshot_type:
        raise ValueError(
            f"Snapshot type mismatch: inferred {kind!r} but received {snapshot_type!r}"
        )

    return kind, year, month


class SnapshotManifest:
    """Maintain a manifest of downloaded and processed snapshot files."""

    def __init__(self, manifest_path: Path | None = None) -> None:
        self.manifest_path = manifest_path or DATA_DIR / "metadata" / "snapshot_manifest.json"
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self._payload: dict[str, Any] = {"schema_version": _SCHEMA_VERSION, "records": {}}
        self._load()

    def _load(self) -> None:
        if not self.manifest_path.exists():
            return

        try:
            with self.manifest_path.open("r", encoding="utf-8") as stream:
                payload = json.load(stream)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse snapshot manifest %s: %s", self.manifest_path, exc)
            return

        if not isinstance(payload, dict):
            logger.warning("Snapshot manifest %s is not a dictionary payload", self.manifest_path)
            return

        if payload.get("schema_version") != _SCHEMA_VERSION:
            logger.warning(
                "Snapshot manifest schema version %s unsupported; expected %s",
                payload.get("schema_version"),
                _SCHEMA_VERSION,
            )
            return

        records = payload.get("records", {})
        if isinstance(records, dict):
            self._payload = payload
        else:
            logger.warning("Snapshot manifest records field malformed; starting fresh")

    def _save(self) -> None:
        with self.manifest_path.open("w", encoding="utf-8") as stream:
            json.dump(self._payload, stream, indent=2, sort_keys=True)

    @staticmethod
    def _key(snapshot_type: SnapshotType, year: int, month: int) -> str:
        return f"{snapshot_type}:{year:04d}-{month:02d}"

    def _ensure_entry(self, snapshot_type: SnapshotType, year: int, month: int) -> SnapshotRecord:
        records: dict[str, SnapshotRecord] = self._payload.setdefault("records", {})  # type: ignore[assignment]
        key = self._key(snapshot_type, year, month)
        entry = records.get(key)
        if entry is None:
            entry = {
                "snapshot_type": snapshot_type,
                "year": year,
                "month": month,
                "raw": None,
                "processed": None,
            }
            records[key] = entry
        return entry

    def record_download(
        self,
        path: Path,
        *,
        snapshot_type: SnapshotType | str | None = None,
    ) -> SnapshotStatus:
        if snapshot_type is None:
            normalized_type: SnapshotType | None = None
        elif isinstance(snapshot_type, str):
            normalized_type = _normalize_snapshot_type(snapshot_type)
        else:
            normalized_type = snapshot_type

        kind, year, month = _parse_snapshot_filename(path, snapshot_type=normalized_type)

        if not path.exists():
            raise FileNotFoundError(path)

        entry = self._ensure_entry(kind, year, month)
        checksum = _compute_checksum(path)
        timestamp = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
        entry["raw"] = {
            "path": str(path),
            "checksum": checksum,
            "timestamp": timestamp,
        }

        self._save()
        return SnapshotStatus(**entry)

    def record_processing(
        self,
        *,
        raw_path: Path | None,
        processed_path: Path,
        snapshot_type: SnapshotType,
    ) -> SnapshotStatus:
        if not processed_path.exists():
            raise FileNotFoundError(processed_path)

        kind, year, month = _parse_snapshot_filename(processed_path, snapshot_type=snapshot_type)
        entry = self._ensure_entry(kind, year, month)

        if raw_path is not None:
            if raw_path.exists():
                checksum = _compute_checksum(raw_path)
                timestamp = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
                entry["raw"] = {
                    "path": str(raw_path),
                    "checksum": checksum,
                    "timestamp": timestamp,
                }
            else:
                logger.warning("Raw snapshot file %s missing while recording processing", raw_path)

        processed_checksum = _compute_checksum(processed_path)
        processed_timestamp = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
        entry["processed"] = {
            "path": str(processed_path),
            "checksum": processed_checksum,
            "timestamp": processed_timestamp,
        }

        self._save()
        return SnapshotStatus(**entry)

    def get_status(self, snapshot_type: SnapshotType, year: int, month: int) -> SnapshotStatus | None:
        records: dict[str, SnapshotRecord] = self._payload.get("records", {})  # type: ignore[assignment]
        entry = records.get(self._key(snapshot_type, year, month))
        if entry is None:
            return None
        return SnapshotStatus(**entry)

    def list_statuses(self) -> list[SnapshotStatus]:
        records: dict[str, SnapshotRecord] = self._payload.get("records", {})  # type: ignore[assignment]
        return [SnapshotStatus(**entry) for entry in records.values()]

