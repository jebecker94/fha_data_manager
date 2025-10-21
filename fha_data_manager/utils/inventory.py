"""Create an inventory of raw and clean data files.

This script scans the configured data directory for raw and clean data files
and records their metadata in a CSV file. It is useful for keeping an at-a-
glance log of which source files are available and how large they are.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

import config

from fha_data_manager.utils.logging import configure_logging

# Set folder paths
PROJECT_DIR = Path(config.PROJECT_DIR)
DATA_DIR = Path(config.DATA_DIR)
RAW_DIR = Path(config.RAW_DIR)
CLEAN_DIR = Path(config.CLEAN_DIR)


logger = logging.getLogger(__name__)


@dataclass
class FileRecord:
    """Metadata describing a single data file."""

    category: str
    file_name: str
    relative_to_project: str
    relative_to_data: str
    parent_directory: str
    suffix: str
    size_bytes: int
    size_mb: float
    size_readable: str
    modified_utc: str
    created_utc: str

    def to_dict(self) -> dict[str, object]:
        return {
            "category": self.category,
            "file_name": self.file_name,
            "relative_to_project": self.relative_to_project,
            "relative_to_data": self.relative_to_data,
            "parent_directory": self.parent_directory,
            "suffix": self.suffix,
            "size_bytes": self.size_bytes,
            "size_mb": self.size_mb,
            "size_readable": self.size_readable,
            "modified_utc": self.modified_utc,
            "created_utc": self.created_utc,
        }


def discover_data_files(base_dir: Path) -> List[Path]:
    """Return every file located under ``base_dir``."""

    if not base_dir.exists():
        logger.warning("Data directory %s does not exist.", base_dir)
        return []

    return sorted(path for path in base_dir.rglob("*") if path.is_file())


def infer_category(path: Path) -> str:
    """Classify a file as raw, clean, or general data."""

    try:
        path.relative_to(RAW_DIR)
    except ValueError:
        pass
    else:
        return "raw"

    try:
        path.relative_to(CLEAN_DIR)
    except ValueError:
        pass
    else:
        return "clean"

    try:
        path.relative_to(DATA_DIR)
    except ValueError:
        return "outside-data"

    return "data"


def relative_to(path: Path, base: Path) -> str:
    """Return a relative path, falling back to the absolute path."""

    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def human_readable_size(size_bytes: int) -> str:
    """Convert a file size in bytes to a human-friendly string."""

    if size_bytes == 0:
        return "0 B"

    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def format_timestamp(timestamp: float) -> str:
    """Format timestamps as ISO-8601 UTC strings."""

    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def build_records(files: Iterable[Path]) -> list[FileRecord]:
    """Convert paths to :class:`FileRecord` objects."""

    records: list[FileRecord] = []

    for path in files:
        stats = path.stat()
        category = infer_category(path)
        parent = relative_to(path.parent, DATA_DIR)
        record = FileRecord(
            category=category,
            file_name=path.name,
            relative_to_project=relative_to(path, PROJECT_DIR),
            relative_to_data=relative_to(path, DATA_DIR),
            parent_directory=parent,
            suffix=path.suffix.lstrip("."),
            size_bytes=stats.st_size,
            size_mb=round(stats.st_size / (1024 ** 2), 4),
            size_readable=human_readable_size(stats.st_size),
            modified_utc=format_timestamp(stats.st_mtime),
            created_utc=format_timestamp(stats.st_ctime),
        )
        records.append(record)

    return records


def write_inventory(records: list[FileRecord], output_path: Path) -> Path:
    """Write the collected records to ``output_path`` as CSV."""

    columns = [
        "category",
        "file_name",
        "relative_to_project",
        "relative_to_data",
        "parent_directory",
        "suffix",
        "size_bytes",
        "size_mb",
        "size_readable",
        "modified_utc",
        "created_utc",
    ]

    rows = [record.to_dict() for record in records]
    rows.sort(key=lambda row: (row["category"], row["relative_to_data"]))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %d records to %s", len(rows), output_path)
    return output_path


def main(log_level: str | int = "INFO") -> Path:
    configure_logging(log_level)

    files = discover_data_files(DATA_DIR)
    records = build_records(files)

    return write_inventory(records, DATA_DIR / "data_inventory.csv")


if __name__ == "__main__":
    main()
