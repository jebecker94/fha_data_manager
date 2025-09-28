"""Save cleaned FHA snapshot files into a partitioned Parquet dataset.

This utility script reads the cleaned monthly snapshot Parquet files that are
produced by :mod:`import_fha_data` and writes them to a local "database"
built on top of a Parquet dataset.  The dataset is partitioned by the snapshot
month (``YYYY-MM``), making it easy to read a specific month or range of
months efficiently with engines such as PyArrow, Polars or DuckDB.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path
from typing import Iterable

import polars as pl
import pyarrow.dataset as ds

import config

logger = logging.getLogger(__name__)

# Column names used by the cleaned monthly snapshots.
YEAR_COLUMN = "Year"
MONTH_COLUMN = "Month"
PARTITION_COLUMN = "snapshot_month"
DATASET_COLUMN = "dataset"


def _clean_directory(path: Path) -> None:
    """Remove an existing directory tree and recreate the directory."""

    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _prepare_table(file_path: Path, dataset_name: str) -> pl.DataFrame:
    """Read a cleaned snapshot and add metadata columns.

    Parameters
    ----------
    file_path:
        Path to the cleaned monthly Parquet file.
    dataset_name:
        Name of the dataset (e.g. ``"single_family"`` or ``"hecm"``).

    Returns
    -------
    pl.DataFrame
        A materialised Polars dataframe containing the snapshot with two
        additional metadata columns:
        ``dataset`` – identifies the product type, and ``snapshot_month`` – the
        month the file represents in ``YYYY-MM`` format.
    """

    lazy_frame = pl.scan_parquet(str(file_path))

    missing_columns = [
        column for column in (YEAR_COLUMN, MONTH_COLUMN) if column not in lazy_frame.columns
    ]
    if missing_columns:
        missing = ", ".join(missing_columns)
        msg = f"Required columns missing from {file_path.name}: {missing}"
        raise ValueError(msg)

    lazy_frame = lazy_frame.filter(
        pl.col(YEAR_COLUMN).is_not_null() & pl.col(MONTH_COLUMN).is_not_null()
    )

    lazy_frame = lazy_frame.with_columns(
        pl.col(YEAR_COLUMN).cast(pl.Int32).alias(YEAR_COLUMN),
        pl.col(MONTH_COLUMN).cast(pl.Int32).alias(MONTH_COLUMN),
    )

    lazy_frame = lazy_frame.with_columns(
        pl.concat_str(
            [
                pl.col(YEAR_COLUMN).cast(pl.Utf8).str.zfill(4),
                pl.col(MONTH_COLUMN).cast(pl.Utf8).str.zfill(2),
            ],
            separator="-",
        ).alias(PARTITION_COLUMN),
        pl.lit(dataset_name).alias(DATASET_COLUMN),
    )

    logger.debug("Loaded %s", file_path.name)
    return lazy_frame.collect()


def _table_generator(files: Iterable[Path], dataset_name: str) -> Iterable:
    """Yield PyArrow tables for each cleaned snapshot file."""

    for file_path in files:
        logger.info("Processing %s snapshot %s", dataset_name, file_path.name)
        dataframe = _prepare_table(file_path, dataset_name)
        yield dataframe.to_arrow()


def build_partitioned_dataset(
    source_directory: Path,
    destination_directory: Path,
    dataset_name: str,
    *,
    overwrite: bool = False,
) -> None:
    """Write cleaned snapshots to a partitioned Parquet dataset.

    Parameters
    ----------
    source_directory:
        Directory containing the cleaned monthly Parquet files.
    destination_directory:
        Base directory for the Parquet dataset.
    dataset_name:
        Name of the dataset; used as a sub-directory inside ``destination`` and
        as the value for the ``dataset`` metadata column.
    overwrite:
        When ``True`` the destination directory is cleared before writing the
        dataset.  Defaults to ``False`` to protect against accidental data
        loss.
    """

    dataset_source = source_directory / dataset_name
    if not dataset_source.exists():
        logger.warning("Skipping %s – source directory does not exist", dataset_name)
        return

    parquet_files = sorted(dataset_source.glob("*.parquet"))
    if not parquet_files:
        logger.warning("Skipping %s – no cleaned parquet files found", dataset_name)
        return

    dataset_destination = destination_directory / dataset_name
    if overwrite:
        _clean_directory(dataset_destination)
    else:
        dataset_destination.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Writing %s cleaned snapshots to %s", dataset_name, dataset_destination
    )

    tables = _table_generator(parquet_files, dataset_name)
    for table in tables:
        schema = table.schema
        print(schema)
        break

    ds.write_dataset(
        data=tables,
        schema=schema,
        base_dir=str(dataset_destination),
        format="parquet",
        partitioning=ds.partitioning(schema=schema),
        existing_data_behavior="overwrite_or_ignore",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for the script."""

    parser = argparse.ArgumentParser(
        description=(
            "Save cleaned FHA snapshot files to a partitioned Parquet dataset "
            "stored on the local filesystem."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory for the Parquet dataset. Defaults to <DATA_DIR>/database.",
    )
    parser.add_argument(
        "--dataset",
        choices=("single_family", "hecm", "all"),
        default="all",
        help="Limit the run to a single dataset type. Defaults to processing both.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove existing Parquet dataset directories before writing.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Configure logging level (e.g. DEBUG, INFO, WARNING).",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""

    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    clean_dir = Path(config.CLEAN_DIR)
    if args.output_dir is not None:
        output_dir = args.output_dir
    else:
        output_dir = Path(config.DATA_DIR) / "database"
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset_names = ["single_family", "hecm"]
    if args.dataset != "all":
        dataset_names = [args.dataset]

    for dataset_name in dataset_names:
        build_partitioned_dataset(
            clean_dir,
            output_dir,
            dataset_name,
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()
