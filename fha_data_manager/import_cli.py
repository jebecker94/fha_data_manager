"""Command-line helpers for importing FHA snapshot datasets."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from fha_data_manager.utils.config import CLEAN_DIR, DATA_DIR, RAW_DIR
from fha_data_manager.import_data import (
    convert_fha_hecm_snapshots,
    convert_fha_sf_snapshots,
    save_clean_snapshots_to_db,
)

DEFAULT_MIN_YEAR = 2010
DEFAULT_MAX_YEAR = 2025


@dataclass(frozen=True)
class _ImportDefaults:
    """Container for defaults shared by snapshot import subcommands."""

    raw_dir: Path
    clean_dir: Path
    database_dir: Path
    file_type: str


_SINGLE_FAMILY_DEFAULTS = _ImportDefaults(
    raw_dir=RAW_DIR / "single_family",
    clean_dir=CLEAN_DIR / "single_family",
    database_dir=DATA_DIR / "database" / "single_family",
    file_type="single_family",
)

_HECM_DEFAULTS = _ImportDefaults(
    raw_dir=RAW_DIR / "hecm",
    clean_dir=CLEAN_DIR / "hecm",
    database_dir=DATA_DIR / "database" / "hecm",
    file_type="hecm",
)


def import_single_family_snapshots(
    raw_dir: Path | str = _SINGLE_FAMILY_DEFAULTS.raw_dir,
    clean_dir: Path | str = _SINGLE_FAMILY_DEFAULTS.clean_dir,
    database_dir: Path | str = _SINGLE_FAMILY_DEFAULTS.database_dir,
    *,
    overwrite: bool = False,
    min_year: int = DEFAULT_MIN_YEAR,
    max_year: int = DEFAULT_MAX_YEAR,
    add_fips: bool = True,
    add_date: bool = True,
) -> None:
    """Import cleaned Single Family snapshots into the hive-structured database."""

    _run_import_pipeline(
        raw_dir=Path(raw_dir),
        clean_dir=Path(clean_dir),
        database_dir=Path(database_dir),
        file_type="single_family",
        overwrite=overwrite,
        min_year=min_year,
        max_year=max_year,
        add_fips=add_fips,
        add_date=add_date,
    )


def import_hecm_snapshots(
    raw_dir: Path | str = _HECM_DEFAULTS.raw_dir,
    clean_dir: Path | str = _HECM_DEFAULTS.clean_dir,
    database_dir: Path | str = _HECM_DEFAULTS.database_dir,
    *,
    overwrite: bool = False,
    min_year: int = DEFAULT_MIN_YEAR,
    max_year: int = DEFAULT_MAX_YEAR,
    add_fips: bool = True,
    add_date: bool = True,
) -> None:
    """Import cleaned HECM snapshots into the hive-structured database."""

    _run_import_pipeline(
        raw_dir=Path(raw_dir),
        clean_dir=Path(clean_dir),
        database_dir=Path(database_dir),
        file_type="hecm",
        overwrite=overwrite,
        min_year=min_year,
        max_year=max_year,
        add_fips=add_fips,
        add_date=add_date,
    )


def _run_import_pipeline(
    *,
    raw_dir: Path,
    clean_dir: Path,
    database_dir: Path,
    file_type: str,
    overwrite: bool,
    min_year: int,
    max_year: int,
    add_fips: bool,
    add_date: bool,
) -> None:
    """Execute the two-step import process shared across snapshot types."""

    raw_dir = raw_dir.expanduser()
    clean_dir = clean_dir.expanduser()
    database_dir = database_dir.expanduser()

    clean_dir.mkdir(parents=True, exist_ok=True)
    database_dir.mkdir(parents=True, exist_ok=True)

    if file_type == "single_family":
        convert_fha_sf_snapshots(raw_dir, clean_dir, overwrite=overwrite)
    else:
        convert_fha_hecm_snapshots(raw_dir, clean_dir, overwrite=overwrite)

    save_clean_snapshots_to_db(
        clean_dir,
        database_dir,
        min_year=min_year,
        max_year=max_year,
        file_type=file_type,
        add_fips=add_fips,
        add_date=add_date,
    )


def _non_negative_int(value: str) -> int:
    """Return ``value`` as a non-negative integer or raise ``argparse`` errors."""

    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")
    return parsed


def _configure_import_subparser(
    subparser: argparse.ArgumentParser,
    *,
    defaults: _ImportDefaults,
    handler: Callable[..., None],
) -> None:
    """Attach shared CLI options to a snapshot import subparser."""

    subparser.set_defaults(handler=handler)
    subparser.add_argument(
        "--raw-dir",
        type=Path,
        default=defaults.raw_dir,
        help=(
            "Directory containing downloaded snapshot workbooks. "
            "Defaults to %(default)s relative to the project root."
        ),
    )
    subparser.add_argument(
        "--clean-dir",
        type=Path,
        default=defaults.clean_dir,
        help=(
            "Directory where cleaned parquet snapshots are written. "
            "Defaults to %(default)s."
        ),
    )
    subparser.add_argument(
        "--database-dir",
        type=Path,
        default=defaults.database_dir,
        help=(
            "Destination directory for the hive-structured database. "
            "Defaults to %(default)s."
        ),
    )
    subparser.add_argument(
        "--min-year",
        type=_non_negative_int,
        default=DEFAULT_MIN_YEAR,
        help="Earliest endorsement year to include when building the database.",
    )
    subparser.add_argument(
        "--max-year",
        type=_non_negative_int,
        default=DEFAULT_MAX_YEAR,
        help="Latest endorsement year to include when building the database.",
    )
    subparser.add_argument(
        "--overwrite",
        action="store_true",
        help="Regenerate parquet files even if they already exist.",
    )
    subparser.add_argument(
        "--no-fips",
        action="store_true",
        help="Skip adding county FIPS codes to the database output.",
    )
    subparser.add_argument(
        "--no-date",
        action="store_true",
        help="Skip adding the derived Date column to the database output.",
    )


def get_argument_parser() -> argparse.ArgumentParser:
    """Construct and return the argument parser for the import CLI."""

    parser = argparse.ArgumentParser(
        description="Convert FHA snapshot workbooks and load them into the database.",
    )
    subparsers = parser.add_subparsers(
        dest="snapshot_type",
        required=True,
        metavar="snapshot",
    )

    sf_parser = subparsers.add_parser(
        "single-family",
        help="Process Single Family snapshot files.",
    )
    _configure_import_subparser(
        sf_parser,
        defaults=_SINGLE_FAMILY_DEFAULTS,
        handler=import_single_family_snapshots,
    )

    hecm_parser = subparsers.add_parser(
        "hecm",
        help="Process HECM snapshot files.",
    )
    _configure_import_subparser(
        hecm_parser,
        defaults=_HECM_DEFAULTS,
        handler=import_hecm_snapshots,
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the import CLI."""

    parser = get_argument_parser()
    args = parser.parse_args(argv)

    min_year = args.min_year
    max_year = args.max_year
    if min_year > max_year:
        parser.error("--min-year cannot be greater than --max-year")

    raw_dir = Path(args.raw_dir)
    clean_dir = Path(args.clean_dir)
    database_dir = Path(args.database_dir)

    args.handler(
        raw_dir=raw_dir,
        clean_dir=clean_dir,
        database_dir=database_dir,
        overwrite=args.overwrite,
        min_year=min_year,
        max_year=max_year,
        add_fips=not args.no_fips,
        add_date=not args.no_date,
    )

    return 0


if __name__ == "__main__":  # pragma: no cover - manual invocation only
    raise SystemExit(main())
