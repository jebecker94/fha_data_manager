"""Command-line helpers for downloading FHA snapshot datasets."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, Sequence

from fha_data_manager.download import download_excel_files_from_url

SINGLE_FAMILY_SNAPSHOT_URL = "https://www.hud.gov/stat/sfh/fha-sf-portfolio-snapshot"
HECM_SNAPSHOT_URL = "https://www.hud.gov/hud-partners/hecmsf-snapshot"

DEFAULT_SINGLE_FAMILY_DESTINATION = Path("data/raw/single_family")
DEFAULT_HECM_DESTINATION = Path("data/raw/hecm")
DEFAULT_PAUSE_LENGTH = 5


def download_single_family_snapshots(
    destination: Path | str = DEFAULT_SINGLE_FAMILY_DESTINATION,
    *,
    pause_length: int = DEFAULT_PAUSE_LENGTH,
    include_zip: bool = True,
    url: str = SINGLE_FAMILY_SNAPSHOT_URL,
) -> None:
    """Download the latest Single Family snapshot files.

    Parameters mirror the existing :func:`download_fha_data.download_excel_files_from_url`
    helper so the function can be used programmatically or via the CLI.
    """

    download_excel_files_from_url(
        url,
        destination,
        pause_length=pause_length,
        include_zip=include_zip,
        file_type="sf",
    )


def download_hecm_snapshots(
    destination: Path | str = DEFAULT_HECM_DESTINATION,
    *,
    pause_length: int = DEFAULT_PAUSE_LENGTH,
    include_zip: bool = True,
    url: str = HECM_SNAPSHOT_URL,
) -> None:
    """Download the latest HECM snapshot files."""

    download_excel_files_from_url(
        url,
        destination,
        pause_length=pause_length,
        include_zip=include_zip,
        file_type="hecm",
    )


def _non_negative_int(value: str) -> int:
    """Return ``value`` as an integer and ensure it is not negative."""

    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("pause-length must be zero or greater")
    return parsed


def _configure_snapshot_subparser(
    subparser: argparse.ArgumentParser,
    *,
    default_destination: Path,
    default_url: str,
    handler: Callable[..., None],
) -> None:
    """Attach shared CLI options to a snapshot download subparser."""

    subparser.set_defaults(handler=handler)
    subparser.add_argument(
        "--destination",
        type=Path,
        default=default_destination,
        help=(
            "Directory where downloaded files should be stored. "
            "Defaults to %(default)s relative to the project root."
        ),
    )
    subparser.add_argument(
        "--pause-length",
        type=_non_negative_int,
        default=DEFAULT_PAUSE_LENGTH,
        help="Seconds to pause between downloads (default: %(default)s)",
    )
    subparser.add_argument(
        "--no-zip",
        action="store_true",
        help="Skip downloading .zip archives linked on the HUD snapshot page.",
    )
    subparser.add_argument(
        "--url",
        default=default_url,
        help="Override the source URL for the snapshot page.",
    )


def get_argument_parser() -> argparse.ArgumentParser:
    """Construct and return the argument parser for the download CLI."""

    parser = argparse.ArgumentParser(
        description="Download FHA Single Family or HECM snapshot files.",
    )
    subparsers = parser.add_subparsers(
        dest="snapshot_type",
        required=True,
        metavar="snapshot",
    )

    sf_parser = subparsers.add_parser(
        "single-family",
        help="Download Single Family snapshot files.",
    )
    _configure_snapshot_subparser(
        sf_parser,
        default_destination=DEFAULT_SINGLE_FAMILY_DESTINATION,
        default_url=SINGLE_FAMILY_SNAPSHOT_URL,
        handler=download_single_family_snapshots,
    )

    hecm_parser = subparsers.add_parser(
        "hecm",
        help="Download HECM snapshot files.",
    )
    _configure_snapshot_subparser(
        hecm_parser,
        default_destination=DEFAULT_HECM_DESTINATION,
        default_url=HECM_SNAPSHOT_URL,
        handler=download_hecm_snapshots,
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the download CLI."""

    parser = get_argument_parser()
    args = parser.parse_args(argv)

    destination = Path(args.destination).expanduser()
    include_zip = not args.no_zip
    pause_length = args.pause_length
    url = args.url

    args.handler(
        destination=destination,
        pause_length=pause_length,
        include_zip=include_zip,
        url=url,
    )

    return 0


if __name__ == "__main__":  # pragma: no cover - manual invocation only
    raise SystemExit(main())
