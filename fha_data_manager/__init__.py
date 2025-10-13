"""Public package interface for FHA Data Manager utilities."""

from .download_cli import (
    HECM_SNAPSHOT_URL,
    SINGLE_FAMILY_SNAPSHOT_URL,
    download_hecm_snapshots,
    download_single_family_snapshots,
    get_argument_parser,
    main,
)

__all__ = [
    "HECM_SNAPSHOT_URL",
    "SINGLE_FAMILY_SNAPSHOT_URL",
    "download_hecm_snapshots",
    "download_single_family_snapshots",
    "get_argument_parser",
    "main",
]
