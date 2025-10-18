"""Download both FHA snapshot datasets using the default configuration."""

from fha_data_manager import (
    download_hecm_snapshots,
    download_single_family_snapshots,
)


def main() -> None:
    """Download Single Family and HECM snapshots with project defaults."""

    download_single_family_snapshots()
    download_hecm_snapshots()


if __name__ == "__main__":
    main()
