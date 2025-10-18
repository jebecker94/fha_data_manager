"""
Example 1: Download FHA Data Snapshots

This script demonstrates how to download both Single Family and HECM 
snapshots from the FHA website using the default configuration.
"""

from fha_data_manager import (
    download_hecm_snapshots,
    download_single_family_snapshots,
)


def main() -> None:
    """Download Single Family and HECM snapshots with project defaults."""
    print("=" * 80)
    print("DOWNLOADING FHA DATA SNAPSHOTS")
    print("=" * 80)
    
    print("\n1. Downloading Single Family snapshots...")
    download_single_family_snapshots()
    
    print("\n2. Downloading HECM snapshots...")
    download_hecm_snapshots()
    
    print("\n" + "=" * 80)
    print("Download complete!")
    print("=" * 80)
    print("\nNext step: Run 02_import_and_clean.py to process the data")


if __name__ == "__main__":
    main()

