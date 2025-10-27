"""
Example 2: Import and Clean FHA Data

This script demonstrates how to import and clean FHA snapshots,
converting them to a standardized parquet format in a hive-structured database.
"""

from fha_data_manager.import_cli import import_hecm_snapshots, import_single_family_snapshots


def main() -> None:
    """Run the Single Family and HECM import pipelines with default settings."""
    print("=" * 80)
    print("IMPORTING AND CLEANING FHA DATA")
    print("=" * 80)
    
    print("\n1. Importing Single Family snapshots...")
    import_single_family_snapshots()

    print("\n2. Importing HECM snapshots...")
    import_hecm_snapshots()

    print("\n" + "=" * 80)
    print("Import complete!")
    print("=" * 80)
    print("\nData has been saved to:")
    print("  - data/silver/single_family/")
    print("  - data/silver/hecm/")
    print("\nNext step: Run 03_load_and_query.py to explore the data")


if __name__ == "__main__":
    main()

