"""
Example 2a: Import and Clean FHA Data Using Polars

This script demonstrates how to import and clean FHA snapshots using the
Polars-based functions, converting them to a standardized parquet format in
a hive-structured database.

This version uses Polars expressions for data transformations instead of
pandas operations.
"""

from pathlib import Path

from fha_data_manager.import_data import (
    convert_fha_hecm_snapshots_polars,
    save_clean_snapshots_to_db,
)
from fha_data_manager.utils.config import BRONZE_DIR, RAW_DIR, SILVER_DIR
from fha_data_manager.utils.logging import configure_logging


def main() -> None:
    """Run the HECM import pipeline with Polars."""
    
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    
    # Configure logging
    configure_logging()
    
    # Set up directories
    raw_dir = RAW_DIR
    bronze_dir = BRONZE_DIR
    silver_dir = SILVER_DIR
    
    # Create directories if they don't exist
    raw_dir.mkdir(exist_ok=True)
    bronze_dir.mkdir(exist_ok=True)
    silver_dir.mkdir(exist_ok=True)
    
    print("=" * 80, flush=True)
    print("IMPORTING AND CLEANING FHA DATA USING POLARS", flush=True)
    print("=" * 80, flush=True)
    
    # HECM
    print("\n3. Converting HECM snapshots to parquet (using Polars)...")
    hecm_raw = raw_dir / 'hecm'
    hecm_bronze = bronze_dir / 'hecm_polars'
    
    print(f"   Raw directory: {hecm_raw}")
    print(f"   Bronze directory: {hecm_bronze}")
    
    convert_fha_hecm_snapshots_polars(
        data_folder=hecm_raw,
        save_folder=hecm_bronze,
        overwrite=False,
    )
    
    print("   ✓ HECM conversion complete")
    
    # Save to database
    print("\n4. Saving HECM to hive-structured database...")
    hecm_silver = silver_dir / 'hecm_polars'
    
    save_clean_snapshots_to_db(
        data_folder=hecm_bronze,
        save_folder=hecm_silver,
        min_year=2010,
        max_year=2025,
        file_type='hecm',
        add_fips=True,
        add_date=True,
    )
    
    print("   ✓ HECM database complete")


if __name__ == "__main__":
    main()

