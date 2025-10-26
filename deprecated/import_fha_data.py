# -*- coding: utf-8 -*-
"""Legacy script for importing/cleaning FHA data - imports from package for backwards compatibility.

This script has been reorganized. All core functionality is now in the
fha_data_manager.import_data module. This file is kept as a thin wrapper to
maintain backwards compatibility with existing workflows.

For new code, prefer:
    from fha_data_manager import convert_fha_sf_snapshots, convert_fha_hecm_snapshots
    # or
    from fha_data_manager import import_single_family_snapshots, import_hecm_snapshots
"""

import logging
from pathlib import Path

from fha_data_manager.utils.config import BRONZE_DIR, DATA_DIR, RAW_DIR, SILVER_DIR
from fha_data_manager.utils.logging import configure_logging

from fha_data_manager.import_data import (
    add_county_fips,
    clean_hecm_sheets,
    convert_fha_hecm_snapshots,
    convert_fha_sf_snapshots,
    create_lender_id_to_name_crosswalk,
    save_clean_snapshots_to_db,
    standardize_county_names,
)

logger = logging.getLogger(__name__)

# Re-export all functions for backwards compatibility
__all__ = [
    "standardize_county_names",
    "add_county_fips",
    "create_lender_id_to_name_crosswalk",
    "clean_hecm_sheets",
    "convert_fha_hecm_snapshots",
    "convert_fha_sf_snapshots",
    "save_clean_snapshots_to_db",
]

# Main Routine
if __name__ == '__main__' :
    configure_logging()

    # Set Folder Paths (using imported constants)
    data_dir = Path(DATA_DIR)
    raw_dir = Path(RAW_DIR)
    bronze_dir = Path(BRONZE_DIR)
    silver_dir = Path(SILVER_DIR)

    # Create Data Folders
    data_dir.mkdir(exist_ok=True)
    raw_dir.mkdir(exist_ok=True)
    bronze_dir.mkdir(exist_ok=True)
    silver_dir.mkdir(exist_ok=True)

    ## Single Family
    # Convert Snapshots
    convert_fha_sf_snapshots(
        data_folder=raw_dir / 'single_family', 
        save_folder=bronze_dir / 'single_family', 
        overwrite=False,
    )

    # Save Clean Snapshots to Database
    save_clean_snapshots_to_db(
        bronze_dir / 'single_family',
        silver_dir / 'single_family',
        min_year=2010,
        max_year=2025,
        file_type='single_family',
        add_fips=True,
        add_date=True,
    )

    ## HECM
    # Convert HECM Snapshots
    convert_fha_hecm_snapshots(
        raw_dir / 'hecm',
        bronze_dir / 'hecm',
        overwrite=False,
    )

    # Save Clean Snapshots to Database
    save_clean_snapshots_to_db(
        bronze_dir / 'hecm', 
        silver_dir / 'hecm', 
        min_year=2010, 
        max_year=2025,
        file_type='hecm',
        add_fips=True,
        add_date=True,
    )
