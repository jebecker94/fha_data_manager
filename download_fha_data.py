"""Legacy script for downloading FHA data - imports from package for backwards compatibility.

This script has been reorganized. All core functionality is now in the
fha_data_manager.download module. This file is kept as a thin wrapper to
maintain backwards compatibility with existing workflows.

For new code, prefer:
    from fha_data_manager import download_excel_files_from_url
    # or
    from fha_data_manager import download_single_family_snapshots, download_hecm_snapshots
"""

import logging
from pathlib import Path

from fha_data_manager.download import (
    download_excel_files_from_url,
    find_month_in_string,
    find_years_in_string,
    handle_file_dates,
    process_zip_file,
    standardize_filename,
)

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Re-export all functions for backwards compatibility
__all__ = [
    "download_excel_files_from_url",
    "find_years_in_string",
    "find_month_in_string",
    "handle_file_dates",
    "standardize_filename",
    "process_zip_file",
]

# Main Routine
if __name__ == "__main__":

    # Download Single-Samily Data
    target_url = "https://www.hud.gov/stat/sfh/fha-sf-portfolio-snapshot"
    download_to_folder = Path("./data/raw/single_family")
    download_excel_files_from_url(target_url, download_to_folder, include_zip=True, file_type='sf')

    # Download HECM Data
    target_url = "https://www.hud.gov/hud-partners/hecmsf-snapshot"
    download_to_folder = Path("./data/raw/hecm")
    download_excel_files_from_url(target_url, download_to_folder, include_zip=True, file_type='hecm')

    # Download Multifamily Data
    # Note: Not yet implemented
