"""Public package interface for FHA Data Manager utilities."""

from .download import (
    download_excel_files_from_url,
    find_month_in_string,
    find_years_in_string,
    handle_file_dates,
    process_zip_file,
    standardize_filename,
)
from .download_cli import (
    HECM_SNAPSHOT_URL,
    SINGLE_FAMILY_SNAPSHOT_URL,
    download_hecm_snapshots,
    download_single_family_snapshots,
)
from .import_cli import (
    import_hecm_snapshots,
    import_single_family_snapshots,
)
from .import_data import (
    add_county_fips,
    clean_hecm_sheets,
    convert_fha_hecm_snapshots,
    convert_fha_sf_snapshots,
    create_lender_id_to_name_crosswalk,
    save_clean_snapshots_to_db,
    standardize_county_names,
)

__all__ = [
    # Download functionality
    "download_excel_files_from_url",
    "find_month_in_string",
    "find_years_in_string",
    "handle_file_dates",
    "process_zip_file",
    "standardize_filename",
    # Download CLI
    "HECM_SNAPSHOT_URL",
    "SINGLE_FAMILY_SNAPSHOT_URL",
    "download_hecm_snapshots",
    "download_single_family_snapshots",
    # Import CLI
    "import_hecm_snapshots",
    "import_single_family_snapshots",
    # Import/cleaning functionality
    "add_county_fips",
    "clean_hecm_sheets",
    "convert_fha_hecm_snapshots",
    "convert_fha_sf_snapshots",
    "create_lender_id_to_name_crosswalk",
    "save_clean_snapshots_to_db",
    "standardize_county_names",
]
