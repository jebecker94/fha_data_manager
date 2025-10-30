"""Core download functionality for FHA snapshot datasets."""

from __future__ import annotations

import logging
import re
import tempfile
import time
import zipfile
from pathlib import Path
from typing import TypeAlias
from urllib.parse import urljoin, urlparse

import requests  # type: ignore[import-untyped]
from bs4 import BeautifulSoup

from fha_data_manager.utils.versioning import SnapshotManifest

# Configure basic logging
logger = logging.getLogger(__name__)

PathLike: TypeAlias = Path | str
Headers: TypeAlias = dict[str, str]
ExcelExtensions: TypeAlias = tuple[str, ...]


def download_excel_files_from_url(
    page_url: str,
    destination_folder: PathLike,
    pause_length: int = 5,
    include_zip: bool = False,
    file_type: str | None = None,
) -> None:
    """Download spreadsheet files linked from ``page_url`` into ``destination_folder``.

    The routine looks for Excel workbooks (``.xlsx``, ``.xls`` and related formats) on
    the target page and optionally processes zip archives that contain spreadsheets.
    Downloaded filenames are standardised when ``file_type`` is provided so they
    match the naming conventions used elsewhere in the project.

    Args:
        page_url: The URL of the webpage to scrape for spreadsheet links.
        destination_folder: Directory where downloaded files should be stored.
        pause_length: Seconds to pause between downloads to avoid hammering the server.
        include_zip: Whether to download ``.zip`` archives in addition to spreadsheets.
        file_type: When provided (``"sf"`` or ``"hecm"``), determines the prefix used
            when standardising filenames. If ``None`` the original filenames are kept.

    Returns:
        ``None``. The function performs downloads for their side-effects only.

    Examples:
        Download the latest single-family spreadsheets directly into the raw
        data directory while standardising filenames:

        >>> from pathlib import Path
        >>> url = "https://www.hud.gov/program_offices/housing/sfh/fha/sfdatasets"
        >>> download_excel_files_from_url(url, Path("data/raw/single_family"), file_type="sf")

        To also pull zip archives that occasionally bundle the workbooks,
        enable ``include_zip`` and stretch out the courtesy pause for larger
        transfers:

        >>> download_excel_files_from_url(
        ...     url,
        ...     Path("data/raw/single_family"),
        ...     pause_length=10,
        ...     include_zip=True,
        ... )
    """

    try:

        manifest = SnapshotManifest()

        # Ensure the destination folder exists
        dest_path = Path(destination_folder)
        dest_path.mkdir(parents=True, exist_ok=True)

        # Specify User Agent for getting page contents
        headers: Headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
        }

        # Get the webpage content
        logger.info("Fetching content from URL: %s", page_url)
        response = requests.get(page_url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

        # Parse the HTML Page
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags with an href attribute
        excel_links_found = 0
        for link_tag in soup.find_all('a', href=True):
            href = link_tag['href']

            # Check if the link points to an Excel file
            excel_extensions: ExcelExtensions = ('.xlsx', '.xls', '.xlsm', '.xlsb')
            if include_zip : # Add Zip (presumed Excel Contents)
                excel_extensions += ('.zip',)
            if href.lower().endswith(excel_extensions):

                excel_links_found += 1

                # Construct the full URL (handles relative links)
                excel_url = urljoin(page_url, href)
                
                # Extract a clean filename from the URL
                try:
                    file_name = Path(urlparse(excel_url).path).name
                    if not file_name: # Handle cases where path might end in /
                        file_name = f"downloaded_excel_{excel_links_found}{Path(excel_url).suffix}"
                except Exception as e:
                    logger.warning("Could not derive filename from URL %s: %s. Using a generic name.", excel_url, e)
                    file_name = f"excel_file_{excel_links_found}{Path(href).suffix or '.xlsx'}"

                # Standardize the filename
                standardized_name = standardize_filename(file_name, file_type)

                # Only Download New Files
                file_path = dest_path / standardized_name
                if not file_path.exists() :
                    logger.info("Downloading %s to %s...", excel_url, file_path)
                    try:

                        # Stream File Content to Local File
                        file_response = requests.get(excel_url, headers=headers, stream=True, timeout=60)
                        file_response.raise_for_status()
                        with file_path.open('wb') as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        # Display Progress
                        logger.info("Successfully downloaded %s", standardized_name)

                        # Process zip files if applicable
                        if file_path.suffix.lower() == '.zip':
                            logger.info("Processing zip file: %s", standardized_name)
                            extracted = process_zip_file(file_path, dest_path, file_type)
                            for extracted_path in extracted:
                                try:
                                    manifest.record_download(extracted_path, snapshot_type=file_type)
                                except Exception as exc:  # pragma: no cover - defensive
                                    logger.warning(
                                        "Unable to record extracted file %s in manifest: %s",
                                        extracted_path,
                                        exc,
                                    )
                        else:
                            try:
                                manifest.record_download(file_path, snapshot_type=file_type)
                            except Exception as exc:  # pragma: no cover - defensive
                                logger.warning(
                                    "Unable to record download %s in manifest: %s",
                                    file_path,
                                    exc,
                                )

                        # Courtesy Pause
                        time.sleep(pause_length)

                    # Display Download Error
                    except requests.exceptions.RequestException as e:
                        logger.error("Error downloading %s: %s", excel_url, e)

                    # Display Inpput/Output Error
                    except IOError as e:
                        logger.error("Error saving file %s to %s: %s", standardized_name, file_path, e)
        
        # Display message if no excel links are discovered
        if excel_links_found == 0:
            logger.info("No Excel file links found on the page.")

    # Display Requests Exception
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching URL %s: %s", page_url, e)

    # Display Unexpected Error
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)


def find_years_in_string(text: str) -> int:
    """Return the four-digit year encoded in ``text``.

    The helper looks for four-digit year patterns first and then for legacy
    two-digit month/year combinations (e.g. ``0113`` for January 2013).

    Args:
        text: The string to search within.

    Returns:
        The four-digit year extracted from ``text``.

    Raises:
        TypeError: If ``text`` is not a string.
        ValueError: If no year-like pattern can be found.
    """
    if not isinstance(text, str):
        raise TypeError("Expected text to be a string when extracting years.")

    # First try to find 4-digit years
    found_years = re.findall(r'(20\d{2})', text)
    if found_years:
        assert len(found_years) == 1, "Warning: The provided string has multiple candidate years."
        return int(found_years[0])

    # If no 4-digit year found, look for 2-digit pattern (e.g., '0113' for Jan 2013)
    # This pattern looks for two digits that could be month (01-12) followed by two digits for year
    # If two-digit pattern found, return the full year (assumes 2000s)
    two_digit_pattern = re.findall(r'(0[1-9]|1[0-2])(\d{2})', text)
    if two_digit_pattern:
        month, year = two_digit_pattern[0]
        full_year = 2000 + int(year)
        return full_year
    
    # If no year pattern found, raise an error
    raise ValueError(f"No valid year pattern found in: {text}")


def find_month_in_string(text: str) -> int | None:
    """Return the month number referenced in ``text`` if one is present.

    The search recognises common three-letter month abbreviations and a handful of
    variants used in the FHA downloads (e.g. ``"JLY"``).

    Args:
        text: The string to inspect.

    Returns:
        The numeric month (``1``-``12``) when a match is found, otherwise ``None``.
    """

    # Dictionary of Month Abbreviations
    month_abbreviations = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "jly": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }

    # Extract Month Abbreviation Strings and Convert to Numerics
    text = text.lower()
    for month_abbreviation, month_number in month_abbreviations.items():
        if month_abbreviation in text:
            return month_number
    return None


def handle_file_dates(file_name: str | Path) -> str:
    """Return a ``_YYYYMM`` suffix derived from ``file_name``.

    The helper is retained for backwards compatibility with scripts that expect a
    year/month suffix rather than a full FHA-standard filename.

    Args:
        file_name: The filename (with or without a path component) to analyse.

    Returns:
        A suffix in the form ``_YYYYMM``.
    """

    # Get Base Name of the File
    base_file_name = Path(file_name).name
    logger.debug("Handling file name: %s", base_file_name)

    # Extract Year/Month and create standard suffix
    year = find_years_in_string(base_file_name)
    month = find_month_in_string(base_file_name)
    ym_suffix = '_' + str(year) + str(month).zfill(2)

    # Return Suffix
    return ym_suffix


def standardize_filename(original_filename: str | Path, file_type: str | None) -> str:
    """Convert FHA snapshot filenames into a standard ``YYYYMMDD`` form.

    Handles multiple filename patterns:

    * Modern format: ``FHA_SFSnapshot_Aug2023.xlsx``
    * Legacy format: ``fha_0113.zip`` (where ``01`` is month and ``13`` is year)

    Args:
        original_filename: The original filename, with or without a path component.
        file_type: Indicates which naming convention to apply (``"sf"`` or ``"hecm"``).
            If ``None`` the original filename is returned.

    Returns:
        A standardised filename matching the project's naming conventions. If the
        filename cannot be parsed the original ``original_filename`` is returned.

    Raises:
        ValueError: If ``file_type`` is not recognised.
    """
    # Get base name without path
    base_name = Path(original_filename).name

    if file_type is None:
        return base_name

    extension = Path(base_name).suffix
    
    try:
        # First try to find month using month name abbreviations
        month = find_month_in_string(base_name)
        
        # If no month name found, try to extract from numeric pattern
        if not month:
            # Look for pattern like '0113' where '01' is month
            month_year_pattern = re.findall(r'(0[1-9]|1[0-2])(\d{2})', base_name)
            if month_year_pattern:
                month = int(month_year_pattern[0][0])
        
        if not month:
            raise ValueError(f"Could not extract month from filename: {base_name}")
        
        # Get year using updated find_years_in_string function
        year = find_years_in_string(base_name)
        
        if not year:
            raise ValueError(f"Could not extract year from filename: {base_name}")

        # Create standardized date string (YYYYMMDD)
        date_str = f"{year}{str(month).zfill(2)}01"

        # Keep original extension (xlsx or zip)
        if file_type == 'sf' :
            new_filename = f"fha_sf_snapshot_{date_str}{extension}"
        elif file_type == 'hecm' :
            new_filename = f"fha_hecm_snapshot_{date_str}{extension}"
        else :
            raise ValueError(f":Invalid file type: {file_type}")

        # Return New Filename
        return new_filename
        
    except Exception as e:
        logger.error("Error standardizing filename %s: %s", base_name, e)
        return base_name  # Return original filename if conversion fails


def process_zip_file(
    zip_path: PathLike, destination_folder: PathLike, file_type: str | None
) -> list[Path]:
    """Extract files from ``zip_path`` into ``destination_folder``.

    Any spreadsheets discovered inside the archive are renamed using
    :func:`standardize_filename` so long as ``file_type`` is provided.

    Args:
        zip_path: Path to the zip file to extract.
        destination_folder: Folder where processed files should be saved.
        file_type: Snapshot type used to standardise filenames (``"sf"`` or
            ``"hecm"``). When ``None`` filenames are preserved as extracted.

    Returns:
        A list containing the paths to extracted spreadsheet files located in
        ``destination_folder``.
    """
    try:
        zip_path = Path(zip_path)
        destination_path = Path(destination_folder)
        destination_path.mkdir(parents=True, exist_ok=True)
        zip_filename = zip_path.name
        try:
            zip_year = find_years_in_string(zip_filename)
            zip_month = find_month_in_string(zip_filename)
            has_zip_date = zip_year is not None and zip_month is not None
        except ValueError:
            has_zip_date = False

        extracted_files: list[Path] = []

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

                for source_path in temp_dir_path.rglob('*'):
                    if not source_path.is_file() or source_path.suffix.lower() not in (
                        '.xlsx', '.xls', '.xlsm', '.xlsb'
                    ):
                        continue

                    try:
                        new_filename = standardize_filename(source_path.name, file_type)
                    except ValueError:
                        if has_zip_date and file_type is not None:
                            date_str = f"{zip_year}{str(zip_month).zfill(2)}01"
                            extension = source_path.suffix
                            if file_type == 'sf':
                                new_filename = f"fha_sf_snapshot_{date_str}{extension}"
                            elif file_type == 'hecm':
                                new_filename = f"fha_hecm_snapshot_{date_str}{extension}"
                            logger.info(
                                "Using zip file date for %s: %s", source_path.name, new_filename
                            )
                        else:
                            new_filename = source_path.name
                            logger.warning(
                                "No date information found for %s, keeping original name",
                                source_path.name,
                            )

                    dest_path = destination_path / new_filename

                    if not dest_path.exists():
                        logger.info("Processing extracted file: %s -> %s", source_path.name, new_filename)
                        dest_path.write_bytes(source_path.read_bytes())
                    else:
                        logger.info("Skipping existing file: %s", new_filename)

                    extracted_files.append(dest_path)

        return extracted_files

    except zipfile.BadZipFile as e:
        logger.error("Error processing zip file %s: %s", zip_path, e)
    except Exception as e:
        logger.error("Unexpected error processing zip file %s: %s", zip_path, e)

    return []

