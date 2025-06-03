# Import Packages
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
import logging
import time
import re
import zipfile
import tempfile

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Download Excel Files from URL
def download_excel_files_from_url(page_url: str, destination_folder: str, pause_length: int=5, include_zip: bool=False, file_type: str=None):
    """
    Finds all linked Excel files on a given webpage and downloads them
    to a specified folder.

    Args:
        page_url (str): The URL of the webpage to scrape for Excel links.
        destination_folder (str): The path to the folder where Excel files
                                  will be downloaded.
    """

    try:

        # Ensure the destination folder exists
        dest_path = Path(destination_folder)
        dest_path.mkdir(parents=True, exist_ok=True)

        # Specify User Agent for getting page contents
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
        }

        # Get the webpage content
        logging.info(f"Fetching content from URL: {page_url}")
        response = requests.get(page_url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

        # Parse the HTML Page
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags with an href attribute
        excel_links_found = 0
        for link_tag in soup.find_all('a', href=True):
            href = link_tag['href']
            
            # Check if the link points to an Excel file
            excel_extensions = ('.xlsx', '.xls', '.xlsm', '.xlsb')
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
                    logging.warning(f"Could not derive filename from URL {excel_url}: {e}. Using a generic name.")
                    file_name = f"excel_file_{excel_links_found}{Path(href).suffix or '.xlsx'}"

                # Standardize the filename
                standardized_name = standardize_filename(file_name, file_type)

                # Only Download New Files
                file_path = dest_path / standardized_name
                if not os.path.exists(file_path) :
                    logging.info(f"Downloading {excel_url} to {file_path}...")
                    try:

                        # Stream File Content to Local File
                        file_response = requests.get(excel_url, headers=headers, stream=True, timeout=60)
                        file_response.raise_for_status()
                        with open(file_path, 'wb') as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        # Display Progress
                        logging.info(f"Successfully downloaded {standardized_name}")

                        # Process zip files if applicable
                        if file_path.suffix.lower() == '.zip':
                            logging.info(f"Processing zip file: {standardized_name}")
                            process_zip_file(str(file_path), str(dest_path), file_type)

                        # Courtesy Pause
                        time.sleep(pause_length)

                    # Display Download Error
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Error downloading {excel_url}: {e}")

                    # Display Inpput/Output Error
                    except IOError as e:
                        logging.error(f"Error saving file {standardized_name} to {file_path}: {e}")
        
        # Display message if no excel links are discovered
        if excel_links_found == 0:
            logging.info("No Excel file links found on the page.")

    # Display Requests Exception
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL {page_url}: {e}")

    # Display Unexpected Error
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

# Find Years in String
def find_years_in_string(text: str) -> int:
    """
    Searches a string for year patterns and returns the full year as an integer.
    Handles both 4-digit years (e.g., 2023) and 2-digit years (e.g., 13 for 2013).
    
    Args:
        text (str): The string to search within.
    
    Returns:
        int: The full year (e.g., 2013). Returns None if no valid year found.
    """
    # Check that input text is a string
    if not isinstance(text, str):
        logging.warning("Input to find_years_in_string was not a string. Returning None.")
        return None

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

# Find Month in String
def find_month_in_string(text: str) -> int:
    """
    Return the numeric value of the month whose abbreviation is contained in the provided string.
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

# Handle File Dates
def handle_file_dates(file_name: str) -> str :
    """
    Takes a file name and creates a standardized suffix in the form _YYYYMM.
    """

    # Get Base Name of the File
    base_file_name = os.path.basename(file_name)
    print(base_file_name)

    # Extract Year/Month and create standard suffix
    year = find_years_in_string(base_file_name)
    month = find_month_in_string(base_file_name)
    ym_suffix = '_' + str(year) + str(month).zfill(2)

    # Return Suffix
    return ym_suffix

# Standardize Filename
def standardize_filename(original_filename: str, file_type: str) -> str:
    """
    Converts various FHA snapshot filenames into a standardized YYYYMMDD format.
    Handles multiple filename patterns:
    - Modern format: 'FHA_SFSnapshot_Aug2023.xlsx'
    - Legacy format: 'fha_0113.zip' (where 01 is month and 13 is year)
    
    Args:
        original_filename (str): The original filename
        
    Returns:
        str: Standardized filename in format 'fha_snapshot_YYYYMMDD.[xlsx/zip]'

    """
    # Get base name without path
    base_name = os.path.basename(original_filename)
    extension = os.path.splitext(base_name)[1]
    
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
            new_filename = f"fha_snapshot_{date_str}{extension}"
        elif file_type == 'hecm' :
            new_filename = f"fha_hecm_snapshot_{date_str}{extension}"
        else :
            raise ValueError(f":Invalid file type: {file_type}")

        # Return New Filename
        return new_filename
        
    except Exception as e:
        logging.error(f"Error standardizing filename {base_name}: {e}")
        return base_name  # Return original filename if conversion fails

# Process Zip Files
def process_zip_file(zip_path: str, destination_folder: str, file_type: str) -> None:
    """
    Extracts and processes files from a zip archive, applying the same standardization
    rules as direct downloads.

    Args:
        zip_path (str): Path to the zip file
        destination_folder (str): Folder where processed files should be saved
        file_type (str): Type of file ('sf' or 'hecm') for standardization
    """
    try:
        # Get the standardized name of the zip file (without .zip extension)
        zip_filename = os.path.basename(zip_path)
        try:
            # Try to extract date information from zip filename
            zip_year = find_years_in_string(zip_filename)
            zip_month = find_month_in_string(zip_filename)
            has_zip_date = zip_year is not None and zip_month is not None
        except ValueError:
            has_zip_date = False

        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract all files to temporary directory
                zip_ref.extractall(temp_dir)
                
                # Process each file in the extracted contents
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        if file.lower().endswith(('.xlsx', '.xls', '.xlsm', '.xlsb')):
                            # Get full path of extracted file
                            source_path = os.path.join(root, file)
                            
                            try:
                                # Try to standardize filename using file's own date info
                                new_filename = standardize_filename(file, file_type)
                            except ValueError as e:
                                # If no date in filename and we have zip date info, use zip's date
                                if has_zip_date:
                                    date_str = f"{zip_year}{str(zip_month).zfill(2)}01"
                                    extension = os.path.splitext(file)[1]
                                    if file_type == 'sf':
                                        new_filename = f"fha_snapshot_{date_str}{extension}"
                                    elif file_type == 'hecm':
                                        new_filename = f"fha_hecm_snapshot_{date_str}{extension}"
                                    logging.info(f"Using zip file date for {file}: {new_filename}")
                                else:
                                    # If no date information available anywhere, use original filename
                                    new_filename = file
                                    logging.warning(f"No date information found for {file}, keeping original name")
                            
                            dest_path = os.path.join(destination_folder, new_filename)
                            
                            # Copy file to destination with standardized name
                            if not os.path.exists(dest_path):
                                logging.info(f"Processing extracted file: {file} -> {new_filename}")
                                with open(source_path, 'rb') as src, open(dest_path, 'wb') as dst:
                                    dst.write(src.read())
                            else:
                                logging.info(f"Skipping existing file: {new_filename}")
                                
    except zipfile.BadZipFile as e:
        logging.error(f"Error processing zip file {zip_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing zip file {zip_path}: {e}")

# Main Routine
if __name__ == "__main__":

    # Download Single-Samily Data
    target_url = "https://www.hud.gov/stat/sfh/fha-sf-portfolio-snapshot"
    download_to_folder = "./data/raw/temp"
    download_excel_files_from_url(target_url, download_to_folder, include_zip=True, file_type='sf')

    # Download HECM Data
    target_url = "https://www.hud.gov/hud-partners/hecmsf-snapshot"
    download_to_folder = "./data/raw/temp_hecm"
    # download_excel_files_from_url(target_url, download_to_folder, include_zip=True, file_type='hecm')

    # Download Multifamily Data
