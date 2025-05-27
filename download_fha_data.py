# Import Packages
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
import logging
import time
import re

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Download Excel Files from URL
def download_excel_files_from_url(page_url: str, destination_folder: str, pause_length: int=5, include_zip: bool=False):
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

                # Get Standard Suffix
                print(file_name)
                suffix = handle_file_dates(file_name)
                print(suffix)

                # Only Download New Files
                file_path = dest_path / file_name
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
                        logging.info(f"Successfully downloaded {file_name}")

                        # Courtesy Pause
                        time.sleep(pause_length)

                    # Display Download Error
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Error downloading {excel_url}: {e}")

                    # Display Inpput/Output Error
                    except IOError as e:
                        logging.error(f"Error saving file {file_name} to {file_path}: {e}")
        
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
    Searches a string for four-digit numbers (assumed to be years)
    and returns them as a list of integers.

    Args:
        text (str): The string to search within.

    Returns:
        list[int]: A list of four-digit numbers found in the string,
                   converted to integers. Returns an empty list if no
                   four-digit numbers are found.
    """

    # Check that input text is a string
    if not isinstance(text, str):
        logging.warning("Input to find_years_in_string was not a string. Returning None.")
        return None

    # Regex to find exactly four digits, using word boundaries to avoid matching parts of longer numbers.
    found_years = re.findall(r'\b(\d{4})\b', text)
    found_years = re.findall(r'\d{4}', text)

    # Handle Missing/Multiple Years
    assert len(found_years) > 0, "Warning: The provided string has no candidate years."
    assert len(found_years) <= 1, "Warning: The provided string has multiple candidate years."

    # Return the Year as an int
    return int(found_years[0])

# Find Month in String
def find_month_in_string(text: str) -> int:
    """
    Return the numeric value of the month whose abbreviation is contained in the provided string.
    """

    # Dictionary of Month Abbreviations
    month_abbreviations = {
        1:"jan",
        2:"feb",
        3:"mar",
        4:"apr",
        5:"may",
        6:"jun",
        7:"jul",
        8:"aug",
        9:"sep",
        10:"oct",
        11:"nov",
        12:"dec",
    }
    for month_number, month_abbreviation in month_abbreviations.items() :
        if month_abbreviation.lower() in text.lower() :
            return month_number
    return None

# Handle File Dates
def handle_file_dates(file_name) -> str :
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

# Main Routine
if __name__ == "__main__":

    # Download Single-Samily Data
    target_url = "https://www.hud.gov/stat/sfh/fha-sf-portfolio-snapshot"
    download_to_folder = "./data/raw/temp"
    download_excel_files_from_url(target_url, download_to_folder, include_zip=True)

    # Download HECM Data
    # target_url = "https://www.hud.gov/hud-partners/hecmsf-snapshot"
    # download_to_folder = "./data/raw/temp_hecm"
    # download_excel_files_from_url(target_url, download_to_folder, include_zip=True)

    # Download Multifamily Data