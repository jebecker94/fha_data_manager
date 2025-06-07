# fha_data_manager
Tools for managing FHA's single-family and HECM snapshot data for research projects.

# Functionality
Cleans individual single-family and HECM snapshots from the FHA website, and creates a standardized dataset for research purposes.

# How To Use
1. Download the FHA Snapshot Data
- Run the download_fha_data script to automatically download the single family and hecm snapshots from their respective FHA web pages and save them with a standardized naming cconvention
- Note: In progress
2. Clean the single family and HECM data files
- Convert the excel (or zipped excel) files into parquets
- Add a new variable called "FHA_Index" to each observation prior to saving. The index variable is a combination of the year and month, then the row number of the observation in the original data file, left-padded with zeroes to be seven digits long.
- Note: The row number increments sequentially across all data sheets in the file. Usually the purchase mortgages come first and then refinances, but this may change over time as there are very few quality checks in place for the release of this data, it seems.
3. Combine the single family and HECM data files
- Implements a handful of standardization checks alongside the combination process. These checks are described below.

## Data Standardization
For the combined files, we perform the following data standardization steps:
- Create datetime variables for each month
- Standardize county names where possible and use the AddFIPS package to translate them into a FIPS code.

## Data Quality Notes:
- For the refinance data tab in August of 2014 there is a major data quality issue with the originating mortgage and sponsor names and numbers. Originating Mortgagee numbers appear to be correct; the sponsor numbers are correct while the sponsor name is always identical to the originating mortgagee name. If this is not corrected for, the crosswalk between names and ID numbers will be incorrect.