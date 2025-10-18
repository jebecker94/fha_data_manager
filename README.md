# FHA Data Manager
Tools for managing FHA's single-family and HECM snapshot data for research projects.

# Functionality
Cleans individual single-family and HECM snapshots from the FHA website, and creates a standardized dataset for research purposes.

# How To Use
1. Download the FHA Snapshot Data
- Run the download_fha_data script to automatically download the single family and hecm snapshots from their respective FHA web pages and save them with a standardized naming cconvention
- The script handles both direct Excel links and zip archives, standardizing filenames as they are saved
2. Clean the single family and HECM data files
- Convert the excel (or zipped excel) files into parquets
- Add a new variable called "FHA_Index" to each observation prior to saving. The index variable is a combination of the year and month, then the row number of the observation in the original data file, left-padded with zeroes to be seven digits long.
- Note: The row number increments sequentially across all data sheets in the file. Usually the purchase mortgages come first and then refinances, but this may change over time as there are very few quality checks in place for the release of this data, it seems.
3. Combine and save the single family and HECM data files to hive-structured database
- Implements a handful of standardization checks and saves to a partitioned parquet structure. These checks are described below.

## Data Standardization
When processing the data, we perform the following data standardization steps:
- Create datetime variables for each month
- Standardize county names where possible and use the AddFIPS package to translate them into a FIPS code.
    - The FIPS code is constructed by using the state name and the county name. In a number of cases the county names are spelled incorrectly in the FHA files. In these cases, I implement a few manual changes where I can reasonably infer the correct county name from the mistaken spelling. In a small number of cases, the county name is a well-known county name from a different state. For these cases I use the FIPS code for the county name in the inferred state.

## Data Quality Notes:
- For the refinance data tab in August of 2014 there is a major data quality issue with the originating mortgage and sponsor names and numbers. Originating Mortgagee numbers appear to be correct; the sponsor numbers are correct while the sponsor name is always identical to the originating mortgagee name. If this is not corrected for, the crosswalk between names and ID numbers will be incorrect.
- A Small number of variables are written differently across various months' data files. These are converted into a standardized set of variable names to make it easier to combine the monthly files.
- The loan purpose value is non-standard in a few of the file types. This is fixed wherever necessary so that the resulting loan purpose types take one of three values.

## Notes on Lender Names
Extensive exploration suggests that the originating mortgagee and sponsor IDs and names are consistent across file types (single family and HECM), loan purpose (purchase and refinance sheets), and time. The mapping between IDs and names is not one-to-one; rather, when an institution experiences a name change or merger/acquisition event, the name might change while the ID remains in use. For instance, Quicken -> Quicken, LLC -> Rocket Mortgage. As some experimental script show, these name changes are predictable overall--very few cases where a name changes and then changes back.

## Note on matching with other datasets
- Matching with HMDA (public)
    * To perform a match between the FHA single-family mortgage data and the HMDA loan-level data, one can do the following:
    1. Sample: 2010-Present HMDA yearly files
    2. Match Variables: Variables differ before and after 2018.
    a. Pre-2018: Location (add zips by matching to a zip-tract crosswalk), loan size (to the nearest 1000), date (match hmda loans in year X by considering loans in year X-1 month 12 until X+1 month 1), lender (best to infer from an initial match on non-lender characteristics), and loan purpose.
    b. Post-2018: Add interest rate, use binned loan amounts (10000 dollar bins centered on 5000s)
- Matching with GNMA (public)
    * To perform a match between the FHA single-family mortgage data and the GNMA loan-level data, one can do the following:
    1. Sample: GNMA monthly origination files
    2. Match Variables: Location (only state), loan size (to the nearest 1000), date (consider matches within a few months, using the loaan origination date where available and the first payment date otherwise), and interest rate
    3. Supplementary data: Matching between FHA and GNMA data is best accomplished with the aid of the HMDA data (which can supplement the FHA data by providing the number of borrowers) and the CoreLogic data (which greatly supplements the FHA data by providing an exact mortgage date, which matches well to the origination date present in GNMA after 2015). Without these files, this match becomes extremely difficult, due to the coarseness of the GNMA data.
- Matching with CoreLogic (non-public)
    * To perform a match between the FHA single family mortgage data and the CoreLogic data, one can do the following:
    1. Sample: CoreLogic FHA loans identified using the loan type indicator
    2. Match Variables: Location (state, county, zip), loan size (to the dollar), interest rate (for a small number of mostly adjustable-rate CoreLogic observations), date (fuzzy, allowing for 1-2 months between the CoreLogic mortgage date and the FHA endorsement month/year), and lender (best to infer lender matches from an initial tight match on non-lender characteristics).

## Track Your Local Data Inventory
Use the ``log_data_inventory.py`` script to create a CSV snapshot of the raw and clean
files you currently have stored in the project.

```
python log_data_inventory.py
```

By default the inventory is saved to ``data/data_inventory.csv`` and includes file size
and timestamp metadata for easy review. Adjust the destination by configuring the
``DATA_DIR`` setting via environment variables (see ``config.py``) if you would like the
inventory saved elsewhere.
