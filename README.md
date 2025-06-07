# fha_data_manager
Tools for managing FHA's single-family and HECM snapshot data for research projects.

# Functionality
Cleans individual single-family and HECM snapshots from the FHA website, and creates a standardized dataset for research purposes.

# How To Use
1. Download the FHA Snapshot Data
- Note: Eventually this will be automated.
2. Standardize the naming of FHA files
- Note: Eventually this will be automated.

## Data Standardization:
- Create datetime variables for each month
- Standardize county names where possible and use the AddFIPS package to translate them into a FIPS code.

## Data Quality Notes:
- For the refinance data tab in August of 2014 there is a major data quality issue with the originating mortgage and sponsor names and numbers. Originating Mortgagee numbers appear to be correct; the sponsor numbers are correct while the sponsor name is always identical to the originating mortgagee name. If this is not corrected for, the crosswalk between names and ID numbers will be incorrect.