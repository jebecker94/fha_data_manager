# Suggestions for extending the FHA Data Manager to use external data

This project is designed to work with minimal use of external resources. However, it would be valuable to integrate the FHA data with external sources like lender IDs (HMDA, FRB, etc.). Below are some suggestions for future extensions that I am open to exploring in collaboration with others.

## Data Enrichment
- [ ] Add support for joining with CBSA-to-county crosswalk files (available from Census/HUD) to enable metro-level analyses in `geo.py`.
- [ ] Create utilities to merge FHA data with HMDA data for enhanced lender and borrower characteristics.
- [ ] Build a crosswalk between FHA lender IDs and RSSD IDs (Federal Reserve) to link with regulatory data.
- [ ] Add support for incorporating HUD area median income (AMI) data for analysis of lending in low/moderate-income areas.
- [ ] Implement house price index (FHFA HPI) integration for analyzing loan-to-value dynamics over time.
- [ ] Add functions to compute market shares and concentration ratios for different lender types (banks, non-banks, credit unions).
