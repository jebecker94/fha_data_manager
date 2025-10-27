# Categorical Variables in Single Family FHA Data

This document lists all possible values for the four categorical variables in the Single Family FHA dataset.

## 1. Loan Purpose

Values this variable can take:
- **Purchase** (11,326,753 occurrences)
- **Refi_FHA** (3,745,277 occurrences)
- **Refi_Conv_Curr** (1,613,078 occurrences)

Total: 16,685,108 records

## 2. Property Type

Values this variable can take:
- **Single Family** (16,032,402 occurrences)
- **Condo** (467,900 occurrences)
- **Rehabilitation** (184,114 occurrences)
- **H4H** (667 occurrences)
- **Other** (25 occurrences)

Total: 16,685,108 records

## 3. Product Type

Values this variable can take:
- **Fixed Rate** (16,530,410 occurrences)
- **Adjustable Rate** (154,698 occurrences)

Total: 16,685,108 records

## 4. Down Payment Source

Values this variable can take:
- **Borrower** (13,386,064 occurrences)
- **Relative** (2,950,147 occurrences)
- **Gov Asst** (243,723 occurrences)
- **Non Profit** (59,297 occurrences)
- **Employer** (45,875 occurrences)
- **null** (2 occurrences)

Total: 16,685,108 records

## Notes

- The counts are based on the silver-tier database (`data/silver/single_family/`)
- All four variables are categorical with discrete values
- Down Payment Source includes a null value (2 occurrences)
- Property Type has 5 distinct values, with "Single Family" being the most common (96% of all records)
- Product Type is binary, with "Fixed Rate" being predominant (99% of all records)

