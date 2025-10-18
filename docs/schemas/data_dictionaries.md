# FHA Data Schemas and Data Dictionaries

Complete schema definitions for FHA Single Family and HECM datasets.

## Single Family Schema

### Required Columns

All Single Family data includes the following columns:

| Column Name | Data Type | Description | Notes |
|------------|-----------|-------------|-------|
| `Property State` | string | Two-letter state code | e.g., "CA", "TX" |
| `Property City` | string | City name | |
| `Property County` | string | County name | |
| `Property Zip` | int32 | ZIP code | 5-digit |
| `Originating Mortgagee` | string | Lender name | |
| `Originating Mortgagee Number` | int32 | Lender ID | May be null (~15% of records) |
| `Sponsor Name` | string | Sponsor name | Present for most loans |
| `Sponsor Number` | int32 | Sponsor ID | |
| `Down Payment Source` | string | Source of down payment | e.g., "Gift", "Savings" |
| `Non Profit Number` | int64 | Non-profit organization ID | Rarely populated |
| `Product Type` | string | FHA product type | e.g., "Standard", "Energy Efficient" |
| `Loan Purpose` | string | Loan purpose | "Purchase" or "Refinance" |
| `Property Type` | string | Property type | e.g., "Single Family", "Condo" |
| `Interest Rate` | float64 | Interest rate | Percentage (e.g., 3.5 = 3.5%) |
| `Mortgage Amount` | int64 | Original loan amount | US Dollars |
| `Year` | int16 | Endorsement year | |
| `Month` | int16 | Endorsement month | 1-12 |
| `FHA_Index` | string | Unique identifier | Format: YYYYMMDD_XXXXXXX |

### Derived/Added Columns

When data is processed through the pipeline, these columns may be added:

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| `Date` | date | Date constructed from Year/Month | Set to first of month |
| `FIPS` | string | County FIPS code | Added when county can be matched |

## HECM (Reverse Mortgage) Schema

### Required Columns

| Column Name | Data Type | Description | Notes |
|------------|-----------|-------------|-------|
| `Property State` | string | Two-letter state code | |
| `Property City` | string | City name | |
| `Property County` | string | County name | |
| `Property Zip` | int32 | ZIP code | |
| `Originating Mortgagee` | string | Lender name | |
| `Originating Mortgagee Number` | int32 | Lender ID | |
| `Sponsor Name` | string | Sponsor name | |
| `Sponsor Number` | int32 | Sponsor ID | |
| `Sponsor Originator` | string | Sponsor originator name | |
| `NMLS` | int64 | NMLS number | |
| `Standard/Saver` | string | Product type | "Standard" or "Saver" |
| `Purchase/Refinance` | string | Loan purpose | |
| `Rate Type` | string | Interest rate type | Fixed or Variable |
| `Interest Rate` | float64 | Interest rate | Percentage |
| `Initial Principal Limit` | float64 | Initial principal limit | US Dollars |
| `Maximum Claim Amount` | float64 | Maximum claim amount | US Dollars |
| `Year` | int16 | Endorsement year | |
| `Month` | int16 | Endorsement month | |
| `Current Servicer ID` | int64 | Current servicer identifier | |
| `Previous Servicer ID` | int64 | Previous servicer identifier | |
| `FHA_Index` | string | Unique identifier | Format: YYYYMMDD_XXXXXXX |

## Data Quality Notes

### Known Issues

#### 1. August 2014 Sponsor Name Bug

**Issue**: In the refinance data tab for August 2014, sponsor names are incorrectly set to the originating mortgagee name, while sponsor numbers remain correct.

**Handling**: The import pipeline automatically sets sponsor names to empty string for this month.

#### 2. Missing Originator IDs

**Issue**: Approximately 15.66% of loans have missing Originating Mortgagee Numbers.

**Pattern**: These loans typically have sponsor information.

**Status**: This appears to be a data reporting pattern rather than an error.

#### 3. Name Variations

**Issue**: Same institution ID may have multiple name spellings due to:
- Typos (e.g., "NORWICH COMMERICAL" vs "NORWICH COMMERCIAL")
- Extra spaces
- Legitimate name changes (e.g., "Quicken Loans" → "Rocket Mortgage")

**Handling**: Use institutional analysis tools to identify and track these variations.

### Validation Rules

The validation suite checks for:

1. **Required Columns Present** - All expected columns exist
2. **FHA_Index Uniqueness** - No duplicate identifiers
3. **ID-Name Consistency** - Same ID doesn't map to multiple names in same month
4. **Data Ranges** - Mortgage amounts and interest rates within reasonable bounds
5. **Completeness** - Missing data below threshold levels

See [Validation Guide](../usage/validation.md) for details.

## FHA_Index Construction

The `FHA_Index` is a unique identifier constructed as follows:

```
YYYYMMDD_XXXXXXX
```

Where:
- `YYYYMM` = Year and month (e.g., 202506 for June 2025)
- `DD` = Always "01" (first of month)
- `XXXXXXX` = Row number in original file, zero-padded to 7 digits

Example: `20250601_0001234` = 1,234th row in June 2025 data

**Note**: Row numbers increment sequentially across all sheets in the original file (typically Purchase first, then Refinance).

## Data Types

### Pandas vs Polars vs PyArrow

The schema is implemented with support for multiple dataframe libraries:

**Pandas/Polars**:
- Strings: `str` or `pl.Utf8`
- Integers: `Int32`, `Int64`, `Int16` (nullable integers)
- Floats: `float64`

**PyArrow**:
- Strings: `pa.string()`
- Integers: `pa.int32()`, `pa.int64()`, `pa.int16()`
- Floats: `pa.float64()`

See `mtgdicts.py` for implementation details.

## Categorical Values

### Loan Purpose
- "Purchase"
- "Refinance"

### Property Type
- "Single Family"
- "Condo"
- "Manufactured Home"
- Others (varies by time period)

### Product Type (Single Family)
- "Standard"
- "Energy Efficient"
- Others (varies)

### Down Payment Source
- "Gift"
- "Savings"
- "Sale of Property"
- "Cash on Hand"
- Others

### Standard/Saver (HECM)
- "Standard"
- "Saver"

### Rate Type (HECM)
- "Fixed"
- "Variable"

## Geographic Data

### State Codes
Two-letter abbreviations following US Postal Service standards.

### County Names
- Free text field
- May contain spelling variations
- Can be matched to FIPS codes using `addfips` package
- Some manual corrections applied for common misspellings

### FIPS Codes
- 5-digit codes (2-digit state + 3-digit county)
- Added during processing when county can be matched
- ~1-2% of records may have missing FIPS due to unmatched counties

## Temporal Coverage

### Single Family
- **Available**: May 2010 - Present
- **Update Frequency**: Monthly (typically released mid-month)
- **Completeness**: Near-complete coverage since 2010

### HECM
- **Available**: July 2010 - Present (post-2011 recommended)
- **Update Frequency**: Monthly
- **Note**: Early years (2010-2011) have formatting inconsistencies

## Usage Examples

### Loading with Schema Validation

```python
from mtgdicts import FHADictionary
import polars as pl

# Get schema
fha_dict = FHADictionary()
schema = fha_dict.single_family.schema

# Load with schema enforcement
df = pl.read_parquet("data.parquet", schema=schema)
```

### Type Casting

```python
# Cast to expected types
df = df.cast(fha_dict.single_family.data_types)
```

## References

- [FHA Single Family Data Portal](https://www.hud.gov/program_offices/housing/rmra/oe/rpts/sfsnap/sfsnap)
- [FHA HECM Data Portal](https://www.hud.gov/program_offices/housing/rmra/oe/rpts/hecm/hecmsfsnap)
- Project repo: `mtgdicts.py` for schema implementation

