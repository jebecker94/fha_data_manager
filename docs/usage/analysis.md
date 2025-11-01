# Analysis Guide

Guide to performing exploratory data analysis and institutional analysis with FHA Data Manager.

## Overview

FHA Data Manager provides two main types of analysis:

1. **Exploratory Analysis** - Trends, volumes, and loan characteristics
2. **Institutional Analysis** - Lender/sponsor identities and relationships

## Exploratory Analysis

### Loading Data

```python
from fha_data_manager.analysis import load_combined_data

lf = load_combined_data("data/database/single_family")
# Collect to a DataFrame only if you need to materialize the entire table
df = lf.collect()
```

### Lender Activity Analysis

Analyze lender market activity and concentration:

```python
from fha_data_manager.analysis import analyze_lender_activity

lender_stats = analyze_lender_activity(lf)

# Top lenders by volume
print(lender_stats['lender_volume'].head(10))

# Yearly lender trends
print(lender_stats['yearly_lenders'])
```

**Outputs**:
- `lender_volume` - Top 20 lenders by loan count and total volume
- `yearly_lenders` - Active lenders and loan counts by year

### Sponsor Activity Analysis

Analyze sponsor participation in FHA lending:

```python
from fha_data_manager.analysis import analyze_sponsor_activity

sponsor_stats = analyze_sponsor_activity(lf)

# Top sponsors
print(sponsor_stats['sponsor_volume'].head(10))

# Sponsorship trends
print(sponsor_stats['yearly_sponsors'])
```

### Loan Characteristics

Analyze loan types, sizes, and distributions:

```python
from fha_data_manager.analysis import analyze_loan_characteristics

loan_stats = analyze_loan_characteristics(df)

# Loan purpose distribution
print(loan_stats['loan_purpose'])

# Down payment sources
print(loan_stats['down_payment'])

# Loan size trends
print(loan_stats['yearly_loan_size'])
```

### Running Complete Analysis

Run all exploratory analyses with visualizations:

```bash
python -m fha_data_manager.analysis.exploratory
```

Or see `examples/05_analyze_data.py` for a complete example.

**Outputs**:
- Console summary statistics
- `output/active_lenders_trend.png` - Lender count over time
- `output/avg_loan_size_trend.png` - Average loan size over time
- `output/loan_purpose_dist.png` - Loan purpose distribution

## Institutional Analysis

### Initialize Analyzer

```python
from fha_data_manager.analysis.institutions import InstitutionAnalyzer

analyzer = InstitutionAnalyzer("data/database/single_family")
analyzer.load_data()
```

### Build Institution Crosswalk

Create a mapping of institution IDs to names over time:

```python
crosswalk = analyzer.build_institution_crosswalk()
print(crosswalk.head(20))
```

**Output columns**:
- `institution_number` - ID number
- `institution_name` - Institution name
- `type` - "Originator" or "Sponsor"
- `first_date` - First appearance
- `last_date` - Last appearance
- `num_months` - Number of months active

### Find Mapping Errors

Identify potential data quality issues in ID-name mappings:

```python
errors = analyzer.find_mapping_errors()
print(f"Found {len(errors)} mapping errors")
print(errors)
```

**Error types**:
- Multiple names for same ID in one month
- Name oscillations (name changes back and forth)

### Analyze Name Changes

Track how institution names change over time:

```python
# Analyze specific institutions (e.g., Quicken/Rocket, Freedom)
name_changes = analyzer.analyze_name_changes_over_time(
    notable_ids=[71970, 75159],
    log_file="output/name_changes.txt"
)

# Or analyze all
name_changes = analyzer.analyze_name_changes_over_time()
```

### Detect Oscillations

Find institutions with inconsistent naming patterns:

```python
oscillations = analyzer.detect_oscillations(
    log_file="output/oscillations.txt"
)

print(f"Found {len(oscillations['originators'])} originator oscillations")
print(f"Found {len(oscillations['sponsors'])} sponsor oscillations")
```

### Analyze ID Spaces

Check for overlaps between originator and sponsor ID spaces:

```python
id_stats = analyzer.analyze_id_spaces(
    log_file="output/id_spaces.txt"
)

print(f"Unique originator IDs: {id_stats['unique_originator_ids']:,}")
print(f"Unique sponsor IDs: {id_stats['unique_sponsor_ids']:,}")
print(f"Overlapping IDs: {id_stats['overlapping_ids']:,}")
```

### Generate Comprehensive Report

Run all institutional analyses and generate a complete report:

```python
analyzer.generate_full_report(output_dir="output")
```

Or via command line:

```bash
python -m fha_data_manager.analysis.institutions
```

**Outputs**:
- `output/institution_crosswalk.csv` - Complete ID-name crosswalk
- `output/institution_mapping_errors.csv` - Detected errors
- `output/institution_analysis_report.txt` - Detailed analysis report

## Custom Analysis Examples

### Example 1: Market Concentration

Calculate market concentration (HHI index):

```python
import polars as pl

df = pl.scan_parquet("data/database/single_family")

# Calculate market shares by year
market_shares = (
    df
    .group_by(["Year", "Originating Mortgagee"])
    .agg(pl.count().alias("loans"))
    .with_columns([
        (pl.col("loans") / pl.col("loans").sum().over("Year")).alias("share")
    ])
    .sort(["Year", "loans"], descending=[False, True])
    .collect()
)

# Calculate HHI by year
hhi = (
    market_shares
    .group_by("Year")
    .agg((pl.col("share") ** 2).sum().alias("HHI"))
)
print(hhi)
```

### Example 2: Geographic Analysis

Analyze lending patterns by state:

```python
state_stats = (
    df
    .group_by("Property State")
    .agg([
        pl.count().alias("loan_count"),
        pl.col("Mortgage Amount").mean().alias("avg_loan_size"),
        pl.col("Interest Rate").mean().alias("avg_rate"),
        pl.col("Originating Mortgagee").n_unique().alias("unique_lenders"),
    ])
    .sort("loan_count", descending=True)
    .collect()
)
print(state_stats.head(20))
```

### Example 3: Time Series Analysis

Analyze trends over time:

```python
import matplotlib.pyplot as plt

monthly_stats = (
    df
    .group_by(["Year", "Month"])
    .agg([
        pl.count().alias("loan_count"),
        pl.col("Mortgage Amount").mean().alias("avg_amount"),
        pl.col("Interest Rate").mean().alias("avg_rate"),
    ])
    .sort(["Year", "Month"])
    .collect()
)

# Create time series plot
plt.figure(figsize=(14, 8))

plt.subplot(3, 1, 1)
plt.plot(range(len(monthly_stats)), monthly_stats["loan_count"])
plt.ylabel("Loan Count")
plt.title("FHA Monthly Trends")

plt.subplot(3, 1, 2)
plt.plot(range(len(monthly_stats)), monthly_stats["avg_amount"])
plt.ylabel("Avg Loan Amount ($)")

plt.subplot(3, 1, 3)
plt.plot(range(len(monthly_stats)), monthly_stats["avg_rate"])
plt.ylabel("Avg Interest Rate (%)")
plt.xlabel("Month")

plt.tight_layout()
plt.savefig("output/monthly_trends.png")
```

## Performance Tips

### 1. Use Lazy Evaluation

```python
# Good - lazy evaluation
df = pl.scan_parquet("data/database/single_family")
result = df.filter(...).group_by(...).collect()

# Avoid - loads everything into memory
df = pl.read_parquet("data/database/single_family")
```

### 2. Filter Early

```python
# Good - filter before heavy operations
result = (
    df
    .filter(pl.col("Year") >= 2020)
    .filter(pl.col("Property State") == "CA")
    .group_by("Originating Mortgagee")
    .agg(pl.count())
    .collect()
)
```

### 3. Use Hive Partitioning

```python
# Load specific partition
df = pl.scan_parquet("data/database/single_family/Year=2025/Month=6")
```

### 4. Sample for Development

```python
# Use a sample for developing queries
sample = pl.read_parquet("data/database/single_family", n_rows=10000)
```

## Next Steps

- See [Validation Guide](validation.md) for data quality checks
- See [Data Pipeline Guide](data_pipeline.md) for complete workflow
- Check [examples/](../../examples/) for complete examples
- Review [Data Schemas](../schemas/data_dictionaries.md) for column details

