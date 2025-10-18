# Data Pipeline Guide

Complete workflow guide for managing FHA data from download to analysis.

## Overview

The FHA Data Manager provides a complete pipeline for:
1. **Downloading** FHA data snapshots
2. **Importing** and cleaning the data
3. **Validating** data quality
4. **Analyzing** the processed data

## Quick Start

```bash
# Install dependencies
uv sync

# 1. Download data
python download_fha_data.py

# 2. Import and clean
python import_fha_data.py

# 3. Validate data quality
python -m fha_data_manager.validation.validators

# 4. Analyze
python -m fha_data_manager.analysis.exploratory
```

## Step-by-Step Guide

### 1. Download FHA Data

The download script fetches Single Family and HECM snapshots from FHA's website:

```bash
python download_fha_data.py
```

Or use the package API:

```python
from fha_data_manager import (
    download_single_family_snapshots,
    download_hecm_snapshots,
)

download_single_family_snapshots()
download_hecm_snapshots()
```

**Output**: Raw files saved to `data/raw/single_family/` and `data/raw/hecm/`

### 2. Import and Clean Data

The import script processes raw files and saves them to a hive-structured database:

```bash
python import_fha_data.py
```

Or use the package API:

```python
from fha_data_manager.import_cli import (
    import_single_family_snapshots,
    import_hecm_snapshots,
)

import_single_family_snapshots()
import_hecm_snapshots()
```

**What it does**:
- Converts Excel/CSV files to Parquet format
- Standardizes column names and data types
- Adds FHA_Index unique identifier
- Handles data quality issues (e.g., Aug 2014 sponsor name bug)
- Saves to hive-partitioned structure

**Output**: Clean data saved to `data/database/single_family/` and `data/database/hecm/`

### 3. Validate Data Quality

Run validation checks to ensure data integrity:

```bash
python -m fha_data_manager.validation.validators
```

Or programmatically:

```python
from fha_data_manager.validation import FHADataValidator

validator = FHADataValidator("data/database/single_family")
validator.load_data()
validator.run_all()
validator.print_summary()
```

**What it checks**:
- Schema compliance
- Data completeness
- ID-name consistency
- Relationship patterns
- Data ranges

See [Validation Guide](validation.md) for details.

### 4. Analyze Data

#### Exploratory Analysis

Run exploratory analysis with visualizations:

```bash
python -m fha_data_manager.analysis.exploratory
```

Or:

```python
from fha_data_manager.analysis import (
    load_combined_data,
    analyze_lender_activity,
    analyze_sponsor_activity,
    analyze_loan_characteristics,
)

df = load_combined_data("data/database/single_family")
lender_stats = analyze_lender_activity(df)
sponsor_stats = analyze_sponsor_activity(df)
loan_stats = analyze_loan_characteristics(df)
```

#### Institution Analysis

Analyze institution identities and mappings:

```bash
python -m fha_data_manager.analysis.institutions
```

Or:

```python
from fha_data_manager.analysis.institutions import InstitutionAnalyzer

analyzer = InstitutionAnalyzer("data/database/single_family")
analyzer.load_data()
analyzer.generate_full_report(output_dir="output")
```

## Data Structure

### Hive-Partitioned Database

The processed data is stored in a hive-partitioned structure for efficient querying:

```
data/database/
├── single_family/
│   ├── Year=2010/
│   │   ├── Month=5/
│   │   │   └── data.parquet
│   │   ├── Month=6/
│   │   │   └── data.parquet
│   │   └── ...
│   ├── Year=2011/
│   └── ...
└── hecm/
    └── (similar structure)
```

### Loading Data

Load all data:
```python
import polars as pl
df = pl.scan_parquet("data/database/single_family")
```

Load specific year/month:
```python
df = pl.scan_parquet("data/database/single_family/Year=2025/Month=6")
```

Filter efficiently:
```python
df = (
    pl.scan_parquet("data/database/single_family")
    .filter(pl.col("Year") >= 2020)
    .filter(pl.col("Property State") == "CA")
    .collect()
)
```

## Configuration

Override default paths using environment variables (create a `.env` file):

```
PROJECT_DIR=/path/to/your/project
DATA_DIR=/path/to/data
RAW_DIR=/path/to/raw/data
CLEAN_DIR=/path/to/clean/data
DATABASE_DIR=/path/to/database
OUTPUT_DIR=/path/to/output
```

Or in code:

```python
from fha_data_manager.utils.config import Config

# Access configured paths
print(Config.DATA_DIR)
print(Config.DATABASE_DIR)

# Ensure directories exist
Config.ensure_directories()
```

## Best Practices

### 1. Incremental Updates

When new monthly data is released:
```bash
# Download only new snapshots
python download_fha_data.py

# Import only new files
python import_fha_data.py
```

The import script automatically detects and processes only new files.

### 2. Data Validation

Always validate after importing:
```bash
python -m fha_data_manager.validation.validators --critical-only
```

Review any failures before proceeding with analysis.

### 3. Track Data Inventory

Log your current data inventory:
```bash
python -m fha_data_manager.utils.inventory
```

This creates `data/data_inventory.csv` with metadata about all files.

### 4. Memory Management

For large datasets, use lazy evaluation:
```python
import polars as pl

# Don't collect immediately
df = pl.scan_parquet("data/database/single_family")

# Filter before collecting
result = (
    df
    .filter(pl.col("Year") == 2025)
    .group_by("Property State")
    .agg(pl.col("FHA_Index").count())
    .collect()  # Only collect final result
)
```

## Troubleshooting

### Common Issues

**Issue**: Download fails
- **Solution**: Check internet connection and FHA website availability

**Issue**: Import fails with encoding errors
- **Solution**: Ensure input files are properly formatted Excel/CSV files

**Issue**: Validation shows high % missing IDs
- **Solution**: This is normal for certain time periods; review historical context

**Issue**: Out of memory during analysis
- **Solution**: Use lazy evaluation with `scan_parquet` instead of `read_parquet`

## Next Steps

- See [Validation Guide](validation.md) for detailed validation checks
- See [Analysis Guide](analysis.md) for analysis examples
- Check [examples/](../../examples/) for complete workflow examples
- Review [Data Schemas](../schemas/data_dictionaries.md) for column definitions

