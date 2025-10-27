# FHA Data Manager - Examples

This directory contains example scripts demonstrating the complete workflow for managing FHA data.

## Workflow Overview

1. **Download** - Download raw FHA data snapshots
2. **Import** - Clean and import data into database
3. **Load & Query** - Load and query the processed data
4. **Validate** - Validate data quality
5. **Analyze** - Perform exploratory and institutional analysis
6. **Network Analysis** - Analyze sponsor-originator relationship networks
7. **Geographic Analysis** - Create choropleth maps showing loan counts by state and county

## Quick Start

Run the examples in order:

```bash
# 1. Download FHA data
python examples/01_download_data.py

# 2. Import and clean the data
python examples/02_import_and_clean.py

# 3. Load and query data
python examples/03_load_and_query.py

# 4. Validate data quality
python examples/04_validate_data.py

# 5. Analyze the data
python examples/05_analyze_data.py

# 6. Network analysis
python examples/06_network_analysis.py

# 7. Geographic analysis with maps
python examples/07_geographic_analysis.py
```

## Example Scripts

### 01_download_data.py
Downloads both Single Family and HECM snapshots from the FHA website.

### 02_import_and_clean.py
Imports and cleans the raw data, saving it to a hive-structured database.

### 03_load_and_query.py
Demonstrates how to load and query the processed data using Polars.

### 04_validate_data.py
Runs data quality validation checks on the database.

### 05_analyze_data.py
Performs exploratory and institutional analysis on the data.

### 06_network_analysis.py
Analyzes sponsor-originator relationship networks using graph theory and centrality metrics.

### 07_geographic_analysis.py
Creates choropleth maps showing FHA loan counts at the state and county levels, 
generating both overall and year-by-year visualizations.

## Notes

- Make sure you have set up your environment with `uv sync` before running examples
- The download step requires internet access
- Import/cleaning steps can take significant time for large datasets
- Output files are saved to the `output/` directory

