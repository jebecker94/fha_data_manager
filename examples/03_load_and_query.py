"""
Example 3: Load and Query FHA Data

This script demonstrates how to load and query the processed FHA data
using Polars with lazy evaluation for efficient data processing.
"""

from pathlib import Path
import polars as pl


def query_single_family_data() -> None:
    """Load and query Single Family data."""
    print("\n" + "=" * 80)
    print("SINGLE FAMILY DATA QUERY")
    print("=" * 80)
    
    # Read Data
    print("\nLoading Single Family data from hive structure...")
    df_sf = pl.scan_parquet("data/database/single_family", include_file_paths='FilePath')
    
    # Query Data - Example: June 2025 data
    print("\nFiltering for Year=2025, Month=6...")
    df_sf = df_sf.filter(pl.col('Year') == 2025)
    df_sf = df_sf.filter(pl.col('Month') == 6)
    df_sf = df_sf.collect()
    
    # Print Results
    print(f"\nFound {len(df_sf):,} records")
    print("\nFirst 10 rows:")
    print(df_sf.head(10))
    
    # Summary statistics
    print("\nSummary Statistics:")
    print(df_sf.select([
        pl.col('Mortgage Amount').mean().alias('Avg Mortgage'),
        pl.col('Mortgage Amount').median().alias('Median Mortgage'),
        pl.col('Interest Rate').mean().alias('Avg Interest Rate'),
    ]))


def query_hecm_data() -> None:
    """Load and query HECM data."""
    print("\n" + "=" * 80)
    print("HECM DATA QUERY")
    print("=" * 80)
    
    # Read Data
    print("\nLoading HECM data from hive structure...")
    df_hecm = pl.scan_parquet("data/database/hecm", include_file_paths='FilePath')
    
    # Query Data - Example: June 2025 data
    print("\nFiltering for Year=2025, Month=6...")
    df_hecm = df_hecm.filter(pl.col('Year') == 2025)
    df_hecm = df_hecm.filter(pl.col('Month') == 6)
    df_hecm = df_hecm.collect()
    
    # Print Results
    print(f"\nFound {len(df_hecm):,} records")
    print("\nFirst 10 rows:")
    print(df_hecm.head(10))


def main() -> None:
    """Run data loading and querying examples."""
    print("=" * 80)
    print("FHA DATA LOADING AND QUERYING EXAMPLES")
    print("=" * 80)
    
    # Query Single Family data
    query_single_family_data()
    
    # Query HECM data
    query_hecm_data()
    
    print("\n" + "=" * 80)
    print("Query complete!")
    print("=" * 80)
    print("\nNext step: Run 04_validate_data.py to check data quality")


if __name__ == "__main__":
    main()

