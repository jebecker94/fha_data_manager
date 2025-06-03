import polars as pl
import glob
from pathlib import Path
import pandas as pd

def analyze_lender_relationships(data_path: str | Path) -> None:
    """
    Analyze relationships between originators and sponsors in FHA data.
    
    Args:
        data_path: Path to the parquet file containing FHA data
    """
    # Load the data
    print(f"Loading data from {data_path}...")
    df = pl.scan_parquet(data_path)
    
    # Get unique values for each field separately
    orig_names = set(df.select("Originating Mortgagee").unique().collect()["Originating Mortgagee"])
    orig_ids = set(df.select("Originating Mortgagee Number").unique().collect()["Originating Mortgagee Number"])
    sponsor_names = set(df.select("Sponsor Name").unique().collect()["Sponsor Name"])
    sponsor_ids = set(df.select("Sponsor Number").unique().collect()["Sponsor Number"])
    
    # Remove None and NaN values
    orig_names = set(x for x in orig_names if x is not None and not pd.isna(x))
    orig_ids = set(x for x in orig_ids if x is not None and not pd.isna(x))
    sponsor_names = set(x for x in sponsor_names if x is not None and not pd.isna(x))
    sponsor_ids = set(x for x in sponsor_ids if x is not None and not pd.isna(x))
    
    # Print basic statistics
    print("\n=== Comparison of Identifier Sets ===")
    print(f"Unique originator names: {len(orig_names):,}")
    print(f"Unique originator IDs: {len(orig_ids):,}")
    print(f"Unique sponsor names: {len(sponsor_names):,}")
    print(f"Unique sponsor IDs: {len(sponsor_ids):,}")
    
    # Check for overlaps in names
    name_overlap = orig_names.intersection(sponsor_names)
    print(f"\nNames that appear as both originator and sponsor: {len(name_overlap):,}")
    if name_overlap:
        print("\nExample overlapping names (top 10):")
        for name in sorted(list(name_overlap))[:10]:
            print(f"- {name}")
    
    # Check for overlaps in IDs
    id_overlap = orig_ids.intersection(sponsor_ids)
    print(f"\nIDs that appear as both originator and sponsor: {len(id_overlap):,}")
    if id_overlap:
        print("\nExample overlapping IDs (top 10):")
        for id_num in sorted(list(id_overlap))[:10]:
            print(f"- {id_num}")
            
    # Analyze cases where same entity appears as both originator and sponsor
    print("\n=== Analysis of Matching ID Patterns ===")
    matching_patterns = (
        df.filter(
            (pl.col("Originating Mortgagee Number").is_not_null()) & 
            (pl.col("Sponsor Number").is_not_null())
        )
        .select([
            pl.col("Originating Mortgagee"),
            pl.col("Originating Mortgagee Number"),
            pl.col("Sponsor Name"),
            pl.col("Sponsor Number")
        ])
        .filter(
            (pl.col("Originating Mortgagee Number") == pl.col("Sponsor Number")) |
            (pl.col("Originating Mortgagee") == pl.col("Sponsor Name"))
        )
        .collect()
    )
    
    if len(matching_patterns) > 0:
        print(f"\nFound {len(matching_patterns):,} loans where originator and sponsor appear to be the same entity")
        print("\nExample matches (top 5):")
        print(matching_patterns.head(5))
    else:
        print("\nNo cases found where originator and sponsor are exactly the same entity")
    
    # Analyze ID number patterns
    print("\n=== ID Number Pattern Analysis ===")
    print("\nOriginator ID number statistics:")
    orig_id_stats = (
        df.select(pl.col("Originating Mortgagee Number"))
        .filter(pl.col("Originating Mortgagee Number").is_not_null())
        .select([
            pl.col("Originating Mortgagee Number").min().alias("min"),
            pl.col("Originating Mortgagee Number").max().alias("max"),
            pl.col("Originating Mortgagee Number").mean().alias("mean"),
            pl.col("Originating Mortgagee Number").std().alias("std")
        ])
        .collect()
    )
    print(orig_id_stats)
    
    print("\nSponsor ID number statistics:")
    sponsor_id_stats = (
        df.select(pl.col("Sponsor Number"))
        .filter(pl.col("Sponsor Number").is_not_null())
        .select([
            pl.col("Sponsor Number").min().alias("min"),
            pl.col("Sponsor Number").max().alias("max"),
            pl.col("Sponsor Number").mean().alias("mean"),
            pl.col("Sponsor Number").std().alias("std")
        ])
        .collect()
    )
    print(sponsor_id_stats)

def main():
    # Find the most recent combined data file
    files = glob.glob("data/fha_combined_sf_originations*.parquet")
    if not files:
        raise FileNotFoundError("No FHA combined data files found")
    
    data_path = files[0]
    analyze_lender_relationships(data_path)

if __name__ == "__main__":
    main() 