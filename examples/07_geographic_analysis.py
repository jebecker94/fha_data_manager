"""
Example 7: Geographic Analysis with Maps

This script demonstrates how to create choropleth maps showing loan counts
by state and county using the FHA data.
"""

from pathlib import Path

import polars as pl
from fha_data_manager.analysis.geo import (
    create_county_loan_count_choropleth,
    create_state_loan_count_choropleth,
)

import requests


def create_state_maps() -> None:
    """Create state-level loan count maps."""
    print("\n" + "=" * 80)
    print("CREATING STATE-LEVEL LOAN COUNT MAPS")
    print("=" * 80)
    
    # Load data
    print("\nLoading single family data...")
    df = pl.scan_parquet("data/silver/single_family")
    
    # Filter for recent years to reduce computation time
    print("\nFiltering data for year >= 2020...")
    df = df.filter(pl.col("Year") >= 2020)
    
    # Collect the data
    df = df.collect()
    
    # Create overall state map
    print("\nCreating overall state loan count map...")
    fig = create_state_loan_count_choropleth(
        df,
        title="FHA Single Family Loans by State (2020-2024)",
        color_scale='Viridis',
    )
    
    # Save to output
    output_path = Path("output") / "state_loan_counts.html"
    output_path.parent.mkdir(exist_ok=True)
    fig.write_html(str(output_path))
    print(f"\n✓ Saved to {output_path}")
    
    # Create maps by year
    print("\nCreating state maps by year...")
    for year in range(2020, 2025+1):
        df_year = df.filter(pl.col("Year") == year)
        fig = create_state_loan_count_choropleth(
            df_year,
            title=f"FHA Single Family Loans by State ({year})",
            color_scale='Viridis',
        )
        
        output_path = Path("output") / f"state_loan_counts_{year}.html"
        fig.write_html(str(output_path))
        print(f"  ✓ Saved to {output_path}")


def create_county_maps() -> None:
    """Create county-level loan count maps."""
    print("\n" + "=" * 80)
    print("CREATING COUNTY-LEVEL LOAN COUNT MAPS")
    print("=" * 80)
    
    # Load data
    print("\nLoading single family data...")
    df = pl.scan_parquet("data/silver/single_family")
    
    # Filter for recent years to reduce computation time
    print("\nFiltering data for year >= 2020...")
    df = df.filter(pl.col("Year") >= 2020)
    
    # Collect the data
    df = df.collect()
    
    # Check if FIPS column exists
    if "FIPS" not in df.columns:
        print("\n⚠ Warning: FIPS column not found in data.")
        print("  County maps require FIPS codes. Skipping county maps.")
        return
    
    # Create overall county map
    print("\nCreating overall county loan count map...")
    counties_geojson = requests.get("https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json").json()
    fig = create_county_loan_count_choropleth(
        df,
        title="FHA Single Family Loans by County (2020-2025)",
        geojson = counties_geojson,
        color_scale='Viridis',
    )
    
    # Save to output
    output_path = Path("output") / "county_loan_counts.html"
    output_path.parent.mkdir(exist_ok=True)
    fig.write_html(str(output_path))
    print(f"\n✓ Saved to {output_path}")
    
    # Create maps by year
    print("\nCreating county maps by year...")
    for year in range(2020, 2025+1):
        df_year = df.filter(pl.col("Year") == year)
        fig = create_county_loan_count_choropleth(
            df_year,
            title=f"FHA Single Family Loans by County ({year})",
            geojson = counties_geojson,
            color_scale='Viridis',
        )
        
        output_path = Path("output") / f"county_loan_counts_{year}.html"
        fig.write_html(str(output_path))
        print(f"  ✓ Saved to {output_path}")


def main() -> None:
    """Run geographic analysis examples."""
    print("=" * 80)
    print("FHA DATA GEOGRAPHIC ANALYSIS")
    print("=" * 80)
    
    # Create state-level maps
    create_state_maps()
    
    # Create county-level maps
    create_county_maps()
    
    print("\n" + "=" * 80)
    print("Geographic analysis complete!")
    print("=" * 80)
    print("\nCheck the output/ directory for maps:")
    print("  - state_loan_counts.html")
    print("  - state_loan_counts_YYYY.html (by year)")
    print("  - county_loan_counts.html")
    print("  - county_loan_counts_YYYY.html (by year)")


if __name__ == "__main__":
    main()
