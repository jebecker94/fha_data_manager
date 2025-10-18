"""Interactive data browsing utilities for FHA single-family data."""

import logging
from typing import Union
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl


logger = logging.getLogger(__name__)


def browse_data(data_path: Union[str, Path] = "data/database/single_family") -> None:
    """
    Browse and tabulate categorical columns in FHA single-family data.
    
    This function loads data from the hive structure and provides summary
    statistics and visualizations for various categorical and numeric columns.
    
    Args:
        data_path: Path to the hive-structured parquet directory
    """
    logger.info("Loading data from %s...", data_path)
    
    # Load Data from Hive Structure
    df = pl.scan_parquet(str(data_path))

    # Tabulate Categorical Columns
    logger.info("\n" + "=" * 80)
    logger.info("CATEGORICAL COLUMN SUMMARIES")
    logger.info("=" * 80)
    
    for column in ["Loan Purpose", "Property Type", "Down Payment Source", 
                   "Product Type", "Year", "Month", 'Date']:
        counts = df.group_by(column).count().sort("count", descending=True)
        logger.info("\n%s:", column)
        logger.info("\n%s", counts.collect())

    # Tabulate FIPS, State, and County
    logger.info("\n" + "=" * 80)
    logger.info("GEOGRAPHIC SUMMARIES")
    logger.info("=" * 80)
    
    for column in ["FIPS", "Property State", "Property County"]:
        counts = df.group_by(column).count().sort("count", descending=True)
        logger.info("\n%s (top 20):", column)
        logger.info("\n%s", counts.limit(20).collect())

    # Count total missing FIPS codes
    missing_count = df.filter(pl.col('FIPS').is_null()).count().collect()
    logger.info("\nTotal records with missing FIPS codes:\n%s", missing_count)

    # Identify Non-missing state/county pairs that lack FIPS codes
    logger.info("\n" + "=" * 80)
    logger.info("MISSING FIPS CODES ANALYSIS")
    logger.info("=" * 80)
    logger.info("\nMost common state/county pairs with missing FIPS codes:")
    
    missing_fips = df.filter(pl.col('FIPS').is_null()).select(['Property State', 'Property County'])
    missing_fips = missing_fips.filter([
        ~pl.col('Property State').is_null(), 
        ~pl.col('Property County').is_null()
    ])
    missing_fips = missing_fips.filter([
        ~pl.col('Property State').is_in(['nan', 'None', '']), 
        ~pl.col('Property County').is_in(['nan', 'None', ''])
    ])
    missing_fips = missing_fips.group_by(["Property State", "Property County"]).count().sort("count", descending=True)
    logger.info("\n%s", missing_fips.limit(20).collect())

    # Miscellaneous Analysis
    logger.info("\n" + "=" * 80)
    logger.info("CALIFORNIA DEEP DIVE")
    logger.info("=" * 80)
    
    # Find Biggest Cities in California
    big_ca_cities = df.filter(pl.col("Property State") == "CA").group_by("Property City").count().sort("count", descending=True)
    logger.info("\nBiggest cities in California (top 20):")
    logger.info("\n%s", big_ca_cities.limit(20).collect())

    # Find Biggest Counties in California
    big_ca_counties = df.filter(pl.col("Property State") == "CA").group_by("Property County").count().sort("count", descending=True)
    logger.info("\nBiggest counties in California:")
    logger.info("\n%s", big_ca_counties.collect())

    # Display a Histogram of Rates
    logger.info("\n" + "=" * 80)
    logger.info("INTEREST RATE DISTRIBUTION")
    logger.info("=" * 80)
    
    rates = df.filter(
        pl.col("Interest Rate") >= 0, 
        pl.col("Interest Rate") <= 10
    ).select(pl.col("Interest Rate")).collect()
    
    logger.info("\nGenerating interest rate histogram...")
    # Note: The histogram generation is commented out since it might not be needed for browsing
    # rates_hist = rates.to_series().hist(bin_count=100)

    # Focus on Home Purchase Loans in California
    logger.info("\n" + "=" * 80)
    logger.info("CALIFORNIA PURCHASE LOANS TIME SERIES")
    logger.info("=" * 80)
    
    ca_purch = df.filter(
        pl.col('Loan Purpose').is_in(['Purchase']), 
        pl.col('Property State').is_in(['CA'])
    )

    # Compute the average loan size by month
    avg_size_ts = ca_purch.group_by(['Year', 'Month']).agg(
        pl.col('Mortgage Amount').mean()
    ).sort(['Year', 'Month']).collect()
    
    logger.info("\nAverage loan size by month (first 10 periods):")
    logger.info("\n%s", avg_size_ts.head(10))
    logger.info("\nAverage loan size by month (last 10 periods):")
    logger.info("\n%s", avg_size_ts.tail(10))

    # Graph the time series of average loan size
    logger.info("\nGenerating time series plot...")
    plt.figure(figsize=(12, 6))
    plt.plot(np.arange(len(avg_size_ts)), avg_size_ts['Mortgage Amount'])
    plt.xlabel('Month')
    plt.ylabel('Average Loan Size ($)')
    plt.title('Average California Purchase Loan Size by Month')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('output/ca_purchase_loan_trend.png')
    logger.info("Saved plot to: output/ca_purchase_loan_trend.png")
    plt.close()


def main() -> None:
    """Run the data browsing utility."""
    browse_data()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

