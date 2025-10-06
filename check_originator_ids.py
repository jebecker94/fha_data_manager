import logging
from pathlib import Path

import polars as pl


logger = logging.getLogger(__name__)

def analyze_lender_relationships(data_path: str | Path) -> None:
    """
    Analyze relationships between originators and sponsors in FHA data.
    
    Args:
        data_path: Path to the parquet file containing FHA data
    """
    # Load the data
    logger.info("Loading data from %s...", data_path)
    df = pl.scan_parquet(data_path)
    
    # Basic originator analysis
    logger.info("\n=== Basic Originator Statistics ===")
    originator_stats = (
        df.select([
            pl.col("Originating Mortgagee"),
            pl.col("Originating Mortgagee Number"),
            pl.col("Sponsor Name"),
            pl.col("Sponsor Number")
        ])
        .collect()
    )
    
    total_loans = len(originator_stats)
    missing_orig_id = originator_stats.filter(pl.col("Originating Mortgagee Number").is_null()).height
    missing_orig_name = originator_stats.filter(pl.col("Originating Mortgagee").is_null()).height
    has_sponsor = originator_stats.filter(pl.col("Sponsor Name").is_not_null()).height
    
    logger.info("Total loans: %s", f"{total_loans:,}")
    logger.info(
        "Loans missing originator ID: %s (%0.1f%%)",
        f"{missing_orig_id:,}",
        missing_orig_id / total_loans * 100,
    )
    logger.info(
        "Loans missing originator name: %s (%0.1f%%)",
        f"{missing_orig_name:,}",
        missing_orig_name / total_loans * 100,
    )
    logger.info(
        "Loans with sponsors: %s (%0.1f%%)",
        f"{has_sponsor:,}",
        has_sponsor / total_loans * 100,
    )
    
    # Analyze sponsor presence when originator ID is missing
    logger.info("\n=== Sponsor Presence Analysis ===")
    missing_id_analysis = (
        originator_stats
        .filter(pl.col("Originating Mortgagee Number").is_null())
        .group_by([
            pl.col("Sponsor Name").is_not_null().alias("has_sponsor_name"),
            pl.col("Sponsor Number").is_not_null().alias("has_sponsor_id")
        ])
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    logger.info("\nBreakdown of loans with missing originator ID:")
    logger.info("\n%s", missing_id_analysis)
    
    # Analyze most common sponsor-originator relationships
    logger.info("\n=== Top Sponsor-Originator Relationships ===")
    relationship_analysis = (
        df.group_by([
            "Originating Mortgagee",
            "Originating Mortgagee Number",
            "Sponsor Name",
            "Sponsor Number"
        ])
        .agg(pl.len().alias("loan_count"))
        .sort("loan_count", descending=True)
        .collect()
        .with_columns([
            (pl.col("loan_count") / total_loans * 100).alias("percent_of_total")
        ])
    )
    
    logger.info("\nTop 10 relationships by loan count:")
    logger.info("\n%s", relationship_analysis.head(10))
    
    # Analyze patterns for originators with missing IDs
    logger.info("\n=== Analysis of Originators with Missing IDs ===")
    missing_id_detail = (
        df.filter(pl.col("Originating Mortgagee Number").is_null())
        .group_by(["Originating Mortgagee", "Sponsor Name", "Sponsor Number"])
        .agg(pl.len().alias("loan_count"))
        .sort("loan_count", descending=True)
        .collect()
    )
    
    logger.info("\nTop 10 originators with missing IDs:")
    logger.info("\n%s", missing_id_detail.head(10))
    
    # Analyze unique relationships
    logger.info("\n=== Relationship Pattern Analysis ===")
    originator_sponsor_patterns = (
        df.group_by("Originating Mortgagee")
        .agg([
            pl.col("Originating Mortgagee Number").unique().len().alias("unique_orig_ids"),
            pl.col("Sponsor Name").unique().len().alias("unique_sponsors"),
            pl.col("Sponsor Number").unique().len().alias("unique_sponsor_ids"),
            pl.len().alias("total_loans")
        ])
        .sort("total_loans", descending=True)
        .collect()
    )
    
    # Filter for interesting patterns
    multiple_relationships = originator_sponsor_patterns.filter(
        (pl.col("unique_orig_ids") > 1) | (pl.col("unique_sponsors") > 1)
    ).sort("total_loans", descending=True)
    
    logger.info("\nOriginators with multiple relationships (top 10):")
    logger.info("\n%s", multiple_relationships.head(10))

def main():
    # Load data from hive structure
    data_path = "data/database/single_family"
    analyze_lender_relationships(data_path)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()