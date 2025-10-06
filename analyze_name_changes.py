import logging
from pathlib import Path
from typing import Dict, Set, Tuple

import pandas as pd
import polars as pl


logger = logging.getLogger(__name__)

def analyze_name_changes(data_path: str | Path) -> None:
    """
    Analyze how name-ID relationships change over time for originators and sponsors.
    
    Args:
        data_path: Path to the parquet file containing FHA data
    """
    logger.info("Loading data from %s...", data_path)
    df = pl.scan_parquet(data_path)
    
    # Create year-month field for temporal analysis
    df = df.with_columns(
        pl.concat_str([
            pl.col("Year").cast(pl.Utf8),
            pl.lit("-"),
            pl.col("Month").cast(pl.Utf8).str.zfill(2)
        ]).alias("period")
    )
    
    # Analyze specific cases (like Quicken/Rocket) first for visibility
    logger.info("\n=== Detailed Analysis of Notable Cases ===")
    
    # Analyze Quicken/Rocket transition
    logger.info("\nAnalyzing Quicken/Rocket transition:")
    quicken_analysis = (
        df.filter(
            pl.col("Originating Mortgagee Number") == 71970
        )
        .group_by("period")
        .agg([
            pl.col("Originating Mortgagee").unique().alias("names"),
            pl.count().alias("loan_count")
        ])
        .sort("period")
        .collect()
    )
    logger.info("\nName changes for ID 71970 (Quicken/Rocket):")
    for row in quicken_analysis.iter_rows(named=True):
        if any('QUICKEN' in name or 'ROCKET' in name for name in row['names']):
            logger.info(
                "%s: %s (%s loans)",
                row['period'],
                ', '.join(row['names']),
                f"{row['loan_count']:,}",
            )
    
    # Analyze Freedom Mortgage transition
    logger.info("\nAnalyzing Freedom Mortgage transition:")
    freedom_analysis = (
        df.filter(
            pl.col("Originating Mortgagee Number") == 75159
        )
        .group_by("period")
        .agg([
            pl.col("Originating Mortgagee").unique().alias("names"),
            pl.count().alias("loan_count")
        ])
        .sort("period")
        .collect()
    )
    logger.info("\nName changes for ID 75159 (Freedom):")
    for row in freedom_analysis.iter_rows(named=True):
        if any('FREEDOM' in name for name in row['names']):
            logger.info(
                "%s: %s (%s loans)",
                row['period'],
                ', '.join(row['names']),
                f"{row['loan_count']:,}",
            )
    
    # Analyze originator name changes
    logger.info("\n=== Analyzing Originator Name Changes Over Time ===")
    originator_changes = (
        df.select([
            "period",
            "Originating Mortgagee",
            "Originating Mortgagee Number"
        ])
        .filter(
            pl.col("Originating Mortgagee Number").is_not_null()
        )
        .group_by([
            "period",
            "Originating Mortgagee Number"
        ])
        .agg([
            pl.col("Originating Mortgagee").unique().alias("names_in_period")
        ])
        .sort(["Originating Mortgagee Number", "period"])
        .collect()
    )
    
    # Find IDs with multiple names
    id_name_changes = {}
    for row in originator_changes.iter_rows(named=True):
        id_num = row["Originating Mortgagee Number"]
        period = row["period"]
        names = row["names_in_period"]
        
        if len(names) > 1 or (id_num in id_name_changes and names[0] not in id_name_changes[id_num]["names"]):
            if id_num not in id_name_changes:
                id_name_changes[id_num] = {"names": set(), "transitions": []}
            
            current_names = set(names)
            if current_names != id_name_changes[id_num]["names"]:
                id_name_changes[id_num]["names"].update(current_names)
                id_name_changes[id_num]["transitions"].append((period, names))
    
    # Print significant name changes
    logger.info("\nTop 10 IDs with most name changes:")
    for id_num, data in sorted(id_name_changes.items(),
                             key=lambda x: len(x[1]["transitions"]),
                             reverse=True)[:10]:
        logger.info("\nID %s (%s changes):", id_num, len(data['transitions']))
        for period, names in data["transitions"]:
            logger.info("  %s: %s", period, ', '.join(names))
    
    # Analyze consistency of changes
    logger.info("\n=== Analyzing Change Patterns ===")
    for id_num in [71970, 75159]:  # Quicken and Freedom
        logger.info("\nAnalyzing change consistency for ID %s:", id_num)
        
        # Get all periods for this ID
        id_timeline = (
            df.filter(pl.col("Originating Mortgagee Number") == id_num)
            .select([
                "period",
                "Originating Mortgagee"
            ])
            .group_by("period")
            .agg([
                pl.col("Originating Mortgagee").unique().alias("names")
            ])
            .sort("period")
            .collect()
        )
        
        # Check for back-and-forth changes
        previous_names = set()
        changes = []
        for row in id_timeline.iter_rows(named=True):
            current_names = set(row["names"])
            if current_names != previous_names:
                changes.append((row["period"], current_names))
                previous_names = current_names
        
        logger.info("Found %s distinct name sets:", len(changes))
        for period, names in changes:
            logger.info("  %s: %s", period, ', '.join(names))

def main():
    # Load data from hive structure
    data_path = "data/database/single_family"
    analyze_name_changes(data_path)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()