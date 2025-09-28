# Import Packages
import glob
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

import polars as pl


logger = logging.getLogger(__name__)

# Define Function to Detect Name Oscillations
def detect_name_oscillations(data_path: str | Path) -> None:
    """
    Analyze cases where lender names oscillate between different values over time.
    Looks for patterns where names switch back to previous values rather than making
    clean transitions.
    
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
    
    # Analyze both originator and sponsor name changes
    for entity_type in ["Originator", "Sponsor"]:
        logger.info("\n=== Analyzing %s Name Oscillations ===", entity_type)
        
        # Select relevant columns based on entity type
        name_col = "Originating Mortgagee" if entity_type == "Originator" else "Sponsor Name"
        id_col = "Originating Mortgagee Number" if entity_type == "Originator" else "Sponsor Number"
        
        # Get name changes over time
        name_changes = (
            df.select([
                "period",
                name_col,
                id_col
            ])
            .filter(
                pl.col(id_col).is_not_null()
            )
            .group_by([
                "period",
                id_col
            ])
            .agg([
                pl.col(name_col).unique().alias("names_in_period")
            ])
            .sort([id_col, "period"])
            .collect()
        )
        
        # Track name sequences for each ID
        id_sequences = defaultdict(list)
        for row in name_changes.iter_rows(named=True):
            id_num = row[id_col]
            period = row["period"]
            names = tuple(sorted(row["names_in_period"]))  # Sort to ensure consistent comparison
            
            # Only add if different from previous
            if not id_sequences[id_num] or names != id_sequences[id_num][-1][1]:
                id_sequences[id_num].append((period, names))
        
        # Find oscillating patterns
        oscillating_ids = {}
        for id_num, sequence in id_sequences.items():
            if len(sequence) < 3:  # Need at least 3 changes to have a back-and-forth
                continue
                
            # Look for any name that appears multiple times non-consecutively
            seen_names = set()
            name_periods = defaultdict(list)
            for period, names in sequence:
                for name in names:
                    name_periods[name].append(period)
                    
            # Check for oscillations
            for name, periods in name_periods.items():
                if len(periods) < 2:
                    continue
                    
                # Check if there are other names between occurrences
                for i in range(len(periods) - 1):
                    current_period = periods[i]
                    next_period = periods[i + 1]
                    
                    # Find if there were different names between these periods
                    intermediate_names = set()
                    for p, names_tuple in sequence:
                        if current_period < p < next_period:
                            intermediate_names.update(names_tuple)
                            
                    if intermediate_names and name not in intermediate_names:
                        if id_num not in oscillating_ids:
                            oscillating_ids[id_num] = {
                                "name": name,
                                "periods": periods,
                                "intermediate_names": intermediate_names
                            }
                            break
        
        # Print results
        if oscillating_ids:
            logger.info(
                "\nFound %s %ss with oscillating names:",
                f"{len(oscillating_ids)}",
                entity_type,
            )
            for id_num, data in oscillating_ids.items():
                logger.info("\nID %s:", id_num)
                logger.info(
                    "Name '%s' appears in periods: %s",
                    data['name'],
                    ', '.join(data['periods']),
                )
                logger.info(
                    "With intermediate names: %s",
                    ', '.join(data['intermediate_names']),
                )
                
                # Get loan counts for context
                loan_counts = (
                    df.filter(pl.col(id_col) == id_num)
                    .group_by("period")
                    .agg([
                        pl.col(name_col).unique().alias("names"),
                        pl.count().alias("loan_count")
                    ])
                    .sort("period")
                    .collect()
                )
                
                logger.info("\nDetailed timeline:")
                for row in loan_counts.iter_rows(named=True):
                    if row["loan_count"] > 0:  # Only show periods with loans
                        logger.info(
                            "  %s: %s (%s loans)",
                            row['period'],
                            ', '.join(row['names']),
                            f"{row['loan_count']:,}",
                        )
        else:
            logger.info("No clear oscillating patterns found for %ss", entity_type)

def main():
    # Find the most recent combined data file
    files = glob.glob("data/fha_combined_sf_originations*.parquet")
    if not files:
        raise FileNotFoundError("No FHA combined data files found")
    
    data_path = files[0]
    detect_name_oscillations(data_path)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()