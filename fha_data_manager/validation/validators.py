"""
Data quality and consistency validation suite for FHA data.

This module provides a structured approach to validating FHA single-family data,
checking for common data quality issues, schema compliance, and consistency problems.
"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl

from fha_data_manager.utils.logging import configure_logging


logger = logging.getLogger(__name__)


class ValidationResult:
    """Store results of a validation check."""
    
    def __init__(self, name: str, passed: bool, details: Dict[str, Any], warning: bool = False):
        self.name = name
        self.passed = passed
        self.details = details
        self.warning = warning  # If True, failure is a warning not an error
    
    def __repr__(self):
        if self.warning and not self.passed:
            status = "[WARN]"
        else:
            status = "[PASS]" if self.passed else "[FAIL]"
        return f"{status}: {self.name}"


class FHADataValidator:
    """Validation suite for FHA single-family data."""
    
    def __init__(self, data_path: str | Path):
        self.data_path = Path(data_path)
        self.df = None
        self.results = []
    
    def load_data(self):
        """Load data from hive structure."""
        logger.info("Loading data from %s...", self.data_path)
        self.df = pl.scan_parquet(str(self.data_path))
        return self
    
    # --- Schema Validation ---
    
    def check_required_columns(self) -> ValidationResult:
        """Check that all required columns exist."""
        required = [
            "Year", "Month", "FHA_Index",
            "Originating Mortgagee", "Originating Mortgagee Number",
            "Sponsor Name", "Sponsor Number",
            "Property State", "Mortgage Amount"
        ]
        
        actual_columns = self.df.collect_schema().names()
        missing = [col for col in required if col not in actual_columns]
        
        passed = len(missing) == 0
        details = {
            "missing_columns": missing,
            "total_columns": len(actual_columns)
        }
        
        return ValidationResult("Required Columns Present", passed, details)
    
    def check_fha_index_uniqueness(self) -> ValidationResult:
        """Check that FHA_Index is unique within the dataset."""
        stats = (
            self.df
            .select([
                pl.len().alias("total_rows"),
                pl.col("FHA_Index").n_unique().alias("unique_indexes")
            ])
            .collect()
        )
        
        total = stats["total_rows"][0]
        unique = stats["unique_indexes"][0]
        passed = total == unique
        
        details = {
            "total_rows": total,
            "unique_indexes": unique,
            "duplicates": total - unique
        }
        
        return ValidationResult("FHA_Index Uniqueness", passed, details)
    
    # --- Data Completeness Checks ---
    
    def check_missing_originator_ids(self, threshold_pct: float = 5.0) -> ValidationResult:
        """Check for missing originator IDs."""
        stats = self.df.select([
            pl.col("Originating Mortgagee Number").is_null().sum().alias("missing"),
            pl.len().alias("total")
        ]).collect()
        
        missing = stats["missing"][0]
        total = stats["total"][0]
        pct = (missing / total) * 100
        
        passed = pct < threshold_pct
        details = {
            "missing_count": f"{missing:,}",
            "total_count": f"{total:,}",
            "percent_missing": round(pct, 2),
            "threshold_percent": threshold_pct
        }
        
        return ValidationResult("Missing Originator IDs Below Threshold", passed, details, warning=True)
    
    def check_missing_originator_names(self, threshold_pct: float = 5.0) -> ValidationResult:
        """Check for missing originator names."""
        stats = self.df.select([
            pl.col("Originating Mortgagee").is_null().sum().alias("missing"),
            pl.len().alias("total")
        ]).collect()
        
        missing = stats["missing"][0]
        total = stats["total"][0]
        pct = (missing / total) * 100
        
        passed = pct < threshold_pct
        details = {
            "missing_count": f"{missing:,}",
            "total_count": f"{total:,}",
            "percent_missing": round(pct, 2),
            "threshold_percent": threshold_pct
        }
        
        return ValidationResult("Missing Originator Names Below Threshold", passed, details, warning=True)
    
    def check_orphaned_sponsors(self) -> ValidationResult:
        """Check for loans with sponsor but missing originator ID."""
        stats = (
            self.df.filter(
                pl.col("Originating Mortgagee Number").is_null() &
                pl.col("Sponsor Number").is_not_null()
            )
            .select(pl.len().alias("orphaned"))
            .collect()
        )
        
        orphaned = stats["orphaned"][0]
        total = self.df.select(pl.len()).collect()[0, 0]
        pct = (orphaned / total) * 100 if total > 0 else 0
        
        # This is more of a warning - it's unusual but may be valid
        passed = orphaned == 0
        details = {
            "orphaned_count": f"{orphaned:,}",
            "percent_of_total": round(pct, 2)
        }
        
        return ValidationResult("No Orphaned Sponsors", passed, details, warning=True)
    
    def check_sponsor_coverage(self) -> ValidationResult:
        """Report on sponsor presence in the dataset."""
        stats = self.df.select([
            pl.col("Sponsor Name").is_not_null().sum().alias("has_sponsor"),
            pl.len().alias("total")
        ]).collect()
        
        has_sponsor = stats["has_sponsor"][0]
        total = stats["total"][0]
        pct = (has_sponsor / total) * 100
        
        # This is informational, not a pass/fail
        passed = True
        details = {
            "loans_with_sponsor": f"{has_sponsor:,}",
            "total_loans": f"{total:,}",
            "percent_with_sponsor": round(pct, 2)
        }
        
        return ValidationResult("Sponsor Coverage (Informational)", passed, details)
    
    # --- Identity Consistency Checks ---
    
    def check_id_name_consistency_monthly(self) -> ValidationResult:
        """Check for IDs mapping to multiple names in same month."""
        # Create period column
        df_with_period = self.df.with_columns(
            pl.concat_str([
                pl.col("Year").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("Month").cast(pl.Utf8).str.zfill(2)
            ]).alias("period")
        )
        
        # Check originators (exclude null IDs)
        inconsistencies = (
            df_with_period
            .filter(pl.col("Originating Mortgagee Number").is_not_null())
            .group_by(["Originating Mortgagee Number", "period"])
            .agg(pl.col("Originating Mortgagee").n_unique().alias("name_count"))
            .filter(pl.col("name_count") > 1)
            .collect()
        )
        
        count = len(inconsistencies)
        passed = count == 0
        
        sample = []
        if count > 0:
            # Get examples
            for row in inconsistencies.head(3).iter_rows(named=True):
                period = row["period"]
                id_num = row["Originating Mortgagee Number"]
                
                # Get the actual names
                names_result = (
                    df_with_period
                    .filter(
                        (pl.col("Originating Mortgagee Number") == pl.lit(id_num)) &
                        (pl.col("period") == period)
                    )
                    .select(pl.col("Originating Mortgagee").unique())
                    .collect()
                )
                names = names_result["Originating Mortgagee"].to_list()
                
                sample.append({
                    "period": period,
                    "id": id_num,
                    "names": names
                })
        
        details = {
            "inconsistent_periods": count,
            "sample": sample
        }
        
        return ValidationResult("ID-Name Consistency Within Months", passed, details)
    
    def check_overlapping_id_spaces(self) -> ValidationResult:
        """Check if originator and sponsor ID spaces overlap."""
        orig_ids = set(
            self.df.select("Originating Mortgagee Number")
            .unique()
            .collect()["Originating Mortgagee Number"]
        )
        sponsor_ids = set(
            self.df.select("Sponsor Number")
            .unique()
            .collect()["Sponsor Number"]
        )
        
        # Remove nulls
        orig_ids = {x for x in orig_ids if x is not None}
        sponsor_ids = {x for x in sponsor_ids if x is not None}
        
        overlap = orig_ids.intersection(sponsor_ids)
        
        # Some overlap may be expected (entities acting as both), but flag it
        passed = len(overlap) == 0
        details = {
            "unique_originator_ids": f"{len(orig_ids):,}",
            "unique_sponsor_ids": f"{len(sponsor_ids):,}",
            "overlapping_ids": len(overlap),
            "sample_overlapping_ids": sorted(list(overlap))[:10] if overlap else []
        }
        
        return ValidationResult("Non-overlapping ID Spaces", passed, details, warning=True)
    
    def check_name_oscillations(self, min_changes: int = 3) -> ValidationResult:
        """Check for institution names that oscillate between values."""
        df_with_period = self.df.with_columns(
            pl.concat_str([
                pl.col("Year").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("Month").cast(pl.Utf8).str.zfill(2)
            ]).alias("period")
        )
        
        # Get name changes over time
        name_changes = (
            df_with_period
            .group_by(["Originating Mortgagee Number", "period"])
            .agg(pl.col("Originating Mortgagee").unique().alias("names"))
            .sort(["Originating Mortgagee Number", "period"])
            .collect()
        )
        
        # Track sequences for each ID
        id_sequences = defaultdict(list)
        for row in name_changes.iter_rows(named=True):
            id_num = row["Originating Mortgagee Number"]
            if id_num is None:
                continue
            period = row["period"]
            names = tuple(sorted(row["names"]))  # Sort for consistent comparison
            
            # Only add if different from previous
            if not id_sequences[id_num] or names != id_sequences[id_num][-1][1]:
                id_sequences[id_num].append((period, names))
        
        # Find oscillating patterns
        oscillating_ids = []
        for id_num, sequence in id_sequences.items():
            if len(sequence) < min_changes:
                continue
            
            # Look for any name that appears multiple times non-consecutively
            name_periods = defaultdict(list)
            for period, names in sequence:
                for name in names:
                    name_periods[name].append(period)
            
            # Check for oscillations
            for name, periods in name_periods.items():
                if len(periods) >= 2:
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
                            oscillating_ids.append({
                                "id": id_num,
                                "oscillating_name": name,
                                "periods": periods,
                                "intermediate_names": list(intermediate_names)
                            })
                            break
                    break
        
        passed = len(oscillating_ids) == 0
        details = {
            "oscillating_ids_count": len(oscillating_ids),
            "min_changes_threshold": min_changes,
            "sample": oscillating_ids[:5]
        }
        
        return ValidationResult("Name Stability (No Oscillations)", passed, details, warning=True)
    
    # --- Relationship Checks ---
    
    def check_originator_sponsor_relationships(self) -> ValidationResult:
        """Analyze patterns in originator-sponsor relationships."""
        # Get stats on relationship patterns
        stats = (
            self.df
            .group_by("Originating Mortgagee")
            .agg([
                pl.col("Originating Mortgagee Number").n_unique().alias("unique_ids"),
                pl.col("Sponsor Name").n_unique().alias("unique_sponsors"),
                pl.len().alias("loan_count")
            ])
            .filter(pl.col("unique_ids") > 1)  # Originators with multiple IDs
            .sort("loan_count", descending=True)
            .collect()
        )
        
        problematic_count = len(stats)
        
        # This is a warning - some variation may be legitimate (mergers, etc.)
        passed = problematic_count == 0
        
        sample = []
        if problematic_count > 0:
            for row in stats.head(5).iter_rows(named=True):
                sample.append({
                    "originator": row["Originating Mortgagee"],
                    "unique_ids": row["unique_ids"],
                    "unique_sponsors": row["unique_sponsors"],
                    "loan_count": f"{row['loan_count']:,}"
                })
        
        details = {
            "originators_with_multiple_ids": problematic_count,
            "sample": sample
        }
        
        return ValidationResult("Consistent Originator-ID Mappings", passed, details, warning=True)
    
    # --- Data Range Checks ---
    
    def check_date_coverage(self) -> ValidationResult:
        """Check temporal coverage of the data."""
        stats = (
            self.df
            .select([
                pl.col("Year").min().alias("min_year"),
                pl.col("Year").max().alias("max_year"),
                pl.concat_str([
                    pl.col("Year").cast(pl.Utf8),
                    pl.lit("-"),
                    pl.col("Month").cast(pl.Utf8).str.zfill(2)
                ]).n_unique().alias("unique_periods")
            ])
            .collect()
        )
        
        min_year = stats["min_year"][0]
        max_year = stats["max_year"][0]
        unique_periods = stats["unique_periods"][0]
        year_span = max_year - min_year + 1
        
        # Informational check
        passed = True
        details = {
            "min_year": min_year,
            "max_year": max_year,
            "year_span": year_span,
            "unique_year_month_periods": unique_periods,
            "expected_periods_if_complete": year_span * 12
        }
        
        return ValidationResult("Date Coverage (Informational)", passed, details)
    
    def check_mortgage_amounts(self) -> ValidationResult:
        """Check for unreasonable mortgage amounts."""
        stats = (
            self.df
            .select([
                pl.col("Mortgage Amount").min().alias("min_amount"),
                pl.col("Mortgage Amount").max().alias("max_amount"),
                pl.col("Mortgage Amount").mean().alias("mean_amount"),
                (pl.col("Mortgage Amount") <= 0).sum().alias("non_positive"),
                (pl.col("Mortgage Amount") > 10000000).sum().alias("extremely_high"),
                pl.len().alias("total")
            ])
            .collect()
        )
        
        non_positive = stats["non_positive"][0]
        extremely_high = stats["extremely_high"][0]
        total = stats["total"][0]
        
        passed = non_positive == 0 and extremely_high == 0
        
        details = {
            "min_amount": f"${stats['min_amount'][0]:,.2f}",
            "max_amount": f"${stats['max_amount'][0]:,.2f}",
            "mean_amount": f"${stats['mean_amount'][0]:,.2f}",
            "non_positive_count": non_positive,
            "extremely_high_count": extremely_high,
            "total": f"{total:,}"
        }
        
        return ValidationResult("Reasonable Mortgage Amounts", passed, details, warning=True)
    
    # --- Run All Validations ---
    
    def run_all(self, include_warnings: bool = True) -> Dict[str, ValidationResult]:
        """Run all validation checks."""
        logger.info("Running all validations...")
        
        checks = [
            # Critical checks
            self.check_required_columns,
            self.check_fha_index_uniqueness,
            
            # Completeness checks (warnings)
            self.check_missing_originator_ids,
            self.check_missing_originator_names,
            self.check_orphaned_sponsors,
            self.check_sponsor_coverage,
            
            # Consistency checks
            self.check_id_name_consistency_monthly,
            self.check_overlapping_id_spaces,
            self.check_name_oscillations,
            self.check_originator_sponsor_relationships,
            
            # Data range checks
            self.check_date_coverage,
            self.check_mortgage_amounts,
        ]
        
        results = {}
        for check in checks:
            logger.info("Running: %s", check.__name__)
            result = check()
            results[check.__name__] = result
            self.results.append(result)
        
        return results
    
    def run_critical(self) -> Dict[str, ValidationResult]:
        """Run only critical validation checks (no warnings)."""
        logger.info("Running critical validations only...")
        
        checks = [
            self.check_required_columns,
            self.check_fha_index_uniqueness,
            self.check_id_name_consistency_monthly,
        ]
        
        results = {}
        for check in checks:
            logger.info("Running: %s", check.__name__)
            result = check()
            results[check.__name__] = result
            self.results.append(result)
        
        return results
    
    def print_summary(self):
        """Print summary of all validation results."""
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        
        passed_critical = 0
        failed_critical = 0
        warnings = 0
        
        for result in self.results:
            print(f"\n{result}")
            
            # Print details in a formatted way
            for key, value in result.details.items():
                if isinstance(value, list) and len(value) > 0:
                    print(f"  {key}:")
                    for item in value[:3]:  # Limit to first 3 items
                        print(f"    - {item}")
                    if len(value) > 3:
                        print(f"    ... and {len(value) - 3} more")
                else:
                    print(f"  {key}: {value}")
            
            # Count results
            if not result.passed:
                if result.warning:
                    warnings += 1
                else:
                    failed_critical += 1
            else:
                if not result.warning:
                    passed_critical += 1
        
        print("\n" + "=" * 80)
        total_critical = passed_critical + failed_critical
        print(f"CRITICAL CHECKS: {passed_critical}/{total_critical} passed")
        if warnings > 0:
            print(f"WARNINGS: {warnings}")
        print("=" * 80)
        
        return failed_critical == 0
    
    def export_results(self, output_path: str | Path):
        """Export validation results to CSV."""
        output_path = Path(output_path)
        
        # Create a flat structure for export
        export_data = []
        for result in self.results:
            row = {
                "check_name": result.name,
                "status": "PASS" if result.passed else ("WARNING" if result.warning else "FAIL"),
                "is_warning": result.warning,
            }
            # Add details as separate columns
            for key, value in result.details.items():
                if isinstance(value, (list, dict)):
                    row[key] = str(value)
                else:
                    row[key] = value
            export_data.append(row)
        
        df = pl.DataFrame(export_data)
        df.write_csv(output_path)
        logger.info("Validation results exported to %s", output_path)


# CLI interface
def main():
    """Run validation suite from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Validate FHA data quality and consistency"
    )
    parser.add_argument(
        "--data-path",
        default="data/database/single_family",
        help="Path to hive-structured data (default: data/database/single_family)"
    )
    parser.add_argument(
        "--critical-only",
        action="store_true",
        help="Run only critical checks (no warnings)"
    )
    parser.add_argument(
        "--export",
        type=str,
        help="Export results to CSV file"
    )
    parser.add_argument(
        "--checks",
        nargs="*",
        help="Specific checks to run (default: all)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help=(
            "Logging verbosity (default: %(default)s). Accepts standard level "
            "names or numeric values."
        ),
    )

    args = parser.parse_args()

    configure_logging(args.log_level)
    
    validator = FHADataValidator(args.data_path)
    validator.load_data()
    
    if args.checks:
        # Run specific checks
        for check_name in args.checks:
            check_method = getattr(validator, check_name, None)
            if check_method:
                result = check_method()
                print(result)
                for key, value in result.details.items():
                    print(f"  {key}: {value}")
            else:
                print(f"Unknown check: {check_name}")
    else:
        # Run all or critical checks
        if args.critical_only:
            validator.run_critical()
        else:
            validator.run_all()
        
        all_passed = validator.print_summary()
        
        if args.export:
            validator.export_results(args.export)
        
        # Exit with appropriate code
        import sys
        sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()

