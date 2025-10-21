"""Exploratory data analysis for FHA single-family data."""

import logging
from pathlib import Path
from typing import Dict, Literal, Union

import matplotlib.pyplot as plt
import polars as pl

from fha_data_manager.utils.logging import configure_logging

logger = logging.getLogger(__name__)


def load_combined_data(data_path: Union[str, Path]) -> pl.DataFrame:
    """
    Load the FHA single-family data from hive structure.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        
    Returns:
        DataFrame containing the combined data
    """
    logger.info("Loading data from %s...", data_path)
    # Load from hive structure using polars
    df = pl.scan_parquet(str(data_path)).collect()
    logger.info("Loaded %s records", f"{len(df):,}")
    return df


def analyze_lender_activity(df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
    """
    Analyze lender activity in the FHA single-family program.
    
    Args:
        df: DataFrame with FHA single-family data
    
    Returns:
        Dictionary of DataFrames with various lender metrics
    """
    results = {}
    
    # Top lenders by volume
    lender_volume = (
        df.group_by('Originating Mortgagee')
        .agg([
            pl.col('FHA_Index').count().alias('Loan Count'),
            pl.col('Mortgage Amount').sum().alias('Total Volume')
        ])
        .with_columns(
            (pl.col('Total Volume') / pl.col('Loan Count')).alias('Average Loan Size')
        )
        .sort('Loan Count', descending=True)
        .head(20)
    )
    results['lender_volume'] = lender_volume

    # Lender activity by year
    yearly_lenders = (
        df.group_by('Year')
        .agg([
            pl.col('Originating Mortgagee').n_unique().alias('Active Lenders'),
            pl.col('FHA_Index').count().alias('Total Loans')
        ])
        .with_columns(
            (pl.col('Total Loans') / pl.col('Active Lenders')).alias('Avg Loans per Lender')
        )
        .sort('Year')
    )
    results['yearly_lenders'] = yearly_lenders

    return results


Frequency = Literal["annual", "quarterly"]


def _normalize_frequency(frequency: str) -> Frequency:
    """Validate and normalize frequency input."""

    freq = frequency.lower()
    if freq in {"annual", "yearly", "year"}:
        return "annual"
    if freq in {"quarter", "quarterly"}:
        return "quarterly"
    msg = "frequency must be 'annual' or 'quarterly'"
    raise ValueError(msg)


def _prepare_period_columns(lf: pl.LazyFrame, frequency: Frequency) -> tuple[pl.LazyFrame, list[str]]:
    """Add period columns used for aggregations."""

    period_columns = ["Year"]
    if frequency == "quarterly":
        lf = lf.with_columns(
            ((pl.col("Month") - 1) // 3 + 1)
            .cast(pl.UInt8)
            .alias("Quarter")
        )
        period_columns.append("Quarter")
    return lf, period_columns


def build_lender_panel(
    df: pl.DataFrame | pl.LazyFrame,
    frequency: str = "annual",
    output_path: str | Path | None = None,
) -> pl.DataFrame:
    """Create a lender-level panel with annual or quarterly metrics.

    Args:
        df: FHA single-family dataset as a DataFrame or LazyFrame.
        frequency: Aggregation frequency (``"annual"`` or ``"quarterly"``).
        output_path: Optional path where the resulting panel will be written
            as a Parquet file.

    Returns:
        A ``pl.DataFrame`` containing lender-level aggregates suitable for
        econometric analysis.
    """

    freq = _normalize_frequency(frequency)
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df
    lf, period_columns = _prepare_period_columns(lf, freq)

    grouping_columns = period_columns + [
        "Originating Mortgagee Number",
        "Originating Mortgagee",
    ]

    aggregated = (
        lf.group_by(grouping_columns)
        .agg(
            [
                pl.len().alias("loan_count"),
                pl.col("Mortgage Amount").sum().alias("total_mortgage_amount"),
                pl.col("Interest Rate").mean().alias("avg_interest_rate"),
                pl.col("Loan Purpose")
                .fill_null("")
                .str.to_lowercase()
                .eq("purchase")
                .sum()
                .alias("purchase_loan_count"),
                pl.col("Loan Purpose")
                .fill_null("")
                .str.to_lowercase()
                .eq("refinance")
                .sum()
                .alias("refinance_loan_count"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("loan_count") > 0)
                .then(pl.col("total_mortgage_amount") / pl.col("loan_count"))
                .otherwise(None)
                .alias("avg_mortgage_amount"),
                pl.when(pl.col("loan_count") > 0)
                .then(pl.col("purchase_loan_count") / pl.col("loan_count"))
                .otherwise(None)
                .alias("purchase_share"),
                pl.when(pl.col("loan_count") > 0)
                .then(pl.col("refinance_loan_count") / pl.col("loan_count"))
                .otherwise(None)
                .alias("refinance_share"),
                pl.lit(freq).alias("frequency"),
            ]
        )
        .sort(grouping_columns)
    )

    result = aggregated.collect()

    if output_path:
        output_path = Path(output_path)
        if output_path.suffix:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.write_parquet(str(output_path))
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            file_name = f"lender_panel_{freq}.parquet"
            result.write_parquet(str(output_path / file_name))

    return result


def analyze_sponsor_activity(df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
    """
    Analyze sponsor activity in the FHA single-family program.
    
    Args:
        df: DataFrame with FHA single-family data
    
    Returns:
        Dictionary of DataFrames with various sponsor metrics
    """
    results = {}
    
    # Filter for sponsored loans
    sponsored_loans = df.filter(pl.col('Sponsor Name').is_not_null())
    
    # Top sponsors by volume
    sponsor_volume = (
        sponsored_loans.group_by('Sponsor Name')
        .agg([
            pl.col('FHA_Index').count().alias('Loan Count'),
            pl.col('Mortgage Amount').sum().alias('Total Volume')
        ])
        .with_columns(
            (pl.col('Total Volume') / pl.col('Loan Count')).alias('Average Loan Size')
        )
        .sort('Loan Count', descending=True)
        .head(20)
    )
    results['sponsor_volume'] = sponsor_volume

    # Sponsorship trends by year
    yearly_sponsors = (
        sponsored_loans.group_by('Year')
        .agg([
            pl.col('Sponsor Name').n_unique().alias('Active Sponsors'),
            pl.col('FHA_Index').count().alias('Sponsored Loans')
        ])
        .sort('Year')
    )
    results['yearly_sponsors'] = yearly_sponsors

    return results


def analyze_loan_characteristics(df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
    """
    Analyze loan characteristics in the FHA single-family program.
    
    Args:
        df: DataFrame with FHA single-family data
    
    Returns:
        Dictionary of DataFrames with various loan metrics
    """
    results = {}
    
    # Loan purpose distribution
    loan_purpose = (
        df.group_by('Loan Purpose')
        .agg(pl.count().alias('count'))
        .sort('count', descending=True)
    )
    results['loan_purpose'] = loan_purpose

    # Down payment source distribution
    down_payment = (
        df.group_by('Down Payment Source')
        .agg(pl.count().alias('count'))
        .sort('count', descending=True)
    )
    results['down_payment'] = down_payment

    # Average loan size by year
    yearly_loan_size = (
        df.group_by('Year')
        .agg([
            pl.col('Mortgage Amount').mean().alias('mean'),
            pl.col('Mortgage Amount').median().alias('median'),
            pl.col('Mortgage Amount').std().alias('std')
        ])
        .sort('Year')
    )
    results['yearly_loan_size'] = yearly_loan_size

    return results


def print_summary_statistics(stats_dict: Dict[str, pl.DataFrame], section: str) -> None:
    """
    Print formatted summary statistics.
    
    Args:
        stats_dict: Dictionary mapping stat names to DataFrames
        section: Name of the section to display
    """
    logger.info("\n%s\n%s\n%s", "=" * 80, section, "=" * 80)
    for name, df in stats_dict.items():
        logger.info("\n%s:", name.replace('_', ' ').title())
        logger.info("\n%s", df)


def main(log_level: str | int = "INFO") -> None:
    """Run exploratory data analysis and generate visualizations."""

    configure_logging(log_level)

    # Load the data from hive structure
    data_path = Path("data/database/single_family")
    df = load_combined_data(data_path)
    
    # Perform analyses
    lender_stats = analyze_lender_activity(df)
    sponsor_stats = analyze_sponsor_activity(df)
    loan_stats = analyze_loan_characteristics(df)
    
    # Print results
    print_summary_statistics(lender_stats, "Lender Activity Analysis")
    print_summary_statistics(sponsor_stats, "Sponsor Activity Analysis")
    print_summary_statistics(loan_stats, "Loan Characteristics Analysis")
    
    # Create some visualizations
    plt.style.use('seaborn-v0_8')
    
    # Plot 1: Active Lenders Over Time
    fig, ax = plt.subplots(figsize=(12, 6))
    yearly_lenders_df = lender_stats['yearly_lenders']
    ax.plot(yearly_lenders_df['Year'], yearly_lenders_df['Active Lenders'], marker='o')
    plt.title('Number of Active FHA Lenders by Year')
    plt.xlabel('Year')
    plt.ylabel('Number of Active Lenders')
    plt.grid(True)
    plt.savefig('output/active_lenders_trend.png')
    plt.close()
    
    # Plot 2: Average Loan Size Over Time
    fig, ax = plt.subplots(figsize=(12, 6))
    yearly_loan_size_df = loan_stats['yearly_loan_size']
    ax.plot(yearly_loan_size_df['Year'], yearly_loan_size_df['mean'], marker='o')
    plt.title('Average FHA Loan Size by Year')
    plt.xlabel('Year')
    plt.ylabel('Average Loan Amount ($)')
    plt.grid(True)
    plt.savefig('output/avg_loan_size_trend.png')
    plt.close()
    
    # Plot 3: Loan Purpose Distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    loan_purpose_df = loan_stats['loan_purpose']
    ax.bar(loan_purpose_df['Loan Purpose'], loan_purpose_df['count'])
    plt.title('Distribution of FHA Loans by Purpose')
    plt.ylabel('Number of Loans')
    plt.xlabel('Loan Purpose')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('output/loan_purpose_dist.png')
    plt.close()


if __name__ == "__main__":
    main()

