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


def analyze_refinance_share(df: pl.DataFrame) -> pl.DataFrame:
    """
    Analyze the share of refinanced loans over time.

    The output should be a DataFrame with the following columns:
    - Date
    - purchase_loan_count
    - refinance_loan_count
    - refinance_share
    """

    # Check for Date column and create it if it doesn't exist
    if 'Date' not in df.columns:
        df = df.with_columns(
            pl.concat_str([
                pl.col('Year').cast(pl.Utf8).str.zfill(4),
                pl.col('Month').cast(pl.Utf8).str.zfill(2),
            ], separator='-').str.to_datetime(format='%Y-%m', strict=False).alias('Date')
        )

    # Group by year and month and calculate the share of refinance loans
    df = df.group_by(['Date']).agg(
        pl.col('Loan Purpose').fill_null('').str.to_lowercase().str.contains('purchase').sum().alias('purchase_loan_count'),
        pl.col('Loan Purpose').fill_null('').str.to_lowercase().str.contains('refi').sum().alias('refinance_loan_count'),
    )
    df = df.with_columns(
        (pl.col('refinance_loan_count') / (pl.col('purchase_loan_count') + pl.col('refinance_loan_count'))).alias('refinance_share'),
    )
    return df


def analyze_fixed_rate_share(df: pl.DataFrame) -> pl.DataFrame :
    """
    Analyze the share of fixed rate loans over time.

    The output should be a DataFrame with the following columns:
    - Date
    - fixed_rate_count
    - adjustable_rate_count
    - adjustable_rate_share
    - fixed_rate_average
    - adjustable_rate_average
    """

    # Check for Date column and create it if it doesn't exist
    if 'Date' not in df.columns:
        df = df.with_columns(
            pl.concat_str([
                pl.col('Year').cast(pl.Utf8).str.zfill(4),
                pl.col('Month').cast(pl.Utf8).str.zfill(2),
            ], separator='-').str.to_datetime(format='%Y-%m', strict=False).alias('Date')
        )

    # Group by year and month and calculate the share of refinance loans
    df = df.group_by(['Date']).agg(
        pl.col('Product Type').fill_null('').str.to_lowercase().str.contains('fixed').sum().alias('fixed_rate_count'),
        pl.col('Product Type').fill_null('').str.to_lowercase().str.contains('adjustable').sum().alias('adjustable_rate_count'),
    )

    # Compute average rates

    df = df.with_columns(
        (pl.col('adjustable_rate_count') / (pl.col('adjustable_rate_count') + pl.col('fixed_rate_count'))).alias('adjustable_rate_share'),
    )
    return df


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


def plot_active_lenders_over_time(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot the number of active FHA lenders over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating active lenders over time plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Originating Mortgagee', 'Year'])
    df_temp = df_temp.group_by('Year').agg([
        pl.col('Originating Mortgagee').n_unique().alias('Active Lenders')
    ])
    df_temp = df_temp.collect().sort('Year')
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_temp['Year'], df_temp['Active Lenders'], marker='o', linewidth=2)
    plt.title('Number of Active FHA Lenders by Year')
    plt.xlabel('Year')
    plt.ylabel('Number of Active Lenders')
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'active_lenders_trend.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_average_loan_size_over_time(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot the average FHA loan size over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating average loan size over time plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Mortgage Amount', 'Year'])
    df_temp = df_temp.group_by('Year').agg([
        pl.col('Mortgage Amount').mean().alias('mean'),
        pl.col('Mortgage Amount').median().alias('median')
    ])
    df_temp = df_temp.collect().sort('Year')
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_temp['Year'], df_temp['mean'], marker='o', linewidth=2, label='Mean')
    plt.plot(df_temp['Year'], df_temp['median'], marker='s', linewidth=2, label='Median')
    plt.title('Average FHA Loan Size by Year')
    plt.xlabel('Year')
    plt.ylabel('Average Loan Amount ($)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'avg_loan_size_trend.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_loan_purpose_distribution(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot the distribution of FHA loans by purpose.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating loan purpose distribution plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Loan Purpose'])
    df_temp = df_temp.group_by('Loan Purpose').agg([
        pl.len().alias('count')
    ])
    df_temp = df_temp.collect().sort('count', descending=True)
    
    plt.figure(figsize=(10, 6))
    plt.bar(df_temp['Loan Purpose'], df_temp['count'])
    plt.title('Distribution of FHA Loans by Purpose')
    plt.ylabel('Number of Loans')
    plt.xlabel('Loan Purpose')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'loan_purpose_dist.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_purchase_and_refinance_trend(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot purchase and refinance loan trends over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating purchase and refinance trend plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Loan Purpose', 'Date'])
    df_temp = df_temp.with_columns([
        pl.when(pl.col('Loan Purpose').str.contains('Purchase'))
        .then(pl.lit('Purchase'))
        .otherwise(pl.lit('Refinance'))
        .alias('loan_category')
    ])
    df_temp = df_temp.group_by(['Date', 'loan_category']).agg([
        pl.len().alias('loan_count')
    ])
    df_temp = df_temp.collect().sort('Date')
    
    # Pivot to get separate columns for purchase and refinance
    purchase_data = df_temp.filter(pl.col('loan_category') == 'Purchase').sort('Date')
    refinance_data = df_temp.filter(pl.col('loan_category') == 'Refinance').sort('Date')
    
    plt.figure(figsize=(12, 6))
    
    if purchase_data.height > 0:
        plt.plot(purchase_data['Date'], purchase_data['loan_count'], 
                marker='o', linewidth=2, label='Purchase Loans')
    
    if refinance_data.height > 0:
        plt.plot(refinance_data['Date'], refinance_data['loan_count'], 
                marker='s', linewidth=2, label='Refinance Loans')
    
    plt.title('Purchase and Refinance Loans by Year')
    plt.xlabel('Date')
    plt.ylabel('Number of Loans')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'purchase_and_refinance_trend.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_down_payment_source_trend(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot the trend of borrower-funded loans over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating down payment source trend plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Down Payment Source', 'Date'])
    df_temp = df_temp.with_columns(
        pl.col('Date').count().over('Date').alias('LoanCount'),
        (pl.col('Down Payment Source') == 'Borrower').sum().over('Date').alias('BorrowerFundedCount')
    )
    df_temp = df_temp.with_columns(
        (pl.col('BorrowerFundedCount') / pl.col('LoanCount')).alias('BorrowerFundedShare')
    )
    df_temp = df_temp.select(['Date', 'BorrowerFundedShare']).unique()
    df_temp = df_temp.collect().sort('Date')
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_temp['Date'], df_temp['BorrowerFundedShare'])
    plt.title('Share of Borrower-Funded Down Payments Over Time')
    plt.xlabel('Date')
    plt.ylabel('Borrower-Funded Share')
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'down_payment_source_trend.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_interest_rate_by_product_type(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot average interest rates by product type over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating interest rate by product type plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Product Type', 'Interest Rate', 'Date'])
    df_temp = df_temp.with_columns(
        pl.col('Interest Rate').mean().over(['Date', 'Product Type']).alias('AverageRate')
    )
    df_temp = df_temp.select(['Date', 'Product Type', 'AverageRate']).unique()
    df_temp = df_temp.collect().sort(['Date', 'Product Type'])
    
    plt.figure(figsize=(12, 6))
    
    # Plot Fixed Rate
    fixed_data = df_temp.filter(pl.col('Product Type').str.contains('Fix'))
    if fixed_data.height > 0:
        plt.plot(fixed_data['Date'], fixed_data['AverageRate'], 
                color='blue', label='Fixed Rate', linewidth=2)
    
    # Plot Adjustable Rate
    adj_data = df_temp.filter(pl.col('Product Type').str.contains('Adj'))
    if adj_data.height > 0:
        plt.plot(adj_data['Date'], adj_data['AverageRate'], 
                color='red', label='Adjustable Rate', linewidth=2)
    
    plt.xlabel('Origination Date')
    plt.ylabel('Monthly Average Rate')
    plt.title('Average Interest Rates by Product Type')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'interest_rate_by_product_type.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_interest_rate_by_property_type(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot average interest rates by property type over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating interest rate by property type plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Property Type', 'Interest Rate', 'Date'])
    df_temp = df_temp.with_columns(
        pl.when(~pl.col('Property Type').str.contains('Single'))
        .then(pl.lit('Non single family'))
        .otherwise(pl.col('Property Type'))
        .alias('Property Type')
    )
    df_temp = df_temp.with_columns(
        pl.col('Interest Rate').mean().over(['Date', 'Property Type']).alias('AverageRate')
    )
    df_temp = df_temp.select(['Date', 'Property Type', 'AverageRate']).unique()
    df_temp = df_temp.collect().sort(['Date', 'Property Type'])
    
    plt.figure(figsize=(12, 6))
    
    # Plot Single Family
    single_data = df_temp.filter(pl.col('Property Type').str.contains('Single'))
    if single_data.height > 0:
        plt.plot(single_data['Date'], single_data['AverageRate'], 
                color='blue', label='Single family', linewidth=2)
    
    # Plot Non-Single Family
    non_single_data = df_temp.filter(~pl.col('Property Type').str.contains('Single'))
    if non_single_data.height > 0:
        plt.plot(non_single_data['Date'], non_single_data['AverageRate'], 
                color='red', label='Not single family', linewidth=2)
    
    plt.xlabel('Origination Date')
    plt.ylabel('Monthly Average Rate')
    plt.title('Average Interest Rates by Property Type')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'interest_rate_by_property_type.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_interest_rate_by_loan_purpose(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot average interest rates by loan purpose over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating interest rate by loan purpose plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Loan Purpose', 'Interest Rate', 'Date'])
    df_temp = df_temp.with_columns(
        pl.col('Interest Rate').mean().over(['Date', 'Loan Purpose']).alias('AverageRate')
    )
    df_temp = df_temp.select(['Date', 'Loan Purpose', 'AverageRate']).unique()
    df_temp = df_temp.collect().sort(['Date', 'Loan Purpose'])
    
    plt.figure(figsize=(12, 6))
    
    # Plot Purchase
    purchase_data = df_temp.filter(pl.col('Loan Purpose').str.contains('Purchase'))
    if purchase_data.height > 0:
        plt.plot(purchase_data['Date'], purchase_data['AverageRate'], 
                color='blue', label='Purchase', linewidth=2)
    
    # Plot FHA Refinance
    fha_refi_data = df_temp.filter(pl.col('Loan Purpose').str.contains('FHA'))
    if fha_refi_data.height > 0:
        plt.plot(fha_refi_data['Date'], fha_refi_data['AverageRate'], 
                color='red', label='Refinance (FHA)', linewidth=2)
    
    # Plot Conventional Refinance
    conv_refi_data = df_temp.filter(pl.col('Loan Purpose').str.contains('Conv'))
    if conv_refi_data.height > 0:
        plt.plot(conv_refi_data['Date'], conv_refi_data['AverageRate'], 
                color='green', label='Refinance (Conv)', linewidth=2)
    
    plt.xlabel('Origination Date')
    plt.ylabel('Monthly Average Rate')
    plt.title('Average Interest Rates by Loan Purpose')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'interest_rate_by_loan_purpose.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def plot_loan_amount_by_loan_purpose(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Plot average loan amounts by loan purpose over time.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plot
    """
    logger.info("Creating loan amount by loan purpose plot...")
    
    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(['Loan Purpose', 'Mortgage Amount', 'Date'])
    df_temp = df_temp.with_columns(
        pl.col('Mortgage Amount').mean().over(['Date', 'Loan Purpose']).alias('AverageSize')
    )
    df_temp = df_temp.select(['Date', 'Loan Purpose', 'AverageSize']).unique()
    df_temp = df_temp.collect().sort(['Date', 'Loan Purpose'])
    
    plt.figure(figsize=(12, 6))
    
    # Plot Purchase
    purchase_data = df_temp.filter(pl.col('Loan Purpose').str.contains('Purchase'))
    if purchase_data.height > 0:
        plt.plot(purchase_data['Date'], purchase_data['AverageSize'], 
                color='blue', label='Purchase', linewidth=2)
    
    # Plot FHA Refinance
    fha_refi_data = df_temp.filter(pl.col('Loan Purpose').str.contains('FHA'))
    if fha_refi_data.height > 0:
        plt.plot(fha_refi_data['Date'], fha_refi_data['AverageSize'], 
                color='red', label='Refinance (FHA)', linewidth=2)
    
    # Plot Conventional Refinance
    conv_refi_data = df_temp.filter(pl.col('Loan Purpose').str.contains('Conv'))
    if conv_refi_data.height > 0:
        plt.plot(conv_refi_data['Date'], conv_refi_data['AverageSize'], 
                color='green', label='Refinance (Conv)', linewidth=2)
    
    plt.xlabel('Origination Date')
    plt.ylabel('Monthly Average Loan Amount')
    plt.title('Average Loan Amounts by Loan Purpose')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    output_path = Path(output_dir) / 'loan_amount_by_loan_purpose.png'
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved plot to %s", output_path)


def create_all_trend_plots(data_path: Union[str, Path], output_dir: Union[str, Path] = "output") -> None:
    """
    Create all trend plots for FHA data analysis.
    
    Args:
        data_path: Path to the hive-structured parquet directory
        output_dir: Directory to save the plots
    """
    logger.info("Creating all trend plots...")
    
    # Ensure output directory exists
    Path(output_dir).mkdir(exist_ok=True)
    
    # Create all plots
    plot_active_lenders_over_time(data_path, output_dir)
    plot_average_loan_size_over_time(data_path, output_dir)
    plot_loan_purpose_distribution(data_path, output_dir)
    plot_purchase_and_refinance_trend(data_path, output_dir)
    plot_down_payment_source_trend(data_path, output_dir)
    plot_interest_rate_by_product_type(data_path, output_dir)
    plot_interest_rate_by_property_type(data_path, output_dir)
    plot_interest_rate_by_loan_purpose(data_path, output_dir)
    plot_loan_amount_by_loan_purpose(data_path, output_dir)
    
    logger.info("All trend plots created successfully")


def main(log_level: str | int = "INFO", create_plots: bool = True, output_dir: Union[str, Path] = "output") -> None:
    """
    Run exploratory data analysis and optionally create visualizations.
    
    Args:
        log_level: Logging level for the analysis
        create_plots: Whether to create all trend plots
        output_dir: Directory to save plots if create_plots is True
    """
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
    
    # Create visualizations if requested
    if create_plots:
        logger.info("Creating all trend plots...")
        create_all_trend_plots(data_path, output_dir)
        logger.info("Analysis and visualization complete!")
    else:
        logger.info("Analysis complete! Use create_all_trend_plots() to generate visualizations.")


if __name__ == "__main__":
    main()

