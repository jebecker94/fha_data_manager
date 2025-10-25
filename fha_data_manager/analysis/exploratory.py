"""Exploratory data analysis for FHA single-family data."""

import logging
from pathlib import Path
from typing import Dict, Literal, Union
import polars as pl
import plotly.express as px

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


def plot_active_lenders_over_time(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create an interactive Plotly version of active lenders over time."""

    logger.info("Creating active lenders over time plot (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(["Originating Mortgagee", "Date"])
    df_temp = df_temp.group_by("Date").agg([
        pl.col("Originating Mortgagee").n_unique().alias("Active Lenders")
    ])
    df_temp = df_temp.collect().sort("Date")

    if df_temp.is_empty():
        logger.warning("No data available to plot active lenders (Plotly).")
        return

    pdf = df_temp.to_pandas()
    fig = px.line(
        pdf,
        x="Date",
        y="Active Lenders",
        markers=True,
        title="Number of Active FHA Lenders Over Time",
    )
    fig.update_layout(xaxis_title="Date", yaxis_title="Number of Active Lenders")

    output_path = Path(output_dir) / "active_lenders_trend.html"
    fig.write_html(str(output_path))
    logger.info("Saved Plotly plot to %s", output_path)


def plot_average_loan_size_over_time(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create an interactive Plotly version of average loan size over time."""

    logger.info("Creating average loan size over time plot (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(["Mortgage Amount", "Date"])
    df_temp = df_temp.group_by("Date").agg([
        pl.col("Mortgage Amount").mean().alias("mean"),
        pl.col("Mortgage Amount").median().alias("median"),
    ])
    df_temp = df_temp.collect().sort("Date")

    if df_temp.is_empty():
        logger.warning("No data available to plot average loan size (Plotly).")
        return

    pdf = df_temp.to_pandas().melt(
        id_vars=["Date"],
        value_vars=["mean", "median"],
        var_name="Statistic",
        value_name="Average Loan Amount",
    )

    fig = px.line(
        pdf,
        x="Date",
        y="Average Loan Amount",
        color="Statistic",
        markers=True,
        title="Average FHA Loan Size Over Time",
    )
    fig.update_layout(xaxis_title="Date", yaxis_title="Average Loan Amount ($)")

    output_path = Path(output_dir) / "avg_loan_size_trend.html"
    fig.write_html(str(output_path))
    logger.info("Saved Plotly plot to %s", output_path)


def plot_purchase_and_refinance_trend(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create an interactive Plotly version of purchase vs refinance trends."""

    logger.info("Creating purchase and refinance trend plot (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(["Loan Purpose", "Date"])
    df_temp = df_temp.with_columns([
        pl.when(pl.col("Loan Purpose").str.contains("Purchase"))
        .then(pl.lit("Purchase"))
        .otherwise(pl.lit("Refinance"))
        .alias("loan_category")
    ])
    df_temp = df_temp.group_by(["Date", "loan_category"]).agg([
        pl.len().alias("loan_count")
    ])
    df_temp = df_temp.collect().sort("Date")

    if df_temp.is_empty():
        logger.warning("No data available to plot purchase vs refinance (Plotly).")
        return

    pdf = df_temp.to_pandas()
    fig = px.line(
        pdf,
        x="Date",
        y="loan_count",
        color="loan_category",
        markers=True,
        title="Purchase and Refinance Loans by Year",
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Number of Loans",
        legend_title="Loan Category",
    )

    output_path = Path(output_dir) / "purchase_and_refinance_trend.html"
    fig.write_html(str(output_path))
    logger.info("Saved Plotly plot to %s", output_path)


def plot_down_payment_source_trend(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create an interactive Plotly version of the down payment source trend."""

    logger.info("Creating down payment source trend plot (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(["Down Payment Source", "Date"])
    df_temp = df_temp.with_columns(
        pl.col("Date").count().over("Date").alias("LoanCount"),
        (pl.col("Down Payment Source") == "Borrower").sum().over("Date").alias("BorrowerFundedCount"),
    )
    df_temp = df_temp.with_columns(
        (pl.col("BorrowerFundedCount") / pl.col("LoanCount")).alias("BorrowerFundedShare")
    )
    df_temp = df_temp.select(["Date", "BorrowerFundedShare"]).unique()
    df_temp = df_temp.collect().sort("Date")

    if df_temp.is_empty():
        logger.warning("No data available to plot down payment source (Plotly).")
        return

    pdf = df_temp.to_pandas()
    fig = px.line(
        pdf,
        x="Date",
        y="BorrowerFundedShare",
        markers=True,
        title="Share of Borrower-Funded Down Payments Over Time",
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Borrower-Funded Share",
    )

    output_path = Path(output_dir) / "down_payment_source_trend.html"
    fig.write_html(str(output_path))
    logger.info("Saved Plotly plot to %s", output_path)


def plot_interest_rate_and_loan_amount_by_product_type(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create interactive Plotly versions of interest rates and loan amounts by product type."""

    logger.info("Creating interest rate and loan amount by product type plots (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(["Product Type", "Interest Rate", "Mortgage Amount", "Date"])
    
    # Calculate averages for both metrics
    df_temp = df_temp.with_columns([
        pl.col("Interest Rate").mean().over(["Date", "Product Type"]).alias("AverageRate"),
        pl.col("Mortgage Amount").mean().over(["Date", "Product Type"]).alias("AverageSize")
    ])
    df_temp = df_temp.select(["Date", "Product Type", "AverageRate", "AverageSize"]).unique()
    df_temp = df_temp.collect().sort(["Date", "Product Type"])

    if df_temp.is_empty():
        logger.warning("No data available to plot interest rate and loan amount by product type (Plotly).")
        return

    pdf = df_temp.to_pandas()

    # Create interest rate plot
    fig_rates = px.line(
        pdf,
        x="Date",
        y="AverageRate",
        color="Product Type",
        title="Average Interest Rates by Product Type",
    )
    fig_rates.update_traces(mode="lines+markers")
    fig_rates.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Monthly Average Rate",
        legend_title="Product Type",
    )

    output_path_rates = Path(output_dir) / "interest_rate_by_product_type.html"
    fig_rates.write_html(str(output_path_rates))
    logger.info("Saved Plotly plot to %s", output_path_rates)

    # Create loan amount plot
    fig_amounts = px.line(
        pdf,
        x="Date",
        y="AverageSize",
        color="Product Type",
        title="Average Loan Amounts by Product Type",
    )
    fig_amounts.update_traces(mode="lines+markers")
    fig_amounts.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Monthly Average Loan Amount",
        legend_title="Product Type",
    )

    output_path_amounts = Path(output_dir) / "loan_amount_by_product_type.html"
    fig_amounts.write_html(str(output_path_amounts))
    logger.info("Saved Plotly plot to %s", output_path_amounts)


def plot_top_lender_group_averages(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
    top_n: int = 20,
) -> None:
    """Plot average rates and loan sizes for top N lenders versus all others."""

    logger.info("Creating top %d lender comparison plots (Plotly)...", top_n)

    lf = pl.scan_parquet(str(data_path))
    lf = lf.select(
        ["Date", "Originating Mortgagee", "Interest Rate", "Mortgage Amount"]
    )

    lender_groups = (
        lf.group_by(["Date", "Originating Mortgagee"])
        .agg(pl.len().alias("loan_count"))
        .with_columns(
            pl.col("loan_count")
            .rank(method="ordinal", descending=True)
            .over("Date")
            .alias("lender_rank")
        )
        .with_columns(
            pl.when(pl.col("lender_rank") <= top_n)
            .then(pl.lit(f"Top {top_n} Lenders"))
            .otherwise(pl.lit("Other Lenders"))
            .alias("lender_group")
        )
        .select(["Date", "Originating Mortgagee", "lender_group"])
    )

    joined = lf.join(lender_groups, on=["Date", "Originating Mortgagee"], how="left")

    summary = (
        joined.group_by(["Date", "lender_group"])
        .agg(
            [
                pl.col("Interest Rate").mean().alias("avg_interest_rate"),
                pl.col("Mortgage Amount").mean().alias("avg_loan_size"),
            ]
        )
        .sort(["Date", "lender_group"])
        .collect()
    )

    if summary.is_empty():
        logger.warning("No data available to plot top lender comparisons (Plotly).")
        return

    pdf = summary.to_pandas()
    pdf.sort_values("Date", inplace=True)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    fig_rates = px.line(
        pdf,
        x="Date",
        y="avg_interest_rate",
        color="lender_group",
        markers=True,
        title=f"Average Interest Rate: Top {top_n} vs Other Lenders",
    )
    fig_rates.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Average Interest Rate",
        legend_title="Lender Group",
    )
    rates_path = output_dir / f"top_{top_n}_lender_interest_rate_comparison.html"
    fig_rates.write_html(str(rates_path))

    fig_sizes = px.line(
        pdf,
        x="Date",
        y="avg_loan_size",
        color="lender_group",
        markers=True,
        title=f"Average Loan Size: Top {top_n} vs Other Lenders",
    )
    fig_sizes.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Average Loan Size ($)",
        legend_title="Lender Group",
    )
    sizes_path = output_dir / f"top_{top_n}_lender_loan_size_comparison.html"
    fig_sizes.write_html(str(sizes_path))

    logger.info("Saved Plotly plots to %s and %s", rates_path, sizes_path)


def plot_interest_rate_and_loan_amount_by_property_type(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create interactive Plotly versions of interest rates and loan amounts by property type."""

    logger.info("Creating interest rate and loan amount by property type plots (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(["Property Type", "Interest Rate", "Mortgage Amount", "Date"])
    
    # Standardize property types (like the original function)
    df_temp = df_temp.with_columns(
        pl.when(~pl.col("Property Type").str.contains("Single"))
        .then(pl.lit("Non single family"))
        .otherwise(pl.col("Property Type"))
        .alias("Property Type")
    )
    
    # Calculate averages for both metrics
    df_temp = df_temp.with_columns([
        pl.col("Interest Rate").mean().over(["Date", "Property Type"]).alias("AverageRate"),
        pl.col("Mortgage Amount").mean().over(["Date", "Property Type"]).alias("AverageSize")
    ])
    df_temp = df_temp.select(["Date", "Property Type", "AverageRate", "AverageSize"]).unique()
    df_temp = df_temp.collect().sort(["Date", "Property Type"])

    if df_temp.is_empty():
        logger.warning("No data available to plot interest rate and loan amount by property type (Plotly).")
        return

    pdf = df_temp.to_pandas()

    # Create interest rate plot
    fig_rates = px.line(
        pdf,
        x="Date",
        y="AverageRate",
        color="Property Type",
        title="Average Interest Rates by Property Type",
    )
    fig_rates.update_traces(mode="lines+markers")
    fig_rates.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Monthly Average Rate",
        legend_title="Property Type",
    )

    output_path_rates = Path(output_dir) / "interest_rate_by_property_type.html"
    fig_rates.write_html(str(output_path_rates))
    logger.info("Saved Plotly plot to %s", output_path_rates)

    # Create loan amount plot
    fig_amounts = px.line(
        pdf,
        x="Date",
        y="AverageSize",
        color="Property Type",
        title="Average Loan Amounts by Property Type",
    )
    fig_amounts.update_traces(mode="lines+markers")
    fig_amounts.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Monthly Average Loan Amount",
        legend_title="Property Type",
    )

    output_path_amounts = Path(output_dir) / "loan_amount_by_property_type.html"
    fig_amounts.write_html(str(output_path_amounts))
    logger.info("Saved Plotly plot to %s", output_path_amounts)


def plot_interest_rate_and_loan_amount_by_loan_purpose(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create interactive Plotly versions of interest rates and loan amounts by loan purpose."""

    logger.info("Creating interest rate and loan amount by loan purpose plots (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    df_temp = df_temp.select(["Loan Purpose", "Interest Rate", "Mortgage Amount", "Date"])
    
    # Calculate averages for both metrics
    df_temp = df_temp.with_columns([
        pl.col("Interest Rate").mean().over(["Date", "Loan Purpose"]).alias("AverageRate"),
        pl.col("Mortgage Amount").mean().over(["Date", "Loan Purpose"]).alias("AverageSize")
    ])
    df_temp = df_temp.select(["Date", "Loan Purpose", "AverageRate", "AverageSize"]).unique()
    df_temp = df_temp.collect().sort(["Date", "Loan Purpose"])

    if df_temp.is_empty():
        logger.warning("No data available to plot interest rate and loan amount by loan purpose (Plotly).")
        return

    pdf = df_temp.to_pandas()

    # Create interest rate plot
    fig_rates = px.line(
        pdf,
        x="Date",
        y="AverageRate",
        color="Loan Purpose",
        title="Average Interest Rates by Loan Purpose",
    )
    fig_rates.update_traces(mode="lines+markers")
    fig_rates.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Monthly Average Rate",
        legend_title="Loan Purpose",
    )

    output_path_rates = Path(output_dir) / "interest_rate_by_loan_purpose.html"
    fig_rates.write_html(str(output_path_rates))
    logger.info("Saved Plotly plot to %s", output_path_rates)

    # Create loan amount plot
    fig_amounts = px.line(
        pdf,
        x="Date",
        y="AverageSize",
        color="Loan Purpose",
        title="Average Loan Amounts by Loan Purpose",
    )
    fig_amounts.update_traces(mode="lines+markers")
    fig_amounts.update_layout(
        xaxis_title="Origination Date",
        yaxis_title="Monthly Average Loan Amount",
        legend_title="Loan Purpose",
    )

    output_path_amounts = Path(output_dir) / "loan_amount_by_loan_purpose.html"
    fig_amounts.write_html(str(output_path_amounts))
    logger.info("Saved Plotly plot to %s", output_path_amounts)


def plot_categorical_counts_over_time(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
    normalized: bool = False,
) -> None:
    """Create stacked line graphs showing counts of categorical variables over time."""

    logger.info("Creating categorical counts over time plots (Plotly)...")

    df_temp = pl.scan_parquet(str(data_path))
    
    # Use existing Date column (don't construct from Year/Month)
    df_temp = df_temp.select([
        "Date", 
        "Down Payment Source", 
        "Property Type", 
        "Product Type", 
        "Loan Purpose"
    ])
    
    # For property type, use all property types (don't standardize like other functions)
    # Keep other variables as-is
    
    df_temp = df_temp.collect().sort("Date")

    if df_temp.is_empty():
        logger.warning("No data available to plot categorical counts over time (Plotly).")
        return

    pdf = df_temp.to_pandas()
    
    # Create plots for each categorical variable
    categorical_vars = [
        ("Down Payment Source", "down_payment_source_counts"),
        ("Property Type", "property_type_counts"),
        ("Product Type", "product_type_counts"),
        ("Loan Purpose", "loan_purpose_counts")
    ]
    
    for var_name, filename in categorical_vars:
        # Count occurrences by Date and category
        counts = pdf.groupby(["Date", var_name]).size().reset_index()
        counts.columns = ["Date", var_name, "Count"]
        
        # Normalize if requested
        if normalized:
            # Calculate total count per date for normalization
            total_counts = counts.groupby("Date")["Count"].sum().reset_index()
            total_counts.columns = ["Date", "Total"]
            
            # Merge and calculate proportions
            counts = counts.merge(total_counts, on="Date")
            counts["Count"] = counts["Count"] / counts["Total"]
            
            # Update filename and title for normalized version
            filename = f"{filename}_normalized"
            title = f"{var_name} Proportions Over Time"
            y_title = "Proportion of Loans"
        else:
            title = f"{var_name} Counts Over Time"
            y_title = "Number of Loans"
        
        # Create stacked line plot
        fig = px.area(
            counts,
            x="Date",
            y="Count",
            color=var_name,
            title=title,
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=y_title,
            legend_title=var_name,
        )
        
        output_path = Path(output_dir) / f"{filename}.html"
        fig.write_html(str(output_path))
        logger.info("Saved Plotly plot to %s", output_path)


def create_all_trend_plots(
    data_path: Union[str, Path],
    output_dir: Union[str, Path] = "output",
) -> None:
    """Create all Plotly-based trend plots for FHA data analysis."""

    logger.info("Creating all Plotly trend plots...")

    Path(output_dir).mkdir(exist_ok=True)

    plot_active_lenders_over_time(data_path, output_dir)
    plot_average_loan_size_over_time(data_path, output_dir)
    plot_purchase_and_refinance_trend(data_path, output_dir)
    plot_down_payment_source_trend(data_path, output_dir)
    plot_interest_rate_and_loan_amount_by_product_type(data_path, output_dir)
    plot_interest_rate_and_loan_amount_by_property_type(data_path, output_dir)
    plot_interest_rate_and_loan_amount_by_loan_purpose(data_path, output_dir)
    plot_top_lender_group_averages(data_path, output_dir)
    plot_categorical_counts_over_time(data_path, output_dir)
    plot_categorical_counts_over_time(data_path, output_dir, normalized=True)

    logger.info("All Plotly trend plots created successfully")


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
    
    # Print results
    print_summary_statistics(lender_stats, "Lender Activity Analysis")
    print_summary_statistics(sponsor_stats, "Sponsor Activity Analysis")
    
    # Create visualizations if requested
    if create_plots:
        logger.info("Creating all trend plots...")
        create_all_trend_plots(data_path, output_dir)
        logger.info("Analysis and visualization complete!")
    else:
        logger.info("Analysis complete! Use create_all_trend_plots() to generate visualizations.")


if __name__ == "__main__":
    main()

