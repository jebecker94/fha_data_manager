"""Geographic summary helpers for FHA single-family data."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl
import plotly.express as px
import plotly.graph_objects as go

Frequency = Literal["annual", "quarterly"]


def _normalize_frequency(frequency: str) -> Frequency:
    """Validate and normalize geographic summary frequency."""

    freq = frequency.lower()
    if freq in {"annual", "yearly", "year"}:
        return "annual"
    if freq in {"quarter", "quarterly"}:
        return "quarterly"
    msg = "frequency must be 'annual' or 'quarterly'"
    raise ValueError(msg)


def _prepare_period_columns(lf: pl.LazyFrame, frequency: Frequency) -> tuple[pl.LazyFrame, list[str]]:
    """Attach the period columns needed for aggregations."""

    period_columns = ["Year"]
    if frequency == "quarterly":
        lf = lf.with_columns(
            ((pl.col("Month") - 1) // 3 + 1)
            .cast(pl.UInt8)
            .alias("Quarter")
        )
        period_columns.append("Quarter")
    return lf, period_columns


def summarize_county_metrics(
    df: pl.DataFrame | pl.LazyFrame,
    frequency: str = "annual",
    *,
    fips_col: str = "FIPS",
    state_col: str = "Property State",
    county_col: str = "Property County",
    output_path: str | Path | None = None,
) -> pl.DataFrame:
    """Create county-level mortgage summaries.

    Args:
        df: FHA single-family dataset enriched with county-level FIPS codes.
        frequency: Aggregation frequency (``"annual"`` or ``"quarterly"``).
        fips_col: Column containing county-level FIPS codes.
        state_col: Column containing state names or abbreviations.
        county_col: Column containing county names.
        output_path: Optional path where the resulting summary will be written
            as a Parquet file. If a directory is provided, the file name will
            be ``county_metrics_<frequency>.parquet``.

    Returns:
        A ``pl.DataFrame`` with loan counts, mortgage statistics, and interest
        rate dispersion measures at the county level.
    """

    freq = _normalize_frequency(frequency)
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df
    lf, period_columns = _prepare_period_columns(lf, freq)

    required_columns = {fips_col, state_col, county_col, "Mortgage Amount", "Interest Rate"}
    missing = required_columns.difference(lf.columns)
    if missing:
        msg = f"Missing required columns for county summary: {sorted(missing)}"
        raise ValueError(msg)

    grouping_columns = period_columns + [state_col, county_col, fips_col]

    summary = (
        lf.group_by(grouping_columns)
        .agg(
            [
                pl.len().alias("loan_count"),
                pl.col("Mortgage Amount").median().alias("median_mortgage_amount"),
                pl.col("Mortgage Amount").mean().alias("avg_mortgage_amount"),
                pl.col("Mortgage Amount").sum().alias("total_mortgage_amount"),
                pl.col("Interest Rate").mean().alias("avg_interest_rate"),
                pl.col("Interest Rate").std().alias("interest_rate_std"),
                pl.col("Interest Rate").quantile(0.75).alias("interest_rate_q3"),
                pl.col("Interest Rate").quantile(0.25).alias("interest_rate_q1"),
            ]
        )
        .with_columns(
            [
                (pl.col("interest_rate_q3") - pl.col("interest_rate_q1")).alias("interest_rate_iqr"),
                pl.lit(freq).alias("frequency"),
            ]
        )
        .drop(["interest_rate_q3", "interest_rate_q1"])
        .sort(grouping_columns)
    )

    result = summary.collect()

    if output_path:
        output_path = Path(output_path)
        if output_path.suffix:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.write_parquet(str(output_path))
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            file_name = f"county_metrics_{freq}.parquet"
            result.write_parquet(str(output_path / file_name))

    return result


def _collect_frame(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Collect the input into a :class:`polars.DataFrame` if it is lazy."""

    return df.collect() if isinstance(df, pl.LazyFrame) else df


def create_state_loan_count_choropleth(
    df: pl.DataFrame | pl.LazyFrame,
    *,
    state_col: str = "Property State",
    title: str | None = None,
    color_scale: str = "Blues",
) -> go.Figure:
    """Create a Plotly choropleth map of loan counts by state.

    Args:
        df: FHA single-family dataset containing state information.
        state_col: Column containing state abbreviations.
        title: Optional title to apply to the resulting figure.
        color_scale: Plotly continuous color scale name to use for the map.

    Returns:
        A :class:`plotly.graph_objects.Figure` representing the state-level
        loan count map.
    """

    frame = _collect_frame(df)
    if state_col not in frame.columns:
        msg = f"Column '{state_col}' not found in dataframe."
        raise ValueError(msg)

    aggregated = (
        frame.group_by(state_col)
        .agg(pl.len().alias("loan_count"))
        .with_columns(pl.col(state_col).cast(pl.Utf8).str.to_uppercase())
        .sort(state_col)
    )

    pdf = aggregated.rename({state_col: "state"}).to_pandas()

    fig = px.choropleth(
        pdf,
        locations="state",
        locationmode="USA-states",
        color="loan_count",
        scope="usa",
        color_continuous_scale=color_scale,
        labels={"loan_count": "Loan Count"},
        hover_data={"state": True, "loan_count": True},
        title=title,
    )
    fig.update_geos(fitbounds="locations", visible=False)
    return fig


def create_county_loan_count_choropleth(
    df: pl.DataFrame | pl.LazyFrame,
    *,
    fips_col: str = "FIPS",
    state_col: str = "Property State",
    county_col: str = "Property County",
    title: str | None = None,
    color_scale: str = "Blues",
    geojson: dict | None = None,
) -> go.Figure:
    """Create a Plotly choropleth map of loan counts by county.

    Args:
        df: FHA single-family dataset containing county FIPS information.
        fips_col: Column containing county-level FIPS codes.
        state_col: Column containing state abbreviations for hover labels.
        county_col: Column containing county names for hover labels.
        title: Optional title to apply to the resulting figure.
        color_scale: Plotly continuous color scale name to use for the map.
        geojson: Optional GeoJSON mapping of U.S. counties. When ``None`` the
            Plotly sample county GeoJSON is used.

    Returns:
        A :class:`plotly.graph_objects.Figure` representing the county-level
        loan count map.
    """

    frame = _collect_frame(df)
    missing_columns = [
        col
        for col in (fips_col, state_col, county_col)
        if col not in frame.columns
    ]
    if missing_columns:
        msg = f"Missing required columns for county choropleth: {missing_columns}"
        raise ValueError(msg)

    aggregated = (
        frame.group_by(fips_col)
        .agg(
            [
                pl.len().alias("loan_count"),
                pl.col(state_col).first().alias(state_col),
                pl.col(county_col).first().alias(county_col),
            ]
        )
        .with_columns(
            pl.col(fips_col)
            .cast(pl.Utf8)
            .str.strip()
            .str.zfill(5)
            .alias("fips")
        )
        .sort("fips")
    )

    pdf = aggregated.rename({state_col: "state", county_col: "county"}).to_pandas()

    county_geojson = geojson or px.data.election_geojson()
    fig = px.choropleth(
        pdf,
        geojson=county_geojson,
        locations="fips",
        color="loan_count",
        scope="usa",
        color_continuous_scale=color_scale,
        labels={"loan_count": "Loan Count"},
        hover_data={"county": True, "state": True, "loan_count": True},
        title=title,
    )
    fig.update_geos(fitbounds="locations", visible=False)
    return fig


def summarize_metro_metrics(
    df: pl.DataFrame | pl.LazyFrame,
    frequency: str = "annual",
    *,
    county_fips_col: str = "FIPS",
    cbsa_col: str = "CBSA Code",
    cbsa_name_col: str | None = "CBSA Title",
    cbsa_crosswalk: pl.DataFrame | pl.LazyFrame | None = None,
    output_path: str | Path | None = None,
) -> pl.DataFrame:
    """Create metro-level mortgage summaries from county-level data.

    Args:
        df: FHA single-family dataset enriched with county-level FIPS codes.
        frequency: Aggregation frequency (``"annual"`` or ``"quarterly"``).
        county_fips_col: Column containing county-level FIPS codes used for
            joins to CBSA information.
        cbsa_col: Column containing CBSA or metropolitan area identifiers.
        cbsa_name_col: Optional column with CBSA titles or names.
        cbsa_crosswalk: Optional dataset mapping counties to CBSAs. When
            provided, it must contain ``county_fips_col`` and ``cbsa_col``
            (plus ``cbsa_name_col`` if provided).
        output_path: Optional path where the resulting summary will be written
            as a Parquet file. If a directory is provided, the file name will
            be ``metro_metrics_<frequency>.parquet``.

    Returns:
        A ``pl.DataFrame`` with metro-level loan counts and mortgage statistics.
    """

    freq = _normalize_frequency(frequency)
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df
    lf, period_columns = _prepare_period_columns(lf, freq)

    if cbsa_crosswalk is not None:
        crosswalk_lf = cbsa_crosswalk.lazy() if isinstance(cbsa_crosswalk, pl.DataFrame) else cbsa_crosswalk
        crosswalk_columns = [county_fips_col, cbsa_col]
        if cbsa_name_col is not None:
            crosswalk_columns.append(cbsa_name_col)
        crosswalk_lf = crosswalk_lf.select(crosswalk_columns).unique()
        lf = lf.join(crosswalk_lf, on=county_fips_col, how="left")

    required_columns = {county_fips_col, cbsa_col, "Mortgage Amount", "Interest Rate"}
    missing = required_columns.difference(lf.columns)
    if missing:
        msg = f"Missing required columns for metro summary: {sorted(missing)}"
        raise ValueError(msg)

    grouping_columns = period_columns + [cbsa_col]
    if cbsa_name_col is not None and cbsa_name_col in lf.columns:
        grouping_columns.append(cbsa_name_col)

    summary = (
        lf.group_by(grouping_columns)
        .agg(
            [
                pl.len().alias("loan_count"),
                pl.col("Mortgage Amount").median().alias("median_mortgage_amount"),
                pl.col("Mortgage Amount").mean().alias("avg_mortgage_amount"),
                pl.col("Mortgage Amount").sum().alias("total_mortgage_amount"),
                pl.col("Interest Rate").mean().alias("avg_interest_rate"),
                pl.col("Interest Rate").std().alias("interest_rate_std"),
                pl.col("Interest Rate").quantile(0.75).alias("interest_rate_q3"),
                pl.col("Interest Rate").quantile(0.25).alias("interest_rate_q1"),
            ]
        )
        .with_columns(
            [
                (pl.col("interest_rate_q3") - pl.col("interest_rate_q1")).alias("interest_rate_iqr"),
                pl.lit(freq).alias("frequency"),
            ]
        )
        .drop(["interest_rate_q3", "interest_rate_q1"])
        .sort(grouping_columns)
    )

    result = summary.collect()

    if output_path:
        output_path = Path(output_path)
        if output_path.suffix:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.write_parquet(str(output_path))
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            file_name = f"metro_metrics_{freq}.parquet"
            result.write_parquet(str(output_path / file_name))

    return result
