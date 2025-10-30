"""Utilities for computing lender market concentration metrics."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Literal

import polars as pl


TimeLevel = Literal["month", "quarter", "year", "all"]
GeographyLevel = Literal["state", "county", "all"]
LenderType = Literal["sponsor", "originator"]


def _sanitize_string_column(column: pl.Expr, alias: str) -> pl.Expr:
    """Strip whitespace and ensure UTF-8 string representation."""

    return column.cast(pl.Utf8).str.strip_chars().alias(alias)


def _build_missing_filter(columns: Iterable[str], string_columns: set[str]) -> pl.Expr:
    """Return a filter expression that drops null or empty string values."""

    conditions: list[pl.Expr] = []
    for name in columns:
        conditions.append(pl.col(name).is_not_null())
        if name in string_columns:
            conditions.append(pl.col(name) != "")
    if not conditions:
        return pl.lit(True)
    expr = conditions[0]
    for condition in conditions[1:]:
        expr = expr & condition
    return expr


def compute_lender_hhi(
    dataset_path: str | Path,
    time_level: TimeLevel = "year",
    geography_level: GeographyLevel = "state",
    lender_type: LenderType = "sponsor",
    drop_missing: bool = True,
) -> pl.DataFrame:
    """Compute Herfindahl-Hirschman Index (HHI) for FHA lenders.

    Args:
        dataset_path: Path to the parquet dataset (typically ``data/silver/single_family``).
        time_level: Temporal aggregation level ("month", "quarter", "year", or "all").
        geography_level: Geographic aggregation level ("state", "county", or "all").
        lender_type: Use sponsor or originator identifiers for concentration calculation.
        drop_missing: Whether to drop records with missing identifiers/dimensions.

    Returns:
        A :class:`polars.DataFrame` with HHI values and supporting metadata.
    """

    if time_level not in {"month", "quarter", "year", "all"}:
        raise ValueError("time_level must be one of 'month', 'quarter', 'year', 'all'")
    if geography_level not in {"state", "county", "all"}:
        raise ValueError("geography_level must be one of 'state', 'county', 'all'")
    if lender_type not in {"sponsor", "originator"}:
        raise ValueError("lender_type must be one of 'sponsor', 'originator'")

    dataset_path = Path(dataset_path)

    lf = pl.scan_parquet(str(dataset_path))

    # Build lender identifier with appropriate fallback logic.
    if lender_type == "sponsor":
        lender_expr = (
            pl.when(
                pl.col("Sponsor Number").is_not_null()
                & (pl.col("Sponsor Number").cast(pl.Utf8).str.strip_chars() != "")
            )
            .then(pl.col("Sponsor Number"))
            .otherwise(pl.col("Originating Mortgagee Number"))
        )
    else:
        lender_expr = pl.col("Originating Mortgagee Number")

    lf = lf.with_columns(
        _sanitize_string_column(lender_expr, "lender_id"),
        pl.lit(lender_type).alias("lender_type"),
    )

    group_columns: list[str] = []
    string_columns: set[str] = {"lender_id"}

    # Time dimensions
    if time_level == "month":
        lf = lf.with_columns(
            pl.col("Year").alias("year"),
            pl.col("Month").alias("month"),
        )
        group_columns.extend(["year", "month"])
    elif time_level == "quarter":
        lf = lf.with_columns(
            pl.col("Year").alias("year"),
            ((pl.col("Month") - 1) // 3 + 1).cast(pl.Int8).alias("quarter"),
        )
        group_columns.extend(["year", "quarter"])
    elif time_level == "year":
        lf = lf.with_columns(pl.col("Year").alias("year"))
        group_columns.append("year")

    # Geography dimensions
    if geography_level == "state":
        lf = lf.with_columns(_sanitize_string_column(pl.col("Property State"), "state"))
        group_columns.append("state")
        string_columns.add("state")
    elif geography_level == "county":
        lf = lf.with_columns(
            _sanitize_string_column(pl.col("Property State"), "state"),
            _sanitize_string_column(pl.col("Property County"), "county"),
        )
        group_columns.extend(["state", "county"])
        string_columns.update({"state", "county"})

    added_all_column = False
    if not group_columns:
        lf = lf.with_columns(pl.lit("all").alias("scope"))
        group_columns.append("scope")
        string_columns.add("scope")
        added_all_column = True

    if drop_missing:
        filter_expr = _build_missing_filter(group_columns + ["lender_id"], string_columns)
        lf = lf.filter(filter_expr)

    # Aggregate loan counts by lender and grouping columns
    lender_group_cols = group_columns + ["lender_id"]
    lf_counts = (
        lf.group_by(lender_group_cols)
        .agg(
            pl.len().alias("loan_count"),
            pl.first("lender_type").alias("lender_type"),
        )
    )

    # Compute market shares within each group and derive HHI
    lf_hhi = (
        lf_counts
        .with_columns(
            (
                pl.col("loan_count")
                / pl.sum("loan_count").over(group_columns)
            ).alias("market_share")
        )
        .with_columns((pl.col("market_share") ** 2).alias("share_sq"))
        .group_by(group_columns)
        .agg(
            pl.col("share_sq").sum().alias("hhi_normalized"),
            pl.sum("loan_count").alias("total_loans"),
            pl.col("lender_id").n_unique().alias("num_lenders"),
            pl.first("lender_type").alias("lender_type"),
        )
        .with_columns((pl.col("hhi_normalized") * 10_000).alias("hhi"))
        .sort(group_columns)
    )

    if added_all_column:
        lf_hhi = lf_hhi.drop("scope")

    return lf_hhi.collect()


__all__ = ["compute_lender_hhi"]

