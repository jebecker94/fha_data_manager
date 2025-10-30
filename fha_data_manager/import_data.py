# -*- coding: utf-8 -*-
"""Core import and cleaning functionality for FHA snapshot datasets."""

from __future__ import annotations

import datetime
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from multiprocessing import get_context
from os import cpu_count
from pathlib import Path
from typing import Callable, Literal, TypeAlias

import addfips
import pandas as pd
import polars as pl
from .utils.mtgdicts import FHADictionary

import fastexcel

_SINGLE_FAMILY_CATEGORICAL_VALUES: dict[str, tuple[str, ...]] = {
    "Loan Purpose": ("Purchase", "Refi_FHA", "Refi_Conv_Curr"),
    "Property Type": (
        "Single Family",
        "Condo",
        "Rehabilitation",
        "H4H",
        "Other",
    ),
    "Product Type": ("Fixed Rate", "Adjustable Rate"),
    "Down Payment Source": (
        "Borrower",
        "Relative",
        "Gov Asst",
        "Non Profit",
        "Employer",
    ),
}

PathLike: TypeAlias = Path | str
SnapshotType: TypeAlias = Literal['single_family', 'hecm']

logger = logging.getLogger(__name__)

COUNTY_FIPS_OVERRIDES: dict[tuple[str, str], str] = {
    ("DC", "WASHINGTON"): "11001",
    ("DC", "DISTRICT OF COLUMBIA"): "11001",
}


# Support Functions
def standardize_county_names(
    df: pl.LazyFrame,
    county_col: str = "Property County",
    state_col: str = "Property State",
) -> pl.LazyFrame:
    """Standardise county names so they align with the FIPS dataset.

    Args:
        df: The snapshot data containing county information.
        county_col: Column containing county names to standardise.
        state_col: Column containing the two-letter state abbreviation.

    Returns:
        A ``LazyFrame`` with harmonised county naming.
    """

    # Note: This function contains considerable hardcoding of county names that reflects
    # the idiosyncrasies of the FHA single family snapshot dataset. Not suitable for
    # deployment in other projects.

    logger.info("Standardizing county names...")

    # First convert empty values and "NAN"/"None" to empty strings
    df = df.with_columns(
        pl.when(pl.col(county_col).is_null())
        .then(pl.lit(""))
        .when(pl.col(county_col).str.to_lowercase().is_in(["nan", "none"]))
        .then(pl.lit(""))
        .otherwise(pl.col(county_col))
        .str.to_uppercase()
        .alias(county_col)
    )

    # Fix obvious state/county mismatches for Alaska
    df = df.with_columns(
        pl.when(
            (pl.col(county_col) == "ANNE ARUNDEL") & (pl.col(state_col) == "AK")
        ).then(pl.lit("MD")).otherwise(pl.col(state_col)).alias(state_col)
    ).with_columns(
        pl.when(
            (pl.col(county_col) == "BUNCOMBE") & (pl.col(state_col) == "AK")
        ).then(pl.lit("NC")).otherwise(pl.col(state_col)).alias(state_col)
    ).with_columns(
        pl.when(
            (pl.col(county_col) == "EL PASO") & (pl.col(state_col) == "AK")
        ).then(pl.lit("TX")).otherwise(pl.col(state_col)).alias(state_col)
    )
    
    # Apply specific county name fixes using when/then expressions
    df = df.with_columns(
        pl.when(
            (pl.col(county_col) == "MATANUSKA SUSITNA") & (pl.col(state_col) == "AK")
        ).then(pl.lit("MATANUSKA-SUSITNA"))
        .when(
            (pl.col(county_col) == "DE KALB") & (pl.col(state_col).is_in(["AL", "IL", "IN"]))
        ).then(pl.lit("DEKALB"))
        .when(
            (pl.col(county_col) == "DU PAGE") & (pl.col(state_col) == "IL")
        ).then(pl.lit("DUPAGE"))
        .when(
            (pl.col(county_col) == "LA SALLE") & (pl.col(state_col).is_in(["IL", "IN"]))
        ).then(pl.lit("LASALLE"))
        .when(
            (pl.col(county_col) == "LA PORTE") & (pl.col(state_col) == "IN")
        ).then(pl.lit("LAPORTE"))
        .when(
            (pl.col(county_col) == "ST JOSEPH") & (pl.col(state_col) == "IN")
        ).then(pl.lit("ST. JOSEPH"))
        .when(
            (pl.col(county_col) == "MACON-BIBB COUNTY") & (pl.col(state_col) == "GA")
        ).then(pl.lit("BIBB"))
        .when(
            (pl.col(county_col) == "ST JOHN THE BAPTIST") & (pl.col(state_col) == "LA")
        ).then(pl.lit("ST. JOHN THE BAPTIST"))
        .when(
            (pl.col(county_col) == "STE GENEVIEVE") & (pl.col(state_col) == "MO")
        ).then(pl.lit("SAINTE GENEVIEVE"))
        .when(
            (pl.col(county_col) == "DE SOTO") & (pl.col(state_col) == "MS")
        ).then(pl.lit("DESOTO"))
        .when(
            (pl.col(county_col) == "BAYAM'N") & (pl.col(state_col) == "PR")
        ).then(pl.lit("BAYAMON"))
        .when(
            (pl.col(county_col) == "LACROSSE") & (pl.col(state_col) == "WI")
        ).then(pl.lit("LA CROSSE"))
        .when(
            (pl.col(county_col) == "LAPLATA") & (pl.col(state_col) == "CO")
        ).then(pl.lit("LA PLATA"))
        .when(
            (pl.col(county_col) == "DEWITT") & (pl.col(state_col) == "IL")
        ).then(pl.lit("DE WITT"))
        .when(
            (pl.col(county_col) == "CAN'VANAS") & (pl.col(state_col) == "PR")
        ).then(pl.lit("CANOVANAS"))
        .when(
            pl.col(county_col).str.contains(" COUNTY$")
        ).then(
            pl.col(county_col).str.replace(" COUNTY$", "")
        )
        .otherwise(pl.col(county_col))
        .alias(county_col)
    )
    
    # Handle common prefixes after specific cases
    df = df.with_columns(
        pl.when(pl.col(county_col).str.starts_with("ST "))
        .then(pl.concat_str([pl.lit("ST. "), pl.col(county_col).str.slice(3)]))
        .when(pl.col(county_col).str.starts_with("STE "))
        .then(pl.concat_str([pl.lit("SAINTE "), pl.col(county_col).str.slice(4)]))
        .otherwise(pl.col(county_col))
        .alias(county_col)
    )

    # Return DataFrame
    return df


def _read_tabular_file(path: Path) -> pl.DataFrame:
    """Read a CSV or parquet file into a Polars DataFrame."""

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pl.read_parquet(path)
    raise ValueError(f"Unsupported file extension for {path}")


def _write_tabular_file(df: pl.DataFrame, path: Path) -> None:
    """Persist a Polars DataFrame to CSV or parquet."""

    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.write_csv(path)
        return
    if suffix in {".parquet", ".pq"}:
        df.write_parquet(path)
        return
    raise ValueError(f"Unsupported file extension for {path}")


def add_county_fips(
    df: pl.LazyFrame,
    state_col: str = "Property State",
    county_col: str = "Property County",
    fips_col: str = "FIPS",
) -> pl.LazyFrame:
    """Add FIPS codes to a dataset with state and county columns.

    Args:
        df: Dataset containing the state and county columns to enrich.
        state_col: Name of the state column.
        county_col: Name of the county column.
        fips_col: Name of the output column that will receive the concatenated FIPS code.

    Returns:
        A ``LazyFrame`` containing a ``fips_col`` column with county-level FIPS codes.
    """
    logger.info("Starting FIPS code addition process...")

    # Standardize the main dataframe's county names
    logger.info("Standardizing main dataframe county names...")
    df = standardize_county_names(df, state_col=state_col, county_col=county_col)

    # Get unique state/county pairs and standardize them
    logger.info("Getting unique county/state pairs...")
    unique_counties = df.select([state_col, county_col]).unique()
    unique_counties = standardize_county_names(unique_counties, state_col=state_col, county_col=county_col)

    # Create list to store county mappings
    county_map: list[pl.LazyFrame] = []

    # Initialize AddFIPS
    logger.info("Initializing AddFIPS...")
    af = addfips.AddFIPS()

    # Generate FIPS codes for each unique county
    logger.info("Generating FIPS codes for unique counties...")
    for state, county in unique_counties.collect().iter_rows():
        df_row = pl.LazyFrame({state_col: [state], county_col: [county]})
        fips = af.get_county_fips(county, state)
        df_row = df_row.with_columns(pl.lit(fips).alias(fips_col))
        county_map.append(df_row)
    
    # Combine all county mappings
    logger.info("Combining county mappings...")
    county_map = pl.concat(county_map, how='diagonal_relaxed')
    county_map = county_map.sort([fips_col, state_col, county_col])

    # Join FIPS codes back to original dataframe
    logger.info("Joining FIPS codes back to main dataframe...")
    df = df.join(county_map, on=[state_col, county_col], how="left")

    # Return DataFrame
    return df


def build_county_fips_crosswalk(
    bronze_folder: PathLike,
    crosswalk_path: PathLike,
    problematic_path: PathLike,
    state_col: str = "Property State",
    county_col: str = "Property County",
    fips_col: str = "FIPS",
    manual_overrides: Mapping[tuple[str, str], str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Create or extend a county FIPS crosswalk from bronze snapshot files.

    The function scans the bronze-level parquet datasets for both the single
    family and HECM programs, standardises county names using the same logic as
    the main import pipeline, and applies the :mod:`addfips` lookup augmented by
    manual overrides. New state/county combinations are appended to the
    ``crosswalk_path`` file, and any unresolved observations are written to
    ``problematic_path``.

    Args:
        bronze_folder: Directory containing ``single_family`` and ``hecm``
            bronze parquet files.
        crosswalk_path: Destination for the successful crosswalk mappings. The
            extension determines whether a CSV or parquet file is written.
        problematic_path: Destination for observations that could not be
            matched to a FIPS code.
        state_col: Name of the state column in the source data.
        county_col: Name of the county column in the source data.
        fips_col: Name of the output FIPS column in the crosswalk.
        manual_overrides: Additional manual mappings from ``(state, county)`` to
            FIPS codes. Overrides are combined with the built-in corrections.

    Returns:
        Tuple containing the updated crosswalk dataframe and the dataframe of
        problematic observations.
    """

    bronze_root = Path(bronze_folder)
    crosswalk_file = Path(crosswalk_path)
    problematic_file = Path(problematic_path)

    manual_map: dict[tuple[str, str], str] = dict(COUNTY_FIPS_OVERRIDES)
    if manual_overrides:
        manual_map.update(
            {(
                state.upper(),
                county.upper(),
            ): code
             for (state, county), code in manual_overrides.items()}
        )

    logger.info("Scanning bronze datasets for unique state/county pairs...")
    lazy_frames: list[pl.LazyFrame] = []
    for dataset in ("single_family", "hecm"):
        dataset_dir = bronze_root / dataset
        if not dataset_dir.exists():
            logger.warning("Bronze dataset folder missing: %s", dataset_dir)
            continue

        dataset_pattern = dataset_dir / "*.parquet"
        try:
            lf = pl.scan_parquet(str(dataset_pattern)).select(
                [
                    pl.col(state_col).cast(pl.Utf8, strict=False).alias(state_col),
                    pl.col(county_col).cast(pl.Utf8, strict=False).alias(county_col),
                ]
            )
        except FileNotFoundError:
            logger.warning("No parquet files found for dataset: %s", dataset_dir)
            continue
        lazy_frames.append(lf)

    if not lazy_frames:
        msg = "No bronze parquet files were found for single_family or hecm."
        raise FileNotFoundError(msg)

    combined = pl.concat(lazy_frames, how="diagonal_relaxed")
    combined = standardize_county_names(
        combined, state_col=state_col, county_col=county_col
    )
    combined = combined.with_columns(pl.col(state_col).str.to_uppercase())

    unique_pairs = (
        combined
        .filter(
            pl.col(state_col).is_not_null()
            & pl.col(county_col).is_not_null()
            & (pl.col(state_col) != "")
            & (pl.col(county_col) != "")
        )
        .select([state_col, county_col])
        .unique()
        .collect()
    )

    logger.info("Loaded %d unique state/county pairs", len(unique_pairs))

    existing_crosswalk: pl.DataFrame | None = None
    if crosswalk_file.exists():
        logger.info("Reading existing crosswalk from %s", crosswalk_file)
        existing_crosswalk = _read_tabular_file(crosswalk_file)
        existing_crosswalk = (
            standardize_county_names(
                existing_crosswalk.lazy(),
                state_col=state_col,
                county_col=county_col,
            )
            .with_columns(
                pl.col(state_col).str.to_uppercase(),
                pl.col(fips_col)
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .str.zfill(5)
                .alias(fips_col),
            )
            .collect()
        )

    known_pairs = (
        existing_crosswalk.select([state_col, county_col]).unique()
        if existing_crosswalk is not None
        else pl.DataFrame(
            {
                state_col: pl.Series([], dtype=pl.Utf8),
                county_col: pl.Series([], dtype=pl.Utf8),
            }
        )
    )

    new_pairs = unique_pairs.join(
        known_pairs, on=[state_col, county_col], how="anti"
    )

    logger.info("Identified %d new state/county pairs", len(new_pairs))

    af = addfips.AddFIPS()
    crosswalk_records: list[tuple[str, str, str]] = []
    problematic_records: list[tuple[str, str]] = []

    for state, county in new_pairs.iter_rows():
        key = (state, county)
        fips = manual_map.get(key)
        if not fips:
            fips = af.get_county_fips(county, state)
        if fips:
            crosswalk_records.append((state, county, str(fips).zfill(5)))
        else:
            problematic_records.append((state, county))

    new_crosswalk = (
        pl.DataFrame(
            {
                state_col: [record[0] for record in crosswalk_records],
                county_col: [record[1] for record in crosswalk_records],
                fips_col: [record[2] for record in crosswalk_records],
            }
        )
        if crosswalk_records
        else pl.DataFrame(
            {
                state_col: pl.Series([], dtype=pl.Utf8),
                county_col: pl.Series([], dtype=pl.Utf8),
                fips_col: pl.Series([], dtype=pl.Utf8),
            }
        )
    )

    new_problematic = (
        pl.DataFrame(
            {
                state_col: [record[0] for record in problematic_records],
                county_col: [record[1] for record in problematic_records],
            }
        )
        if problematic_records
        else pl.DataFrame(
            {
                state_col: pl.Series([], dtype=pl.Utf8),
                county_col: pl.Series([], dtype=pl.Utf8),
            }
        )
    )

    crosswalk_frames: list[pl.DataFrame] = []
    if existing_crosswalk is not None:
        crosswalk_frames.append(existing_crosswalk)
    if not new_crosswalk.is_empty():
        crosswalk_frames.append(new_crosswalk)

    if crosswalk_frames:
        updated_crosswalk = (
            pl.concat(crosswalk_frames, how="diagonal_relaxed")
            .unique(subset=[state_col, county_col], keep="first")
            .sort([state_col, county_col])
        )
    else:
        updated_crosswalk = pl.DataFrame(
            {
                state_col: pl.Series([], dtype=pl.Utf8),
                county_col: pl.Series([], dtype=pl.Utf8),
                fips_col: pl.Series([], dtype=pl.Utf8),
            }
        )

    existing_problematic: pl.DataFrame | None = None
    if problematic_file.exists():
        logger.info("Reading existing problematic observations from %s", problematic_file)
        existing_problematic = (
            standardize_county_names(
                _read_tabular_file(problematic_file).lazy(),
                state_col=state_col,
                county_col=county_col,
            )
            .with_columns(pl.col(state_col).str.to_uppercase())
            .collect()
        )

    problematic_frames: list[pl.DataFrame] = []
    if existing_problematic is not None and not existing_problematic.is_empty():
        resolved_pairs = updated_crosswalk.select([state_col, county_col])
        existing_problematic = existing_problematic.join(
            resolved_pairs, on=[state_col, county_col], how="anti"
        )
        if not existing_problematic.is_empty():
            problematic_frames.append(existing_problematic)
    if not new_problematic.is_empty():
        problematic_frames.append(new_problematic)

    if problematic_frames:
        updated_problematic = (
            pl.concat(problematic_frames, how="diagonal_relaxed")
            .unique()
            .sort([state_col, county_col])
        )
    else:
        updated_problematic = pl.DataFrame(
            {
                state_col: pl.Series([], dtype=pl.Utf8),
                county_col: pl.Series([], dtype=pl.Utf8),
            }
        )

    logger.info(
        "Crosswalk now contains %d rows; %d problematic observations recorded",
        len(updated_crosswalk),
        len(updated_problematic),
    )

    _write_tabular_file(updated_crosswalk, crosswalk_file)
    _write_tabular_file(updated_problematic, problematic_file)

    return updated_crosswalk, updated_problematic


def _apply_single_family_categoricals(df: pl.LazyFrame) -> pl.LazyFrame:
    """Cast key single-family variables to categorical dtypes.

    The allowed category values are represented in :data:`_SINGLE_FAMILY_CATEGORICAL_VALUES`.
    """

    casts: list[pl.Expr] = []
    schema = df.schema

    for column, values in _SINGLE_FAMILY_CATEGORICAL_VALUES.items():
        if column not in schema:
            continue
        casts.append(
            pl.col(column)
            .cast(pl.Utf8, strict=False)
            .cast(pl.Categorical)
            .alias(column)
        )

    if casts:
        df = df.with_columns(casts)

    return df


def create_lender_id_to_name_crosswalk(clean_data_folder: PathLike) -> pl.DataFrame:
    """Create a lender ID/name crosswalk from cleaned snapshot parquet files."""

    logger.info("Creating lender ID to name crosswalk...")

    lazy_frames: list[pl.LazyFrame] = []

    clean_path = Path(clean_data_folder)
    sf_files = sorted((clean_path / 'single_family').glob('fha_sf_snapshot*.parquet'))
    sf_files = [file for file in sf_files if '201408' not in file.name]
    for file in sf_files:
        logger.info("Get institution data from: %s", file)
        file_date = pd.to_datetime(file.stem.split('_')[-1], format='%Y%m%d')

        sf_originators = (
            pl.scan_parquet(str(file))
            .select(['Originating Mortgagee Number', 'Originating Mortgagee'])
            .rename(
                {
                    'Originating Mortgagee Number': 'Institution_Number',
                    'Originating Mortgagee': 'Institution_Name',
                }
            )
            .with_columns(pl.lit(file_date).alias('File_Date'))
        )
        lazy_frames.append(sf_originators)

        sf_sponsors = (
            pl.scan_parquet(str(file))
            .select(['Sponsor Number', 'Sponsor Name'])
            .rename(
                {
                    'Sponsor Number': 'Institution_Number',
                    'Sponsor Name': 'Institution_Name',
                }
            )
            .with_columns(pl.lit(file_date).alias('File_Date'))
        )
        lazy_frames.append(sf_sponsors)

    hecm_files = sorted((clean_path / 'hecm').glob('fha_hecm_snapshot*.parquet'))
    for file in hecm_files:
        logger.info("Get institution data from: %s", file)
        file_date = pd.to_datetime(file.stem.split('_')[-1], format='%Y%m%d')

        hecm_originators = (
            pl.scan_parquet(str(file))
            .select(['Originating Mortgagee Number', 'Originating Mortgagee'])
            .rename(
                {
                    'Originating Mortgagee Number': 'Institution_Number',
                    'Originating Mortgagee': 'Institution_Name',
                }
            )
            .with_columns(pl.lit(file_date).alias('File_Date'))
        )
        lazy_frames.append(hecm_originators)

        hecm_sponsors = (
            pl.scan_parquet(str(file))
            .select(['Sponsor Number', 'Sponsor Name'])
            .rename(
                {
                    'Sponsor Number': 'Institution_Number',
                    'Sponsor Name': 'Institution_Name',
                }
            )
            .with_columns(pl.lit(file_date).alias('File_Date'))
        )
        lazy_frames.append(hecm_sponsors)

    combined = (
        pl.concat(lazy_frames, how='diagonal_relaxed')
        .unique()
        .drop_nulls()
        .sort(['Institution_Number', 'File_Date', 'Institution_Name'])
        .collect()
    )

    enriched = (
        combined.with_columns(
            pl.col("File_Date")
            .min()
            .over(["Institution_Number", "Institution_Name"])
            .alias("First_Observed"),
            pl.col("File_Date")
            .max()
            .over(["Institution_Number", "Institution_Name"])
            .alias("Last_Observed"),
        )
        .select(
            [
                "Institution_Number",
                "Institution_Name",
                "First_Observed",
                "Last_Observed",
            ]
        )
        .unique()
        .with_columns(
            pl.col("First_Observed").dt.strftime("%Y-%m").alias("First_Observed_Period"),
            pl.col("Last_Observed").dt.strftime("%Y-%m").alias("Last_Observed_Period"),
            pl.col("First_Observed").dt.date().alias("First_Observed"),
            pl.col("Last_Observed").dt.date().alias("Last_Observed"),
        )
    )

    conflict_flags = (
        enriched.group_by("Institution_Number")
        .agg(
            pl.col("Institution_Name")
            .n_unique()
            .alias("Distinct_Name_Count"),
        )
        .with_columns(
            (pl.col("Distinct_Name_Count") > 1).alias("Has_Name_Conflict"),
        )
    )

    crosswalk = (
        enriched.join(conflict_flags, on="Institution_Number", how="left")
        .with_columns(
            pl.col("Distinct_Name_Count").fill_null(1),
            pl.col("Has_Name_Conflict").fill_null(False),
        )
        .sort(["Institution_Number", "First_Observed", "Institution_Name"])
    )

    return crosswalk


def clean_sf_sheets(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean Excel sheets for FHA single-family data using Polars.
    
    Parameters
    ----------
    df : polars DataFrame
        Raw single-family data.
    
    Returns
    -------
    pl.DataFrame
        Cleaned single-family data.
    """
    
    # Initial column name cleaning:
    # 1. Strip whitespace from column names
    # 2. Replace underscores with spaces in column names
    df = df.rename(lambda col: col.strip() if isinstance(col, str) else col)
    df = df.rename(lambda col: col.replace('_', ' ') if isinstance(col, str) else col)
    
    # Rename Columns to Standardize - only rename columns that exist
    rename_dict: dict[str, str] = {
        'Endorsement Month': 'Month',
        'Original Mortgage Amount': 'Mortgage Amount',
        'Origination Mortgagee/Sponsor Originator': 'Originating Mortgagee',
        'Origination Mortgagee Sponsor Or': 'Originating Mortgagee',
        'Orig Num': 'Originating Mortgagee Number',
        'Property/Product Type': 'Property Type',
        'Property Type Final': 'Property Type',
        'Sponosr Number': 'Sponsor Number',
        'Sponsor Num': 'Sponsor Number',
        'Endorsement  Year': 'Year',
        'Endorsment Year': 'Year',
        'Endorsement Year': 'Year',
    }
    rename_dict_filtered = {old: new for old, new in rename_dict.items() if old in df.columns}
    df = df.rename(rename_dict_filtered)

    # Drop unnamed columns
    unnamed_cols = [col for col in df.columns if 'unnamed' in col.lower()]
    if unnamed_cols:
        df = df.drop(unnamed_cols)
    
    # Convert numeric columns
    numeric_columns = [
        'Property Zip',
        'Originating Mortgagee Number',
        'Sponsor Number',
        'Non Profit Number',
        'Interest Rate',
        'Mortgage Amount',
        'Year',
        'Month',
    ]
    
    for column in numeric_columns:
        if column in df.columns:
            df = df.with_columns(
                pl.col(column).cast(pl.Float64, strict=False)
            )
    
    # Drop bad observations
    if 'Loan Purpose' in df.columns:
        df = df.filter(pl.col('Loan Purpose') != 'Loan_Purpose')
    
    # Replace bad loan purposes for 2016
    if 'Loan Purpose' in df.columns:
        df = df.with_columns(
            pl.when(pl.col('Loan Purpose').is_in(['Fixed Rate', 'Adjustable Rate']))
            .then(pl.lit('Purchase'))
            .when(pl.col('Loan Purpose').is_in(['Rehabilitation', 'Single Family']))
            .then(pl.lit('Purchase'))
            .otherwise(pl.col('Loan Purpose'))
            .alias('Loan Purpose')
        )

        # Replace '-' with '_' in Loan Purpose
        # Note: Replaces Refi_Conv-Curr with Refi_Conv_Curr
        df = df.with_columns(
            pl.col('Loan Purpose').str.replace('-', '_').alias('Loan Purpose')
        )
    
    # Standardize down payment types
    if 'Down Payment Source' in df.columns:
        df = df.with_columns(
            pl.when(pl.col('Down Payment Source') == 'NonProfit')
            .then(pl.lit('Non Profit'))
            .when(
                (pl.col('Down Payment Source').cast(pl.Utf8).str.strip_chars() == '') |
                (pl.col('Down Payment Source') == 'nan')
            )
            .then(None)
            .otherwise(pl.col('Down Payment Source'))
            .alias('Down Payment Source')
        )
    
    # Replace loan purpose types
    if 'Loan Purpose' in df.columns:
        df = df.with_columns(
            pl.col('Loan Purpose').str.replace('-', '_').alias('Loan Purpose')
        )
    
    # Fix county names and sponsor names
    if 'Property County' in df.columns:
        df = df.with_columns(
            pl.when(pl.col('Property County') == '#NULL!')
            .then(None)
            .otherwise(pl.col('Property County'))
            .alias('Property County')
        )
    
    if 'Sponsor Name' in df.columns:
        df = df.with_columns(
            pl.when(pl.col('Sponsor Name') == 'Not Available')
            .then(None)
            .otherwise(pl.col('Sponsor Name'))
            .alias('Sponsor Name')
        )
    
    # Convert to appropriate data types based on schema
    fhad = FHADictionary()
    data_types = fhad.single_family.data_types
    
    for column, dtype in data_types.items():
        if column in df.columns:
            # Map string dtypes to polars types
            if dtype == 'str':
                df = df.with_columns(pl.col(column).cast(pl.Utf8))
            elif dtype == 'Int32':
                df = df.with_columns(pl.col(column).cast(pl.Int32))
            elif dtype == 'Int64':
                df = df.with_columns(pl.col(column).cast(pl.Int64))
            elif dtype == 'Int16':
                df = df.with_columns(pl.col(column).cast(pl.Int16))
            elif dtype == 'float64':
                df = df.with_columns(pl.col(column).cast(pl.Float64))
    
    return df


def convert_fha_sf_snapshots(data_folder: Path, save_folder: Path, overwrite: bool = False) -> None:
    """
    Convert raw single-family snapshots to cleaned parquet files using Polars.

    Parameters
    ----------
    data_folder : pathlib.Path
        Directory containing the raw Excel monthly SF snapshots.
    save_folder : pathlib.Path
        Directory where cleaned parquet snapshots are saved.
    overwrite : boolean, optional
        Whether to overwrite output files if a version already exists.
        The default is False.

    Returns
    -------
    None.
    """
    save_folder.mkdir(parents=True, exist_ok=True)

    tasks: list[_SnapshotConversionTask] = []

    # Read data file-by-file
    for year in range(2010, 2099):
        for mon in range(1, 13):
            files = sorted(data_folder.glob(f'fha_sf_snapshot_{year}{mon:02d}01*.xls*'))
            if not files:
                continue

            input_file = files[0]
            output_file = save_folder / f'fha_sf_snapshot_{year}{mon:02d}01.parquet'

            if output_file.exists() and not overwrite:
                logger.info('File %s already exists!', output_file)
                continue

            tasks.append(
                _SnapshotConversionTask(
                    input_file=input_file,
                    output_file=output_file,
                    year=year,
                    month=mon,
                )
            )

    logger.info(f'Found {len(tasks)} files to process')
    if tasks:
        logger.info(f'First file: {tasks[0].input_file}, output: {tasks[0].output_file}')
    
    _run_parallel_conversions(tasks, _convert_single_family_snapshot)


def _convert_single_family_snapshot(task: _SnapshotConversionTask) -> None:
    """Worker function for converting a single-family monthly snapshot using Polars."""

    logger.info('Reading and Converting File: %s', task.input_file)

    try:
        reader = fastexcel.read_excel(task.input_file)
        sheets = reader.sheet_names
        sheets = [x for x in sheets if "Data" in x or "Purchase" in x or "Refinance" in x]
        
        if not sheets:
            logger.warning("No relevant sheets found in %s", task.input_file)
            return
        
        # Read each sheet, convert to polars, and clean it
        frames = []
        for sheet in sheets:
            try:
                df = reader.load_sheet(sheet).to_polars()
                df = clean_sf_sheets(df)
                frames.append(df)
            except Exception as exc:
                logger.warning("Error reading sheet %s from %s: %s", sheet, task.input_file, exc)
                continue
        
        if not frames:
            logger.warning("No valid sheets could be read from %s", task.input_file)
            return

        # Concatenate all sheets
        df = pl.concat(frames, how='diagonal_relaxed')
        
        # Add FHA_Index
        df = df.with_columns(
            (pl.int_range(0, len(df)) + 1).alias('row_num')
        ).with_columns(
            pl.concat_str([
                pl.lit(f'{task.year}{task.month:02d}01_'),
                pl.col('row_num').cast(pl.Utf8).str.zfill(7)
            ]).alias('FHA_Index')
        ).drop('row_num')
        
        # Save to parquet
        df.write_parquet(task.output_file)
        
    except Exception as exc:
        logger.error('Error converting file %s: %s', task.input_file, exc)


def clean_hecm_sheets(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean HECM sheets using Polars.

    Parameters
    ----------
    df : polars DataFrame
        Raw HECM data.

    Returns
    -------
    pl.DataFrame
        Cleaned HECM data.
    """

    # Rename columns
    rename_dict: dict[str, str] = {
        'NMLS*': 'NMLS',
        'Sponosr Number': 'Sponsor Number',
        'Standard Saver': 'Standard/Saver',
        'Purchase /Refinance': 'Purchase/Refinance',
        'Purchase Refinance': 'Purchase/Refinance',
        'Previous Servicer': 'Previous Servicer ID',
        'Endorsement Year': 'Year',
        'Endorsement Month': 'Month',
        'Hecm Type': 'HECM Type',
        'Originating Mortgagee/Sponsor Originator': 'Originating Mortgagee',
        'Originating Mortgagee Sponsor Originator': 'Originating Mortgagee',
        'Originating Mortgagee Sponsor Or': 'Originating Mortgagee',
        'Sponsored Originator': 'Sponsor Originator',
    }
    rename_dict_filtered = {old: new for old, new in rename_dict.items() if old in df.columns}
    df = df.rename(rename_dict_filtered)

    # Drop unnamed columns
    unnamed_cols = [col for col in df.columns if 'unnamed' in col.lower()]
    if unnamed_cols:
        df = df.drop(unnamed_cols)
    
    # Replace "Not Available" and null values with None
    for col in df.columns:
        # if string column, replace 'Not Available' and null values with None
        if df.schema[col] in [pl.Utf8, pl.Categorical, pl.String]:
            df = df.with_columns(
                pl.when(pl.col(col).is_in(['Not Available', 'nan', 'None']))
                .then(pl.lit(None))
                .otherwise(pl.col(col))
                .alias(col)
            )
        
        df = df.with_columns(
            pl.when(pl.col(col).is_null())
            .then(pl.lit(None))
            .otherwise(pl.col(col))
            .alias(col)
        )
    
    # Convert numeric columns
    numeric_cols = [
        'Property Zip',
        'Originating Mortgagee Number',
        'Sponsor Number',
        'NMLS',
        'Interest Rate',
        'Initial Principal Limit',
        'Maximum Claim Amount',
        'Year',
        'Month',
        'Current Servicer ID',
        'Previous Servicer ID',
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False)
            )

    # Convert to appropriate data types based on schema
    fhad = FHADictionary()
    data_types = fhad.hecm.data_types
    
    for column, dtype in data_types.items():
        if column in df.columns:
            # Map string dtypes to polars types
            if dtype == 'str':
                df = df.with_columns(pl.col(column).cast(pl.Utf8))
            elif dtype == 'Int32':
                df = df.with_columns(pl.col(column).cast(pl.Int32))
            elif dtype == 'Int64':
                df = df.with_columns(pl.col(column).cast(pl.Int64))
            elif dtype == 'Int16':
                df = df.with_columns(pl.col(column).cast(pl.Int16))
            elif dtype == 'float64':
                df = df.with_columns(pl.col(column).cast(pl.Float64))

    return df


def convert_fha_hecm_snapshots(data_folder: Path, save_folder: Path, overwrite: bool = False) -> None:
    """
    Convert raw HECM snapshots to cleaned parquet files using Polars.

    Parameters
    ----------
    data_folder : pathlib.Path
        Directory containing the raw Excel monthly HECM snapshots.
    save_folder : pathlib.Path
        Directory where cleaned parquet HECM snapshots are saved.
    overwrite : boolean, optional
        Whether to overwrite output files if a version already exists.
        The default is False.

    Returns
    -------
    None.
    """
    save_folder.mkdir(parents=True, exist_ok=True)

    tasks: list[_SnapshotConversionTask] = []

    # Read data file-by-file
    for year in range(2010, 2099):
        for mon in range(1, 13):
            files = sorted(data_folder.glob(f'fha_hecm_snapshot_{year}{mon:02d}01*.xls*'))
            if not files:
                continue

            input_file = files[0]
            output_file = save_folder / f'fha_hecm_snapshot_{year}{mon:02d}01.parquet'

            if output_file.exists() and not overwrite:
                logger.info('File %s already exists!', output_file)
                continue

            tasks.append(
                _SnapshotConversionTask(
                    input_file=input_file,
                    output_file=output_file,
                    year=year,
                    month=mon,
                )
            )

    _run_parallel_conversions(tasks, _convert_hecm_snapshot)


def _convert_hecm_snapshot(task: _SnapshotConversionTask) -> None:
    """Worker function for converting a HECM monthly snapshot using Polars."""

    logger.info('Reading and Converting File: %s', task.input_file)

    try:
        reader = fastexcel.read_excel(task.input_file)
        sheets = reader.sheet_names
        sheets = [x for x in sheets if "Data" in x or "Purchase" in x or "Refinance" in x or "data" in x]

        if not sheets:
            logger.warning("No relevant sheets found in %s", task.input_file)
            return

        # Read each sheet, convert to polars, and clean it
        frames = []
        for sheet in sheets:
            try:
                df = reader.load_sheet(sheet).to_polars()
                df = clean_hecm_sheets(df)
                frames.append(df)
            except Exception as exc:
                logger.warning("Error reading sheet %s from %s: %s", sheet, task.input_file, exc)
                continue
        
        if not frames:
            logger.warning("No valid sheets could be read from %s", task.input_file)
            return
        
        # Concatenate all sheets
        df = pl.concat(frames)
        
        # Add FHA_Index
        df = df.with_columns(
            (pl.int_range(0, len(df)) + 1).alias('row_num')
        ).with_columns(
            pl.concat_str([
                pl.lit(f'H{task.year}{task.month:02d}01_'),
                pl.col('row_num').cast(pl.Utf8).str.zfill(7)
            ]).alias('FHA_Index')
        ).drop('row_num')
        
        # Save to parquet
        df.write_parquet(task.output_file)
        
    except Exception as exc:
        logger.error('Error saving file %s: %s', task.output_file, exc)


def save_clean_snapshots_to_db(
    data_folder: Path,
    save_folder: Path,
    min_year: int = 2010,
    max_year: int = 2025,
    file_type: SnapshotType = 'single_family',
    add_fips: bool = True,
    add_date: bool = True,
) -> None:
    """
    Saves cleaned snapshots to a database.

    Parameters
    ----------
    data_folder : pathlib.Path
        Location containing the cleaned parquet monthly snapshots.
    save_folder : pathlib.Path
        Destination directory for the hive-partitioned parquet database.
    min_year, max_year : int, optional
        Inclusive range of years to scan when gathering monthly files.
    file_type : {"single_family", "hecm"}, optional
        Indicates which schema adjustments to apply during export.
    add_fips : bool, optional
        When ``True`` (default) county FIPS codes are appended to the output.
    add_date : bool, optional
        When ``True`` (default) a ``Date`` column is synthesized from year and
        month fields.

    Returns
    -------
    None.

    Examples
    --------
    Build the on-disk database after converting raw files:

    >>> from pathlib import Path
    >>> clean_sf = Path("data/clean/single_family")
    >>> db_sf = Path("data/database/single_family")
    >>> save_clean_snapshots_to_db(clean_sf, db_sf, file_type="single_family")

    Restrict the exported range to recent years and skip FIPS enrichment for a
    faster exploratory build:

    >>> save_clean_snapshots_to_db(
    ...     clean_sf,
    ...     db_sf,
    ...     min_year=2020,
    ...     max_year=2024,
    ...     add_fips=False,
    ... )

    """
    
    # Get Files and Combine
    frames: list[pl.LazyFrame] = []
    for year in range(min_year, max_year+1) :
        files = sorted(data_folder.glob(f'fha_*snapshot*{year}*.parquet'))
        for file in files :
            df_a = pl.scan_parquet(str(file))
            frames.append(df_a)
    df = pl.concat(frames, how='diagonal_relaxed')

    
    # Replace null values with empty strings
    for column in ["Originating Mortgagee", "Sponsor Name"]:
        df = df.with_columns(
            pl.when(pl.col(column).is_null())
            .then(pl.lit(""))
            .otherwise(pl.col(column))
            .alias(column)
        )
        df = df.with_columns(
            pl.when(pl.col(column).is_in(["nan", "None"]))
            .then(pl.lit(""))
            .otherwise(pl.col(column))
            .alias(column)
        )

    # Iterate over rows and add FIPS codes to unique_counties
    if add_fips:
        df = add_county_fips(df)

    # Create Datetime Column
    if add_date:
        df = df.with_columns(
            pl.concat_str([
                pl.col('Year').cast(pl.Utf8).str.zfill(4),
                pl.col('Month').cast(pl.Utf8).str.zfill(2),
            ], separator='-').str.to_datetime(format='%Y-%m', strict=False).alias('Date')
        )

    # Replace Sponsor Name with '' for August 2014
    if file_type == 'single_family':
        df = df.with_columns(
            pl.when(pl.col('Date') == datetime.datetime(2014,8,1))
            .then(pl.lit(''))
            .otherwise(pl.col('Sponsor Name'))
            .alias('Sponsor Name')
        )

    # Drop Duplicates: Unclear why there are duplicates in the combined file, but there are.
    df = df.unique()

    # Drop Null Rows in Year and Month
    df = df.drop_nulls(subset=['Year', 'Month'])

    if file_type == 'single_family':
        df = _apply_single_family_categoricals(df)

    # Sink
    df.sink_parquet(
        pl.PartitionByKey(
            save_folder,
            by=['Year','Month'],
            include_key=True,
        ),
        mkdir=True,
    )

@dataclass(frozen=True)
class _SnapshotConversionTask:
    """Encapsulate the information needed to convert a monthly snapshot."""

    input_file: Path
    output_file: Path
    year: int
    month: int


def _run_parallel_conversions(
    tasks: list[_SnapshotConversionTask],
    worker: Callable[[_SnapshotConversionTask], None],
) -> None:
    """Execute snapshot conversion tasks, leveraging multiprocessing when useful."""

    if not tasks:
        return

    # ``spawn`` works across platforms and avoids issues when the project is embedded in
    # other applications. Fallback to a sequential loop if only one task needs work.
    process_count = min(len(tasks), max(1, cpu_count() or 1))

    if process_count <= 1:
        for task in tasks:
            worker(task)
        return

    ctx = get_context("spawn")
    with ctx.Pool(processes=process_count) as pool:
        pool.map(worker, tasks)
