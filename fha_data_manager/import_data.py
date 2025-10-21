# -*- coding: utf-8 -*-
"""Core import and cleaning functionality for FHA snapshot datasets."""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from multiprocessing import get_context
from os import cpu_count
from pathlib import Path
from typing import Callable, Literal, TypeAlias

import addfips
import numpy as np
import pandas as pd
import polars as pl
from mtgdicts import FHADictionary

PathLike: TypeAlias = Path | str
SnapshotType: TypeAlias = Literal['single_family', 'hecm']

logger = logging.getLogger(__name__)


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
            pl.col('File_Date')
            .min()
            .over(['Institution_Number', 'Institution_Name'])
            .alias('Min_Date'),
            pl.col('File_Date')
            .max()
            .over(['Institution_Number', 'Institution_Name'])
            .alias('Max_Date'),
        )
        .drop(['File_Date'])
        .unique()
    )

    return enriched


# Single-Family
def clean_sf_sheets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean excel sheets for FHA single-family data.
    """

    # Rename Columns to Standardize
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
    df.columns = [x.replace('_', ' ').strip() for x in df.columns]
    df = df.rename(columns=rename_dict, errors='ignore')

    # Drop Unnamed Columns
    unnamed_columns = [x for x in df.columns if 'Unnamed' in x]
    df = df.drop(columns=unnamed_columns)

    # Convert Columns to Numeric
    numeric_columns: list[str] = [
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
            df[column] = pd.to_numeric(df[column], errors='coerce')
    
    # Drop Bad Observations (Only One I Know)
    drop_index = df[df['Loan Purpose'] == 'Loan_Purpose'].index
    df = df.drop(drop_index)

    # Replace Bad Loan Purposes for 2016
    df.loc[df['Loan Purpose'].isin(['Fixed Rate', 'Adjustable Rate']), 'Loan Purpose'] = 'Purchase'
    df.loc[df['Loan Purpose'].isin(['Rehabilitation', 'Single Family']), 'Loan Purpose'] = 'Purchase'

    # Standardize Down Payment Types
    df.loc[df['Down Payment Source'] == 'NonProfit', 'Down Payment Source'] = 'Non Profit'

    # Replace Loan Purpose Types
    df['Loan Purpose'] = [x.replace('-', '_') for x in df['Loan Purpose']]

    # Fix County Names and Sponsor Names
    df.loc[df['Property County'] == '#NULL!', 'Property County'] = None
    df.loc[df['Sponsor Name'] == 'Not Available', 'Sponsor Name'] = None

    # Convert Columns
    fhad = FHADictionary()
    data_types = fhad.single_family.data_types
    for column, dtype in data_types.items():
        if column in df.columns:
            df[column] = df[column].astype(dtype)

    # Return DataFrame
    return df


def convert_fha_sf_snapshots(data_folder: Path, save_folder: Path, overwrite: bool = False) -> None:
    """
    Convert raw single-family snapshots to cleaned parquet files.

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

    # Read Data File-by-File
    for year in range(2010, 2099) :
        for mon in range(1, 13) :
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

    _run_parallel_conversions(tasks, _convert_single_family_snapshot)


def _convert_single_family_snapshot(task: _SnapshotConversionTask) -> None:
    """Worker function for converting a single-family monthly snapshot."""

    logger.info('Reading and Converting File: %s', task.input_file)

    xls = pd.ExcelFile(task.input_file)
    sheets = xls.sheet_names
    sheets = [x for x in sheets if "Data" in x or "Purchase" in x or "Refinance" in x]
    df_sheets: dict[str, pd.DataFrame] = {}
    if sheets:
        df_sheets = pd.read_excel(task.input_file, sheets)

    frames = [clean_sf_sheets(df_s) for df_s in df_sheets.values()]
    if not frames:
        return

    df = pd.concat(frames)
    df['FHA_Index'] = [f'{task.year}{task.month:02d}01_{x:07d}' for x in np.arange(df.shape[0])]
    df.to_parquet(task.output_file, index=False)


# HECM
def clean_hecm_sheets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean HECM sheets.

    Parameters
    ----------
    df : pandas DataFrame
        Raw HECM data.

    Returns
    -------
    df : pandas DataFrame
        Cleaned HECM data.

    """

    # Rename Columns
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
    
    df.columns = [x.replace('_', ' ').strip() for x in df.columns]
    df = df.rename(columns=rename_dict, errors='ignore')

    # Replace Not Available and Replace np.nan Columns with NoneTypes
    for col in df.columns:
        df.loc[df[col] == 'Not Available', col] = None
        df.loc[pd.isna(df[col]), col] = None

    # Convert Columns
    numeric_cols: list[str] = [
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
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert Columns
    fhad = FHADictionary()
    data_types = fhad.hecm.data_types
    for column, dtype in data_types.items():
        if column in df.columns:
            df[column] = df[column].astype(dtype)

    return df


def convert_fha_hecm_snapshots(data_folder: Path, save_folder: Path, overwrite: bool = False) -> None:
    """
    Converts and cleans monthly HECM snapshots, standardizing variable names
    across files.
    Note: Currently, only cleans years >2011. Early years have formatting
    issues that make cleaning difficult.

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

    # Read Data File-by-File
    for year in range(2010, 2099) :
        for mon in range(1, 13) :
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
    """Worker function for converting a HECM monthly snapshot."""

    logger.info('Reading and Converting File: %s', task.input_file)

    xls = pd.ExcelFile(task.input_file)
    sheets = xls.sheet_names
    sheets = [x for x in sheets if "Data" in x or "Purchase" in x or "Refinance" in x or "data" in x]
    df_sheets: dict[str, pd.DataFrame] = {}
    if sheets:
        df_sheets = pd.read_excel(task.input_file, sheets)

    frames = [clean_hecm_sheets(df_s) for df_s in df_sheets.values()]
    if not frames:
        return

    df = pd.concat(frames)
    df['FHA_Index'] = [f'H{task.year}{task.month:02d}01_{x:07d}' for x in np.arange(df.shape[0])]
    try:
        df.to_parquet(task.output_file, index=False)
    except Exception as exc:  # pragma: no cover - defensive logging
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

    # Sink
    df.sink_parquet(
        pl.PartitionByKey(
            # save_folder / "{key[0].name}={key[0].value}/{key[1].name}={key[1].value}/000.parquet",
            save_folder,
            by=[pl.col('Year'), pl.col('Month')],
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
