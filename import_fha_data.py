# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 11:16:13 2022
Last Modified on Sunday March 23 2025
@author: Jonathan E. Becker
"""

## Setup
# Import Packages
import os
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from mtgdicts import FHADictionary
import config

#%% Local Functions
## Single-Family
# Clean Single Family Sheets
def clean_sf_sheets(df) :
    """
    Clean excel sheets for FHA single-family data.
    """
    
    # Rename Columns
    rename_dict = {'Endorsement Month': 'Month',
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
                   'Endorsement Year': 'Year'}
    df.columns = [x.replace('_',' ').strip() for x in df.columns]
    df.rename(columns = rename_dict, inplace = True, errors = 'ignore')
    
    # Drop Unnamed Columns
    unnamed_columns = [x for x in df.columns if 'Unnamed' in x]
    df.drop(columns = unnamed_columns, inplace = True)

    # Convert Columns to Numeric
    numeric_columns = ['Property Zip',
                       'Originating Mortgagee Number',
                       'Sponsor Number',
                       'Non Profit Number',
                       'Interest Rate',
                       'Mortgage Amount',
                       'Year',
                       'Month']
    for column in numeric_columns :
        if column in df.columns :
            df[column] = pd.to_numeric(df[column], errors = 'coerce')
    
    # Drop Bad Observations (Only One I Know)
    drop_index = df[df['Loan Purpose'] == 'Loan_Purpose'].index
    df.drop(drop_index, inplace = True)

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
    for row in data_types.items() :
        if row[0] in df.columns :
            df[row[0]] = df[row[0]].astype(row[1])

    # Return DataFrame
    return df

# Convert FHA Single-Family Files
def convert_fha_sf_snapshots(data_folder, save_folder, overwrite = False) :
    """
    Converts and cleans monthly HECM snapshots, standardizing variable names
    across files.

    Parameters
    ----------
    data_folder : string
        Path of folder where excel monthly SF snapshots are located.
    save_folder : string
        Path of folder where gzipped csv monthly SF snapshots are saved.
    overwrite : boolean, optional
        Whether to overwrite output files if a version already exists.
        The default is False.

    Returns
    -------
    None.

    """

    # Read Data File-by-File
    for year in range(2010, 2099) :

        for mon in ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'] :

            # Check if Raw File Exists and Convert
            files = glob.glob(f'{data_folder}/FHA_SFSnapshot_{mon}{year}.xls*')
            if files :

                # File Names
                input_file = files[0]
                output_file = f'{save_folder}/fha_sfsnapshot_{mon}{year}.parquet'

                # Convert File if Not Exists or if Overwrite Mode is On
                if not os.path.exists(output_file) or overwrite :

                    # Display Progress
                    print('Reading and Converting File:', input_file)

                    # Read File
                    xls = pd.ExcelFile(input_file)
                    sheets = xls.sheet_names
                    sheets = [x for x in sheets if "Data" in x or "Purchase" in x or "Refinance" in x]
                    df_sheets = pd.read_excel(input_file, sheets)

                    # Read Sheets
                    df = []
                    for df_s in df_sheets.values() :
                        df_s = clean_sf_sheets(df_s)
                        df.append(df_s)

                    # Combine Sheets and Save
                    df = pd.concat(df)
                    
                    # Create FHA Index Variable
                    df['Group ID'] = range(df.shape[0])
                    df['Minimum Group ID'] = df.groupby(['Year','Month'])['Group ID'].transform('min')
                    df['Group ID'] = df['Group ID'] - df['Minimum Group ID']
                    df['FHA Index'] = df['Year'].astype('str') + df['Month'].astype('str').str.zfill(2) + '_' + df['Group ID'].astype('str')
                    df = df.drop(columns = ['Group ID', 'Minimum Group ID'])
                    
                    # Save
                    dt = pa.Table.from_pandas(df, preserve_index=False)
                    pq.write_table(dt, output_file)

                else :

                    # Display Progress
                    print('File', output_file, 'already exists!')

# Combine FHA Single-Family Snapshots
def combine_fha_sf_snapshots(data_folder, save_folder, min_year = 2010, max_year = 2024, file_suffix = '_2012-2023') :
    """
    Combines cleaned monthly SF snapshots into a single file containing all
    years/months.

    Parameters
    ----------
    data_folder : string
        Path of folder where monthly SF snapshots are located.
    save_folder : string
        Path of folder where combined SF snapshots will be saved.
    min_year : integer, optional
        The first year of data to include in combined snapshots.
        The default is 2010.
    max_year : integer, optional
        The last year of data to include in combined snapshots.
        The default is 2024.

    Returns
    -------
    None.

    """

    # Get Yearly Files and Combine
    df = []
    for year in range(min_year, max_year+1) :
        files = glob.glob(f'{data_folder}/fha_sfsnapshot*{year}.parquet')
        for file in files :
            df_a = pq.read_table(file)
            df.append(df_a)
    df = pa.concat_tables(df)

    # Save Combine File
    if file_suffix is None :
        file_suffix = f'_{min_year}-{max_year}'
    save_file = f'{save_folder}/fha_combined_sf_originations{file_suffix}.parquet'
    pq.write_table(df, save_file)

#%% HECM
# Clean Sheets
def clean_hecm_sheets(df) :
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
    rename_dict = {'NMLS*': 'NMLS',
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
    
    df.columns = [x.replace('_',' ').strip() for x in df.columns]
    df.rename(columns = rename_dict, inplace = True, errors = 'ignore')

    # Replace Not Available and Replace np.nan Columns with NoneTypes
    for col in df.columns :
        df.loc[df[col] == 'Not Available', col] = None
        df.loc[pd.isna(df[col]), col] = None

    # Convert Columns
    numeric_cols = ['Property Zip',
                    'Originating Mortgagee Number',
                    'Sponsor Number',
                    'NMLS',
                    'Interest Rate',
                    'Initial Principal Limit',
                    'Maximum Claim Amount',
                    'Year',
                    'Month',
                    'Current Servicer ID',
                    'Previous Servicer ID']
    for col in numeric_cols :
        if col in df.columns :
            df[col] = pd.to_numeric(df[col], errors = 'coerce')

    # Convert Columns
    fhad = FHADictionary()
    data_types = fhad.hecm.data_types
    for row in data_types.items() :
        if row[0] in df.columns :
            df[row[0]] = df[row[0]].astype(row[1])

    return df

# Convert FHA HECM Files
def convert_fha_hecm_snapshots(data_folder, save_folder, overwrite = False) :
    """
    Converts and cleans monthly HECM snapshots, standardizing variable names
    across files.
    Note: Currently, only cleans years >2011. Early years have formatting
    issues that make cleaning difficult.

    Parameters
    ----------
    data_folder : string
        Path of folder where excel monthly HECM snapshots are located.
    save_folder : string
        Path of folder where gzipped csv monthly HECM snapshots are saved.
    overwrite : boolean, optional
        Whether to overwrite output files if a version already exists.
        The default is False.

    Returns
    -------
    None.

    """

    # Read Data File-by-File
    df = []
    for year in range(2012, 2099) :

        for mon in ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'] :

            # Check if Raw File Exists and Convert
            files = glob.glob(f'{data_folder}/FHA_HECMSnapshot_{mon}{year}.xls*')
            if files :

                # File Names
                input_file = files[0]
                output_file = f'{save_folder}/fha_hecmsnapshot_{mon}{year}.parquet'

                # Convert File if Not Exists or if Overwrite Mode is On
                if not os.path.exists(output_file) or overwrite :

                    # Display Progress
                    print('Reading and Converting File:', input_file)

                    # Read File
                    xls = pd.ExcelFile(input_file)
                    sheets = xls.sheet_names
                    sheets = [x for x in sheets if "Data" in x or "Purchase" in x or "Refinance" in x or "data" in x]
                    df_sheets = pd.read_excel(input_file, sheets)

                    # Read Sheets
                    df = []
                    for df_s in df_sheets.values() :
                        df_s = clean_hecm_sheets(df_s)
                        df.append(df_s)

                    # Combine Sheets and Save
                    if df :
                        df = pd.concat(df)

                        # Create FHA Index Variable
                        df['Group ID'] = range(df.shape[0])
                        df['Minimum Group ID'] = df.groupby(['Year','Month'])['Group ID'].transform('min')
                        df['Group ID'] = df['Group ID'] - df['Minimum Group ID']
                        df['FHA Index'] = 'H' + df['Year'].astype('str') + df['Month'].astype('str').str.zfill(2) + '_' + df['Group ID'].astype('str')
                        df = df.drop(columns = ['Group ID', 'Minimum Group ID'])
                    
                        # Save
                        dt = pa.Table.from_pandas(df, preserve_index=False)
                        pq.write_table(dt, output_file)

                else :

                    # Display Progress
                    print('File', output_file, 'already exists!')

# Combine FHA HECM Snapshots
def combine_fha_hecm_snapshots(data_folder, save_folder, min_year=2012, max_year=2024, file_suffix=None) :
    """
    Combines cleaned monthly HECM snapshots into a single file containing all
    years/months.
    Note: Currently, only combines years >2011. Early years have formatting
    issues that make combination difficult.

    Parameters
    ----------
    data_folder : string
        Path of folder where monthly HECM snapshots are located.
    save_folder : string
        Path of folder where combined HECM snapshots will be saved.
    min_year : integer, optional
        The first year of data to include in combined snapshots.
        The default is 2012.
    max_year : integer, optional
        The last year of data to include in combined snapshots.
        The default is 2024.
    file_suffix : str
        The suffix to use for the combined file name. The default is None.

    Returns
    -------
    None.

    """

    # Get Files and Combine
    df = []
    for year in range(min_year, max_year+1) :
        files = glob.glob(f'{data_folder}/fha_hecmsnapshot*{year}.parquet')
        for file in files :
            df_a = pq.read_table(file)
            df.append(df_a)
    df = pa.concat_tables(df, promote = True)

    # Save Combined File
    if file_suffix is None :
        file_suffix = f'_{min_year}-{max_year}'
    save_file = f'{save_folder}/fha_combined_hecm_originations{file_suffix}.parquet'
    pq.write_table(df, save_file)

#%% Main Routine
if __name__ == '__main__' :

    # Set Folder Paths
    DATA_DIR = config.DATA_DIR
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR

    # Create Data Folders
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)

    ## Single Family
    # Convert Snapshots
    DATA_FOLDER = RAW_DIR / 'single_family'
    SAVE_FOLDER = CLEAN_DIR / 'single_family'
    convert_fha_sf_snapshots(DATA_FOLDER, SAVE_FOLDER, overwrite=False)

    # Combine All Months
    DATA_FOLDER = CLEAN_DIR / 'single_family'
    SAVE_FOLDER = DATA_DIR
    combine_fha_sf_snapshots(DATA_FOLDER, SAVE_FOLDER, min_year=2010, max_year=2024, file_suffix='_201006-202411')

    ## HECM
    # Convert HECM Snapshots
    DATA_FOLDER = RAW_DIR / 'hecm'
    SAVE_FOLDER = CLEAN_DIR / 'hecm'
    convert_fha_hecm_snapshots(DATA_FOLDER, SAVE_FOLDER, overwrite=False)

    # Combine All Months
    DATA_FOLDER = CLEAN_DIR / 'hecm'
    SAVE_FOLDER = DATA_DIR
    combine_fha_hecm_snapshots(DATA_FOLDER, SAVE_FOLDER, min_year=2012, max_year=2024, file_suffix='_201201-202410')
