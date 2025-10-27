# Import libraries
import polars as pl
from pathlib import Path

# Main routine
if __name__ == '__main__':

    # Set up folders
    PROJECT_DIR = Path(__file__).parent.parent

    # Load data
    df_hecm = pl.scan_parquet(PROJECT_DIR / 'data/silver/hecm')

    # Collect Data
    df_hecm = df_hecm.collect()

    # Sort by index
    df_hecm = df_hecm.sort(by=['FIPS','Date'], descending=[True, False])

    #%% Browse categorical columns
    columns = ['RefinanceType','Rate Type','Standard/Saver','Purchase/Refinance','HECM Type']
    for column in columns :
        df_temp = pl.scan_parquet(PROJECT_DIR / 'data/silver/hecm')
        df_temp = df_temp.select(pl.col(column))
        print('Column summary for column:',
            column,
            '\n',
            df_temp.select(pl.col(column).value_counts()).collect(),
        )
