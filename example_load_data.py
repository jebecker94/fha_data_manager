## Single Family
# Import Packages
from pathlib import Path
import polars as pl

# Read Data
df_sf = pl.scan_parquet("data/database/single_family", include_file_paths='FilePath')

# Query Data
df_sf = df_sf.filter(pl.col('Year') == 2025)
df_sf = df_sf.filter(pl.col('Month') == 6)
df_sf = df_sf.collect()

# Print Data
print(df_sf)

## HECM
# Read Data
df_hecm = pl.scan_parquet("data/database/hecm", include_file_paths='FilePath')

# Query Data
df_hecm = df_hecm.filter(pl.col('Year') == 2025)
df_hecm = df_hecm.filter(pl.col('Month') == 6)
df_hecm = df_hecm.collect()

# Print Data
print(df_hecm)
