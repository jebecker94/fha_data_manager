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

bad = []
for p in Path("data/database/hecm").rglob("*.parquet"):
    try:
        df_temp = pl.read_parquet(p)  # or use scan_parquet(p).fetch(1)
        print(df_temp)
    except Exception as e:
        bad.append((str(p), repr(e)))

print(f"{len(bad)} bad files")
for path, err in bad[:20]:
    print(path, "->", err)

# Read Data
df_hecm = pl.scan_parquet("data/database/hecm", include_file_paths='FilePath')

# Query Data
df_hecm = df_hecm.filter(pl.col('Year') == 2025)
df_hecm = df_hecm.filter(pl.col('Month') == 6)
df_hecm = df_hecm.collect()

# Print Data
print(df_hecm)
