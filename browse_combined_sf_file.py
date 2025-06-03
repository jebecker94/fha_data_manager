# Import Packages
import polars as pl
import glob
import addfips

# Load Data
files = glob.glob("data/fha_combined_sf_originations*.parquet")
df = pl.scan_parquet(files[0])

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
        .otherwise(pl.col("Sponsor Name"))
        .alias("Sponsor Name")
    )

# Tabulate Categorical Columns
for column in ["Loan Purpose", "Property Type", "Down Payment Source", "Product Type","Year","Month"]:
    counts = df.group_by(column).count().sort("count", descending=True)
    print(counts.collect())

# Iterate over rows and add FIPS codes to unique_counties
af = addfips.AddFIPS()
unique_counties = df.select(["Property State", "Property County"]).unique()
county_map = []
for row in unique_counties.collect().iter_rows() :
    df_row = pl.LazyFrame({"Property State": [row[0]], "Property County": [row[1]]})
    fips = af.get_county_fips(row[1], row[0])
    df_row = df_row.with_columns(
        pl.lit(fips).alias("FIPS")
    )
    county_map.append(df_row)
county_map = pl.concat(county_map, how='diagonal_relaxed')
county_map = county_map.sort(["FIPS",'Property State','Property County'])
print(county_map.collect())
df = df.join(county_map, on=["Property State", "Property County"], how="left")

# Tabulate FIPS, State, and County
for column in ["FIPS","Property State","Property County"]:
    counts = df.group_by(column).count().sort("count", descending=True)
    print(counts.collect())

# Find Biggest Cities in California
big_ca = df.filter(pl.col("Property State") == "CA").group_by("Property City").count().sort("count", descending=True)
print(big_ca.collect())

# Find Biggest Counties in California
big_ca = df.filter(pl.col("Property State") == "CA").group_by("Property County").count().sort("count", descending=True)
print(big_ca.collect())

# Display a Historam of Rates
rates = df.filter(pl.col("Interest Rate") >= 0, pl.col("Interest Rate") <= 10).select(pl.col("Interest Rate")).collect()
rates.to_series().hist(bin_count=100)

# Focus on Home Purchase Loans in California
df = df.collect()
ca_purch = df.filter(pl.col('Loan Purpose').is_in(['Purchase']), pl.col('Property State').is_in(['CA']))

# Compute the average loan size by month
ca_purch.group_by(['Year','Month']).agg(pl.col('Loan Amount').mean()) #.sort(['Year','Month']).collect()
