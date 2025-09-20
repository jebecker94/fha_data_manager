# Import Packages
import glob
import logging

import addfips
import matplotlib.pyplot as plt
import numpy as np
import polars as pl


logger = logging.getLogger(__name__)

# Main Routine
if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    # Load Data
    files = glob.glob("data/fha_combined_sf_originations*.parquet")
    df = pl.scan_parquet(files[0])

    # Tabulate Categorical Columns
    for column in ["Loan Purpose", "Property Type", "Down Payment Source", "Product Type", "Year", "Month",'Date']:
        counts = df.group_by(column).count().sort("count", descending=True)
        logger.info("\n%s", counts.collect())

    # Tabulate FIPS, State, and County
    for column in ["FIPS","Property State","Property County"]:
        counts = df.group_by(column).count().sort("count", descending=True)
        logger.info("\n%s", counts.collect())

    # Count total missing FIPS codes
    missing_count = df.filter(pl.col('FIPS').is_null()).count().collect()
    logger.info("\nTotal records with missing FIPS codes:\n%s", missing_count)

    # Identify Non-missing state/county pairs that lack FIPS codes and filter out rows with missing state or county
    logger.info("\nMost common state/county pairs with missing FIPS codes:")
    missing_fips = df.filter(pl.col('FIPS').is_null()).select(['Property State','Property County'])
    missing_fips = missing_fips.filter([~pl.col('Property State').is_null(), ~pl.col('Property County').is_null()])
    missing_fips = missing_fips.filter([~pl.col('Property State').is_in(['nan','None','']), ~pl.col('Property County').is_in(['nan','None',''])])
    missing_fips = missing_fips.group_by(["Property State", "Property County"]).count().sort("count", descending=True)
    logger.info("\n%s", missing_fips.limit(20).collect())
    mf = missing_fips.collect()

    ## Miscellaneous
    # Find Biggest Cities in California
    big_ca = df.filter(pl.col("Property State") == "CA").group_by("Property City").count().sort("count", descending=True)
    logger.info("\n%s", big_ca.collect())

    # Find Biggest Counties in California
    big_ca = df.filter(pl.col("Property State") == "CA").group_by("Property County").count().sort("count", descending=True)
    logger.info("\n%s", big_ca.collect())

    # Display a Historam of Rates
    rates = df.filter(pl.col("Interest Rate") >= 0, pl.col("Interest Rate") <= 10).select(pl.col("Interest Rate")).collect()
    rates_hist = rates.to_series().hist(bin_count=100)

    # Focus on Home Purchase Loans in California
    ca_purch = df.filter(pl.col('Loan Purpose').is_in(['Purchase']), pl.col('Property State').is_in(['CA']))

    # Compute the average loan size by month
    avg_size_ts = ca_purch.group_by(['Year','Month']).agg(pl.col('Mortgage Amount').mean()).sort(['Year','Month']).collect()

    # Graph the time series of average loan size
    plt.figure(figsize=(10, 6))
    plt.plot(np.arange(len(avg_size_ts)), avg_size_ts['Mortgage Amount'])
    plt.xlabel('Month')
    plt.ylabel('Average Loan Size')
    plt.title('Average Loan Size by Month')
    plt.show()
