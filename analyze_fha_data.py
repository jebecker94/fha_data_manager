# Import Packages
import glob
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import seaborn as sns


logger = logging.getLogger(__name__)

def load_combined_data(data_path: Path) -> pd.DataFrame:
    """
    Load the combined FHA single-family data.
    
    Args:
        data_path: Path to the combined data parquet file
        
    Returns:
        DataFrame containing the combined data
    """
    logger.info("Loading data from %s...", data_path)
    df = pq.read_table(data_path).to_pandas()
    logger.info("Loaded %s records", f"{len(df):,}")
    return df

def analyze_lender_activity(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Analyze lender activity in the FHA single-family program.
    
    Returns dictionary of DataFrames with various lender metrics.
    """
    results = {}
    
    # Top lenders by volume
    lender_volume = df.groupby('Originating Mortgagee').agg({
        'FHA Index': 'count',
        'Mortgage Amount': 'sum'
    }).sort_values('FHA Index', ascending=False)
    
    lender_volume.columns = ['Loan Count', 'Total Volume']
    lender_volume['Average Loan Size'] = lender_volume['Total Volume'] / lender_volume['Loan Count']
    results['lender_volume'] = lender_volume.head(20)

    # Lender activity by year
    yearly_lenders = df.groupby('Year').agg({
        'Originating Mortgagee': 'nunique',
        'FHA Index': 'count'
    })
    yearly_lenders.columns = ['Active Lenders', 'Total Loans']
    yearly_lenders['Avg Loans per Lender'] = yearly_lenders['Total Loans'] / yearly_lenders['Active Lenders']
    results['yearly_lenders'] = yearly_lenders

    return results

def analyze_sponsor_activity(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Analyze sponsor activity in the FHA single-family program.
    
    Returns dictionary of DataFrames with various sponsor metrics.
    """
    results = {}
    
    # Filter for sponsored loans
    sponsored_loans = df[df['Sponsor Name'].notna()]
    
    # Top sponsors by volume
    sponsor_volume = sponsored_loans.groupby('Sponsor Name').agg({
        'FHA Index': 'count',
        'Mortgage Amount': 'sum'
    }).sort_values('FHA Index', ascending=False)
    
    sponsor_volume.columns = ['Loan Count', 'Total Volume']
    sponsor_volume['Average Loan Size'] = sponsor_volume['Total Volume'] / sponsor_volume['Loan Count']
    results['sponsor_volume'] = sponsor_volume.head(20)

    # Sponsorship trends by year
    yearly_sponsors = sponsored_loans.groupby('Year').agg({
        'Sponsor Name': 'nunique',
        'FHA Index': 'count'
    })
    yearly_sponsors.columns = ['Active Sponsors', 'Sponsored Loans']
    results['yearly_sponsors'] = yearly_sponsors

    return results

def analyze_loan_characteristics(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Analyze loan characteristics in the FHA single-family program.
    
    Returns dictionary of DataFrames with various loan metrics.
    """
    results = {}
    
    # Loan purpose distribution
    loan_purpose = df['Loan Purpose'].value_counts()
    results['loan_purpose'] = pd.DataFrame(loan_purpose)

    # Down payment source distribution
    down_payment = df['Down Payment Source'].value_counts()
    results['down_payment'] = pd.DataFrame(down_payment)

    # Average loan size by year
    yearly_loan_size = df.groupby('Year')['Mortgage Amount'].agg(['mean', 'median', 'std'])
    results['yearly_loan_size'] = yearly_loan_size

    return results

def print_summary_statistics(stats_dict: Dict[str, pd.DataFrame], section: str):
    """Print formatted summary statistics."""
    logger.info("\n%s\n%s\n%s", "=" * 80, section, "=" * 80)
    for name, df in stats_dict.items():
        logger.info("\n%s:", name.replace('_', ' ').title())
        logger.info("\n%s", df)

def main():

    # Load the data
    files = glob.glob("data/fha_combined_sf_originations*.parquet")
    data_path = files[0]
    df = load_combined_data(data_path)
    
    # Perform analyses
    lender_stats = analyze_lender_activity(df)
    sponsor_stats = analyze_sponsor_activity(df)
    loan_stats = analyze_loan_characteristics(df)
    
    # Print results
    print_summary_statistics(lender_stats, "Lender Activity Analysis")
    print_summary_statistics(sponsor_stats, "Sponsor Activity Analysis")
    print_summary_statistics(loan_stats, "Loan Characteristics Analysis")
    
    # Create some visualizations
    plt.style.use('seaborn')
    
    # Plot 1: Active Lenders Over Time
    fig, ax = plt.subplots(figsize=(12, 6))
    lender_stats['yearly_lenders']['Active Lenders'].plot(kind='line', marker='o')
    plt.title('Number of Active FHA Lenders by Year')
    plt.ylabel('Number of Active Lenders')
    plt.grid(True)
    plt.savefig('output/active_lenders_trend.png')
    
    # Plot 2: Average Loan Size Over Time
    fig, ax = plt.subplots(figsize=(12, 6))
    loan_stats['yearly_loan_size']['mean'].plot(kind='line', marker='o')
    plt.title('Average FHA Loan Size by Year')
    plt.ylabel('Average Loan Amount ($)')
    plt.grid(True)
    plt.savefig('output/avg_loan_size_trend.png')
    
    # Plot 3: Loan Purpose Distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    loan_stats['loan_purpose'].plot(kind='bar')
    plt.title('Distribution of FHA Loans by Purpose')
    plt.ylabel('Number of Loans')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('output/loan_purpose_dist.png')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()