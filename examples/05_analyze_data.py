"""
Example 5: Analyze FHA Data

This script demonstrates how to perform exploratory data analysis
and institutional analysis on the FHA database.
"""

from fha_data_manager.analysis import (
    load_combined_data,
    analyze_lender_activity,
    analyze_sponsor_activity,
    analyze_loan_characteristics,
    InstitutionAnalyzer,
)


def run_exploratory_analysis() -> None:
    """Run exploratory data analysis."""
    print("\n" + "=" * 80)
    print("EXPLORATORY DATA ANALYSIS")
    print("=" * 80)
    
    # Load data
    print("\nLoading data...")
    df = load_combined_data("data/database/single_family")
    
    # Analyze lender activity
    print("\nAnalyzing lender activity...")
    lender_stats = analyze_lender_activity(df)
    print("\nTop 10 Lenders by Volume:")
    print(lender_stats['lender_volume'].head(10))
    
    # Analyze sponsor activity
    print("\nAnalyzing sponsor activity...")
    sponsor_stats = analyze_sponsor_activity(df)
    print("\nTop 10 Sponsors by Volume:")
    print(sponsor_stats['sponsor_volume'].head(10))
    
    # Analyze loan characteristics
    print("\nAnalyzing loan characteristics...")
    loan_stats = analyze_loan_characteristics(df)
    print("\nLoan Purpose Distribution:")
    print(loan_stats['loan_purpose'])
    
    print("\n✓ Exploratory analysis complete!")
    print("  Visualizations saved to output/ directory")


def run_institution_analysis() -> None:
    """Run institutional identity analysis."""
    print("\n" + "=" * 80)
    print("INSTITUTIONAL ANALYSIS")
    print("=" * 80)
    
    # Initialize analyzer
    print("\nInitializing institution analyzer...")
    analyzer = InstitutionAnalyzer("data/database/single_family")
    analyzer.load_data()
    
    # Generate comprehensive report
    print("\nGenerating comprehensive institution analysis report...")
    print("(This may take a minute...)")
    analyzer.generate_full_report(output_dir="output")
    
    print("\n✓ Institution analysis complete!")
    print("  Results saved to output/ directory:")
    print("    - institution_crosswalk.csv")
    print("    - institution_mapping_errors.csv")
    print("    - institution_analysis_report.txt")


def main() -> None:
    """Run all analysis examples."""
    print("=" * 80)
    print("FHA DATA ANALYSIS EXAMPLES")
    print("=" * 80)
    
    # Run exploratory analysis
    run_exploratory_analysis()
    
    # Run institution analysis
    run_institution_analysis()
    
    print("\n" + "=" * 80)
    print("All analyses complete!")
    print("=" * 80)
    print("\nCheck the output/ directory for results and visualizations")


if __name__ == "__main__":
    main()

