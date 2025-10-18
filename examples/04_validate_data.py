"""
Example 4: Validate FHA Data Quality

This script demonstrates how to run data quality validation checks
on the FHA database to ensure data integrity and consistency.
"""

from fha_data_manager.validation import FHADataValidator


def main() -> None:
    """Run data validation checks."""
    print("=" * 80)
    print("FHA DATA QUALITY VALIDATION")
    print("=" * 80)
    
    # Initialize validator
    print("\nInitializing validator...")
    validator = FHADataValidator("data/database/single_family")
    
    # Load data
    print("Loading data...")
    validator.load_data()
    
    # Run all validation checks
    print("\nRunning all validation checks...")
    print("(This may take a minute...)")
    validator.run_all()
    
    # Print summary
    print("\n")
    all_passed = validator.print_summary()
    
    # Export results
    output_file = "output/validation_results.csv"
    validator.export_results(output_file)
    print(f"\nValidation results exported to: {output_file}")
    
    print("\n" + "=" * 80)
    if all_passed:
        print("✓ All critical validation checks passed!")
    else:
        print("✗ Some validation checks failed - review the report above")
    print("=" * 80)
    print("\nNext step: Run 05_analyze_data.py to analyze the data")


if __name__ == "__main__":
    main()

