# Data Validation and Analysis Usage Guide

This guide covers the new consolidated validation and analysis scripts.

## Overview

### New Scripts

1. **`validations.py`** - Comprehensive data quality validation suite
   - Replaces: `check_common_ids.py`, `check_originator_ids.py`
   - Includes checks from institution mapping analysis

2. **`analyze_institutions.py`** - Institution identity and mapping analysis
   - Replaces: `analyze_institution_mappings.py`, `analyze_name_changes.py`, `analyze_name_oscillations.py`

## Quick Start

### Running Validations

```bash
# Run all validation checks
python validations.py

# Run only critical checks (no warnings)
python validations.py --critical-only

# Export results to CSV
python validations.py --export output/validation_results.csv

# Specify data path
python validations.py --data-path data/database/single_family

# Run specific checks
python validations.py --checks check_missing_originator_ids check_fha_index_uniqueness
```

### Running Institution Analysis

```bash
# Run full comprehensive analysis
python analyze_institutions.py

# Specify output directory
python analyze_institutions.py --output-dir analysis_output

# Build crosswalk only (faster)
python analyze_institutions.py --crosswalk-only

# Specify data path
python analyze_institutions.py --data-path data/database/single_family
```

## Using as Python Modules

### Validation Suite

```python
from validations import FHADataValidator

# Initialize and load data
validator = FHADataValidator("data/database/single_family")
validator.load_data()

# Run all validations
validator.run_all()
validator.print_summary()

# Run specific check
result = validator.check_missing_originator_ids(threshold_pct=3.0)
print(result)
print(result.details)

# Run only critical checks
validator.run_critical()
validator.print_summary()

# Export results
validator.export_results("output/validation_results.csv")
```

### Institution Analysis

```python
from analyze_institutions import InstitutionAnalyzer

# Initialize and load data
analyzer = InstitutionAnalyzer("data/database/single_family")
analyzer.load_data()

# Build crosswalk
crosswalk = analyzer.build_institution_crosswalk()
print(crosswalk)

# Find mapping errors
errors = analyzer.find_mapping_errors()
print(f"Found {len(errors)} errors")

# Analyze name changes for specific IDs
name_changes = analyzer.analyze_name_changes_over_time(
    notable_ids=[71970, 75159]  # Quicken/Rocket, Freedom
)

# Detect oscillations
oscillations = analyzer.detect_oscillations()

# Generate full report
analyzer.generate_full_report(output_dir="output")
```

## Validation Checks

### Critical Checks
- [PASS] **Required Columns Present** - All necessary columns exist
- [PASS] **FHA_Index Uniqueness** - No duplicate FHA_Index values
- [PASS] **ID-Name Consistency Within Months** - IDs don't map to multiple names in same month

### Warning Checks
- [WARN] **Missing Originator IDs Below Threshold** - Completeness check
- [WARN] **Missing Originator Names Below Threshold** - Completeness check
- [WARN] **No Orphaned Sponsors** - Sponsors without originator IDs
- [WARN] **Non-overlapping ID Spaces** - Originator and sponsor IDs don't overlap
- [WARN] **Name Stability** - No oscillating name patterns
- [WARN] **Consistent Originator-ID Mappings** - Originators don't have multiple IDs
- [WARN] **Reasonable Mortgage Amounts** - No zero/negative or extremely high amounts

### Informational Checks
- [INFO] **Sponsor Coverage** - Percentage of loans with sponsors
- [INFO] **Date Coverage** - Temporal span of the dataset

## Institution Analysis Outputs

When running the full institution analysis, the following files are generated:

1. **`institution_crosswalk.csv`**
   - Complete mapping of institution IDs to names
   - Columns: `institution_number`, `institution_name`, `type`, `first_date`, `last_date`, `num_months`
   - Includes both originators and sponsors

2. **`institution_mapping_errors.csv`**
   - Detected mapping inconsistencies
   - Columns: `institution_number`, `date`, `names`, `issue`
   - Issues include: multiple names in same month, oscillations

3. **`institution_analysis_report.txt`**
   - Comprehensive text report with:
     - ID space overlap analysis
     - Detailed name change timelines
     - Oscillation detection
     - Summary statistics

## Tips

1. **Start with validations** - Run `validations.py` first to catch critical issues
2. **Use `--critical-only`** for quick checks during development
3. **Run institution analysis periodically** - It's more comprehensive and slower
4. **Keep exploratory analysis separate** - `analyze_fha_data.py` is for ad-hoc exploration and visualization
5. **Export validation results** - Use `--export` to track validation metrics over time
