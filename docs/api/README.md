# API Reference

API documentation for FHA Data Manager modules.

## Module Structure

```
fha_data_manager/
├── __init__.py           # Package exports
├── download_cli.py       # Download functionality
├── import_cli.py         # Import/cleaning functionality
├── analysis/             # Analysis modules
│   ├── exploratory.py   # Exploratory data analysis
│   └── institutions.py   # Institutional analysis
├── validation/           # Validation modules
│   └── validators.py     # Data quality validators
└── utils/                # Utility modules
    ├── config.py         # Configuration management
    └── inventory.py      # Data inventory logging
```

## Main Package (`fha_data_manager`)

### Download Functions

```python
from fha_data_manager import (
    download_single_family_snapshots,
    download_hecm_snapshots,
    SINGLE_FAMILY_SNAPSHOT_URL,
    HECM_SNAPSHOT_URL,
)
```

#### `download_single_family_snapshots()`
Download Single Family snapshots from FHA website.

**Returns**: None (saves files to `data/raw/single_family/`)

#### `download_hecm_snapshots()`
Download HECM snapshots from FHA website.

**Returns**: None (saves files to `data/raw/hecm/`)

### Import Functions

```python
from fha_data_manager.import_cli import (
    import_single_family_snapshots,
    import_hecm_snapshots,
)
```

#### `import_single_family_snapshots()`
Import and clean Single Family snapshots, saving to hive structure.

**Returns**: None (saves to `data/database/single_family/`)

#### `import_hecm_snapshots()`
Import and clean HECM snapshots, saving to hive structure.

**Returns**: None (saves to `data/database/hecm/`)

## Analysis Module (`fha_data_manager.analysis`)

### Exploratory Analysis

```python
from fha_data_manager.analysis import (
    load_combined_data,
    analyze_lender_activity,
    analyze_sponsor_activity,
    analyze_loan_characteristics,
)
```

#### `load_combined_data(data_path: str | Path) -> pl.DataFrame`
Load FHA data from hive structure.

**Parameters**:
- `data_path`: Path to hive-structured parquet directory

**Returns**: Polars DataFrame

#### `analyze_lender_activity(df: pl.DataFrame) -> Dict[str, pl.DataFrame]`
Analyze lender market activity.

**Parameters**:
- `df`: DataFrame with FHA data

**Returns**: Dictionary with keys:
- `'lender_volume'`: Top 20 lenders by volume
- `'yearly_lenders'`: Lender counts by year

#### `analyze_sponsor_activity(df: pl.DataFrame) -> Dict[str, pl.DataFrame]`
Analyze sponsor participation.

**Parameters**:
- `df`: DataFrame with FHA data

**Returns**: Dictionary with keys:
- `'sponsor_volume'`: Top 20 sponsors by volume
- `'yearly_sponsors'`: Sponsor counts by year

#### `analyze_loan_characteristics(df: pl.DataFrame) -> Dict[str, pl.DataFrame]`
Analyze loan characteristics.

**Parameters**:
- `df`: DataFrame with FHA data

**Returns**: Dictionary with keys:
- `'loan_purpose'`: Loan purpose distribution
- `'down_payment'`: Down payment source distribution
- `'yearly_loan_size'`: Loan size statistics by year

### Institution Analysis

```python
from fha_data_manager.analysis.institutions import InstitutionAnalyzer
```

#### `class InstitutionAnalyzer`

**Constructor**: `InstitutionAnalyzer(data_path: str | Path)`

**Methods**:

##### `load_data() -> InstitutionAnalyzer`
Load data from hive structure. Returns self for chaining.

##### `build_institution_crosswalk() -> pl.DataFrame`
Build ID-name crosswalk for institutions.

**Returns**: DataFrame with columns:
- `institution_number`, `institution_name`, `type`
- `first_date`, `last_date`, `num_months`

##### `find_mapping_errors() -> pl.DataFrame`
Find potential mapping errors in institution data.

**Returns**: DataFrame with error details

##### `analyze_name_changes_over_time(notable_ids: List[int] = None, log_file = None) -> Dict`
Analyze how institution names change over time.

**Parameters**:
- `notable_ids`: Specific IDs to analyze in detail
- `log_file`: Optional file path for detailed logging

**Returns**: Dictionary mapping IDs to name change sequences

##### `detect_oscillations(log_file = None) -> Dict[str, List[Dict]]`
Detect oscillating name patterns.

**Returns**: Dictionary with 'originators' and 'sponsors' keys

##### `analyze_id_spaces(log_file = None) -> Dict`
Analyze originator and sponsor ID spaces.

**Returns**: Dictionary with overlap statistics

##### `generate_full_report(output_dir: str | Path = "output") -> None`
Generate comprehensive analysis report.

**Saves**:
- `institution_crosswalk.csv`
- `institution_mapping_errors.csv`
- `institution_analysis_report.txt`

## Validation Module (`fha_data_manager.validation`)

```python
from fha_data_manager.validation import FHADataValidator, ValidationResult
```

### `class ValidationResult`

Stores results of a validation check.

**Attributes**:
- `name: str` - Check name
- `passed: bool` - Whether check passed
- `details: Dict` - Detailed results
- `warning: bool` - If True, failure is a warning not critical error

### `class FHADataValidator`

**Constructor**: `FHADataValidator(data_path: str | Path)`

**Methods**:

##### `load_data() -> FHADataValidator`
Load data from hive structure. Returns self for chaining.

##### `check_required_columns() -> ValidationResult`
Check that all required columns exist.

##### `check_fha_index_uniqueness() -> ValidationResult`
Verify FHA_Index values are unique.

##### `check_missing_originator_ids(threshold_pct: float = 5.0) -> ValidationResult`
Check for missing originator IDs.

##### `check_missing_originator_names(threshold_pct: float = 5.0) -> ValidationResult`
Check for missing originator names.

##### `check_orphaned_sponsors() -> ValidationResult`
Check for sponsors without originator IDs.

##### `check_id_name_consistency_monthly() -> ValidationResult`
Check ID-name consistency within months.

##### `check_overlapping_id_spaces() -> ValidationResult`
Check if originator and sponsor IDs overlap.

##### `check_name_oscillations(min_changes: int = 3) -> ValidationResult`
Check for oscillating institution names.

##### `check_mortgage_amounts() -> ValidationResult`
Validate mortgage amount ranges.

##### `run_all() -> Dict[str, ValidationResult]`
Run all validation checks.

##### `run_critical() -> Dict[str, ValidationResult]`
Run only critical checks.

##### `print_summary() -> bool`
Print validation summary. Returns True if all critical checks passed.

##### `export_results(output_path: str | Path) -> None`
Export results to CSV.

## Utils Module (`fha_data_manager.utils`)

### Configuration

```python
from fha_data_manager.utils import Config
```

#### `class Config`

Centralized configuration management.

**Class Attributes**:
- `PROJECT_DIR: Path` - Project root directory
- `DATA_DIR: Path` - Data directory
- `RAW_DIR: Path` - Raw data directory
- `CLEAN_DIR: Path` - Clean data directory
- `DATABASE_DIR: Path` - Database directory
- `OUTPUT_DIR: Path` - Output directory

**Methods**:

##### `ensure_directories() -> None`
Create all configured directories if they don't exist.

## Type Hints

All functions include proper type hints. Use your IDE's autocomplete or type checker for detailed parameter information.

## Examples

See the [examples/](../../examples/) directory for complete usage examples of all modules.

