"""Tests for the validation module."""

import pytest
import polars as pl
from fha_data_manager.validation import FHADataValidator, ValidationResult


class TestValidationResult:
    """Test ValidationResult class."""
    
    def test_validation_result_creation(self):
        """Test creating a ValidationResult."""
        result = ValidationResult(
            name="Test Check",
            passed=True,
            details={"count": 100}
        )
        assert result.name == "Test Check"
        assert result.passed is True
        assert result.details == {"count": 100}
        assert result.warning is False
    
    def test_validation_result_repr(self):
        """Test ValidationResult string representation."""
        result = ValidationResult("Test", True, {})
        assert "[PASS]" in str(result)
        
        result = ValidationResult("Test", False, {})
        assert "[FAIL]" in str(result)
        
        result = ValidationResult("Test", False, {}, warning=True)
        assert "[WARN]" in str(result)


class TestFHADataValidator:
    """Test FHADataValidator class."""
    
    def test_validator_initialization(self, sample_data_file):
        """Test validator can be initialized."""
        validator = FHADataValidator(sample_data_file)
        assert validator.data_path == sample_data_file
        assert validator.df is None
        assert len(validator.results) == 0
    
    def test_validator_load_data(self, sample_data_file):
        """Test validator can load data."""
        validator = FHADataValidator(sample_data_file)
        validator.load_data()
        assert validator.df is not None
    
    def test_check_required_columns(self, sample_data_file):
        """Test required columns check."""
        validator = FHADataValidator(sample_data_file)
        validator.load_data()
        result = validator.check_required_columns()
        assert isinstance(result, ValidationResult)
        assert result.passed is True
        assert "missing_columns" in result.details
        assert len(result.details["missing_columns"]) == 0
    
    def test_check_fha_index_uniqueness(self, sample_data_file):
        """Test FHA_Index uniqueness check."""
        validator = FHADataValidator(sample_data_file)
        validator.load_data()
        result = validator.check_fha_index_uniqueness()
        assert isinstance(result, ValidationResult)
        assert result.passed is True
        assert "duplicates" in result.details
        assert result.details["duplicates"] == 0
    
    def test_check_missing_originator_ids(self, sample_data_file):
        """Test missing originator IDs check."""
        validator = FHADataValidator(sample_data_file)
        validator.load_data()
        result = validator.check_missing_originator_ids(threshold_pct=10.0)
        assert isinstance(result, ValidationResult)
        assert "percent_missing" in result.details
    
    def test_run_critical(self, sample_data_file):
        """Test running critical checks only."""
        validator = FHADataValidator(sample_data_file)
        validator.load_data()
        results = validator.run_critical()
        assert len(results) > 0
        assert all(isinstance(r, ValidationResult) for r in validator.results)


def test_validation_with_missing_data():
    """Test validation with missing data."""
    # Create data with missing originator IDs
    data = {
        'Property State': ['CA', 'TX'],
        'Property City': ['LA', 'Houston'],
        'Property County': ['Los Angeles', 'Harris'],
        'Property Zip': [90001, 77001],
        'Originating Mortgagee': ['Lender A', 'Lender B'],
        'Originating Mortgagee Number': [None, 12345],  # One missing
        'Sponsor Name': ['Sponsor X', 'Sponsor Y'],
        'Sponsor Number': [100, 200],
        'Down Payment Source': ['Gift', 'Savings'],
        'Non Profit Number': [None, None],
        'Product Type': ['Standard', 'Standard'],
        'Loan Purpose': ['Purchase', 'Purchase'],
        'Property Type': ['Single Family', 'Single Family'],
        'Interest Rate': [3.5, 3.75],
        'Mortgage Amount': [350000, 275000],
        'Year': [2025, 2025],
        'Month': [6, 6],
        'FHA_Index': ['20250601_00001', '20250601_00002'],
    }
    df = pl.DataFrame(data)
    
    # Save to temp file and test
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        df.write_parquet(tmp.name)
        
        validator = FHADataValidator(tmp.name)
        validator.load_data()
        result = validator.check_missing_originator_ids(threshold_pct=60.0)
        assert result.passed is True  # 50% missing, threshold is 60%
        assert result.details["percent_missing"] == 50.0

