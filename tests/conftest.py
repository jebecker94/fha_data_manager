"""Pytest configuration and fixtures for FHA Data Manager tests."""

import pytest
from pathlib import Path
import polars as pl
import tempfile


@pytest.fixture
def sample_single_family_data():
    """Create a small sample of single family data for testing."""
    data = {
        'Property State': ['CA', 'TX', 'NY', 'FL', 'CA'],
        'Property City': ['Los Angeles', 'Houston', 'New York', 'Miami', 'San Diego'],
        'Property County': ['Los Angeles', 'Harris', 'New York', 'Miami-Dade', 'San Diego'],
        'Property Zip': [90001, 77001, 10001, 33101, 92101],
        'Originating Mortgagee': ['Lender A', 'Lender B', 'Lender A', 'Lender C', 'Lender A'],
        'Originating Mortgagee Number': [12345, 23456, 12345, 34567, 12345],
        'Sponsor Name': ['Sponsor X', 'Sponsor Y', 'Sponsor X', 'Sponsor Z', 'Sponsor X'],
        'Sponsor Number': [100, 200, 100, 300, 100],
        'Down Payment Source': ['Gift', 'Savings', 'Gift', 'Gift', 'Savings'],
        'Non Profit Number': [None, None, None, None, None],
        'Product Type': ['Standard', 'Standard', 'Energy Efficient', 'Standard', 'Standard'],
        'Loan Purpose': ['Purchase', 'Purchase', 'Refinance', 'Purchase', 'Purchase'],
        'Property Type': ['Single Family', 'Single Family', 'Condo', 'Single Family', 'Condo'],
        'Interest Rate': [3.5, 3.75, 3.25, 3.6, 3.55],
        'Mortgage Amount': [350000, 275000, 425000, 310000, 380000],
        'Year': [2025, 2025, 2025, 2025, 2025],
        'Month': [6, 6, 6, 6, 6],
        'FHA_Index': ['20250601_00001', '20250601_00002', '20250601_00003', '20250601_00004', '20250601_00005'],
    }
    return pl.DataFrame(data)


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_data_file(sample_single_family_data, temp_data_dir):
    """Save sample data to a temporary parquet file."""
    file_path = temp_data_dir / "test_data.parquet"
    sample_single_family_data.write_parquet(file_path)
    return file_path

