# FHA Data Manager - Tests

This directory contains the test suite for FHA Data Manager.

## Running Tests

### Run all tests:
```bash
pytest tests/
```

### Run with verbose output:
```bash
pytest tests/ -v
```

### Run specific test file:
```bash
pytest tests/test_validation.py
```

### Run specific test:
```bash
pytest tests/test_validation.py::TestFHADataValidator::test_validator_initialization
```

### Run with coverage:
```bash
pytest tests/ --cov=fha_data_manager --cov-report=html
```

## Test Structure

- `conftest.py` - Pytest configuration and fixtures
- `test_validation.py` - Tests for data validation module
- `test_analysis.py` - Tests for analysis modules
- `test_utils.py` - Tests for utility functions
- `fixtures/` - (Future) Sample data files for testing

## Writing Tests

When adding new tests:

1. Use the provided fixtures in `conftest.py`
2. Follow the naming convention `test_*.py` for test files
3. Use descriptive test names that explain what is being tested
4. Group related tests in classes
5. Add docstrings to explain what each test validates

## Test Coverage

Current test coverage focuses on:
- ✓ Validation module (validators)
- ✓ Analysis modules (exploratory, institutions)
- ✓ Utility functions (config)

Future coverage should include:
- [ ] Download functionality
- [ ] Import/cleaning pipeline
- [ ] Schema definitions
- [ ] Integration tests

## Notes

- Tests use sample data fixtures to avoid requiring large datasets
- Temporary directories are used for file I/O tests
- Mock objects should be used for external dependencies (network calls, etc.)

