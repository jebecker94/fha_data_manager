# Contributing to FHA Data Manager

Thank you for your interest in contributing to FHA Data Manager!

## Development Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd fha_data_manager
```

### 2. Install Dependencies

We use `uv` for dependency management:

```bash
# Install uv if needed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

### 3. Set Up Pre-commit Hooks (Recommended)

```bash
uv pip install pre-commit
pre-commit install
```

## Project Structure

```
fha_data_manager/
├── fha_data_manager/       # Main package
│   ├── analysis/           # Analysis modules
│   ├── validation/         # Validation modules
│   └── utils/              # Utility modules
├── tests/                  # Test suite
├── examples/               # Usage examples
├── docs/                   # Documentation
├── data/                   # Data directory (not in repo)
└── output/                 # Output directory (not in repo)
```

## Making Changes

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

Follow these guidelines:

#### Code Style
- Follow PEP 8 style guide
- Use type hints for function parameters and returns
- Add docstrings to all public functions and classes
- Keep functions focused and single-purpose

#### Documentation
- Update relevant documentation in `docs/`
- Add docstrings using Google or NumPy style
- Include examples for new features

#### Tests
- Add tests for new functionality
- Ensure all tests pass: `pytest tests/`
- Maintain or improve code coverage

### 3. Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=fha_data_manager --cov-report=html

# Run specific test file
pytest tests/test_validation.py
```

### 4. Check Code Quality

```bash
# Run linters
ruff check fha_data_manager/

# Format code
ruff format fha_data_manager/

# Type checking (if using mypy)
mypy fha_data_manager/
```

### 5. Commit Changes

Use descriptive commit messages:

```bash
git add .
git commit -m "Add feature: description of change"
```

Follow conventional commits format:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test changes
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

## Adding New Features

### Adding a New Validation Check

1. Add method to `FHADataValidator` class in `fha_data_manager/validation/validators.py`
2. Add the check to `run_all()` method
3. Write tests in `tests/test_validation.py`
4. Document in `docs/usage/validation.md`

Example:

```python
def check_my_validation(self) -> ValidationResult:
    """Check for my specific data quality issue."""
    # Your validation logic here
    stats = self.df.select([...]).collect()
    
    passed = # your logic
    details = {"metric": value}
    
    return ValidationResult("My Check", passed, details)
```

### Adding a New Analysis Function

1. Add function to appropriate module in `fha_data_manager/analysis/`
2. Export from `fha_data_manager/analysis/__init__.py`
3. Write tests in `tests/test_analysis.py`
4. Add example to `examples/05_analyze_data.py`
5. Document in `docs/usage/analysis.md`

### Adding New Data Source Support

1. Update schema in `mtgdicts.py`
2. Add download function if needed
3. Add import/cleaning pipeline
4. Update documentation

## Testing Guidelines

### Writing Tests

- Use pytest fixtures from `tests/conftest.py`
- Test both success and failure cases
- Use descriptive test names
- Group related tests in classes

Example:

```python
class TestMyFeature:
    """Test my new feature."""
    
    def test_feature_with_valid_input(self, sample_data):
        """Test feature works with valid input."""
        result = my_function(sample_data)
        assert result is not None
    
    def test_feature_with_invalid_input(self):
        """Test feature handles invalid input gracefully."""
        with pytest.raises(ValueError):
            my_function(None)
```

### Running Tests

```bash
# All tests
pytest

# Specific file
pytest tests/test_validation.py

# Specific test
pytest tests/test_validation.py::TestValidator::test_load_data

# With coverage
pytest --cov=fha_data_manager

# Stop on first failure
pytest -x

# Verbose output
pytest -v
```

## Documentation Guidelines

### Docstring Format

Use Google-style docstrings:

```python
def my_function(param1: str, param2: int) -> bool:
    """
    Brief description of what the function does.
    
    More detailed description if needed. Can span multiple
    lines and include examples.
    
    Args:
        param1: Description of param1
        param2: Description of param2
    
    Returns:
        Description of return value
    
    Raises:
        ValueError: When invalid input is provided
    
    Example:
        >>> result = my_function("test", 42)
        >>> print(result)
        True
    """
    pass
```

### Updating Documentation

When adding features, update:
- Relevant guide in `docs/usage/`
- API reference in `docs/api/`
- Examples in `examples/`
- Main README if needed

## Pull Request Process

1. **Update tests and documentation**
2. **Ensure all tests pass**
3. **Update CHANGELOG.md** (if applicable)
4. **Create pull request** with description:
   - What changes were made
   - Why the changes were needed
   - How to test the changes
5. **Address review feedback**

## Code Review Guidelines

When reviewing code, check for:
- Correct functionality
- Test coverage
- Documentation completeness
- Code style consistency
- Performance considerations
- Security implications

## Release Process

(For maintainers)

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create git tag: `git tag v0.x.x`
4. Push tag: `git push origin v0.x.x`
5. Build and publish: `uv build && uv publish`

## Questions or Issues?

- Check existing issues in the repository
- Ask questions in discussions
- Reach out to maintainers

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (see LICENSE file).

## Thank You!

Your contributions make this project better for everyone in the FHA data research community.

