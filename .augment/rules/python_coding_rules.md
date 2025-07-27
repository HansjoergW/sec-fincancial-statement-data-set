---
type: "agent_requested"
description: "Python coding standards and guidelines for the secfsdstools project"
---

# Python Coding Rules - secfsdstools Project

## Overview
This document defines the coding standards and guidelines for the secfsdstools project. These rules are enforced through automated tools (Ruff, Pylint, Black) and should be followed by all contributors.

## Code Formatting and Style

### Line Length and Formatting
- **Maximum line length**: 120 characters (configured in Black and Pylint)
- **Code formatter**: Black (automatically applied on save in VSCode)
- **Import organization**: Ruff handles import sorting and unused import removal
- **Indentation**: 4 spaces (Python standard)

### Import Organization
- Imports are automatically organized by Ruff on save
- Remove unused imports automatically
- Group imports in this order:
  1. Standard library imports
  2. Third-party imports  
  3. Local application imports
- Use absolute imports when possible

### Naming Conventions
- **Classes**: PascalCase (e.g., `CompanyIndexReader`)
- **Functions/Methods**: snake_case (e.g., `get_company_collector`)
- **Variables**: snake_case (e.g., `apple_cik`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `LOGGER`)
- **Private methods**: prefix with underscore (e.g., `_calculate_daily_start_quarter`)

## Code Quality Standards

### Documentation
- **All public classes and methods must have docstrings**
- Use Google-style docstrings for consistency
- Include parameter types and return types in docstrings
- Document complex business logic with inline comments

### Error Handling
- Use specific exception types rather than generic `Exception`
- Provide meaningful error messages
- Log errors appropriately using the configured logger
- Handle edge cases explicitly

### Type Hints
- Use type hints for all function parameters and return values
- Import types from `typing` module when needed
- Use `Optional[Type]` for nullable parameters
- Consider using `Union` types when appropriate

## Linting Configuration

### Ruff Settings (Applied to src/ only)
- **Enabled rules**:
  - F401: unused imports (automatically fixed)
  - E: pycodestyle errors
  - F: pyflakes rules
  - I: isort (import sorting)
  - W: pycodestyle warnings
- **Ignored rules**:
  - E501: line too long (handled by Black)
- **Scope**: Only `src/` directory (excludes tests and sandbox)

### Pylint Settings

#### For Source Code (src/)
- **Disabled rules**:
  - R0801: duplicate-code
  - R0902: too-many-instance-attributes
  - R0903: too-few-public-methods
  - R0912: too-many-branches
  - R0913: too-many-arguments
  - R0917: too-many-positional-arguments
  - W0511: fixme comments

#### For Tests (tests/)
- **Additional disabled rules** (more relaxed):
  - C0103: invalid-name
  - C0111, C0114, C0115, C0116: missing docstrings
  - C0301: line-too-long
  - W0212: protected-access (common in tests)
  - W0612, W0613: unused variables/arguments (pytest fixtures)
  - W0621: redefined-outer-name (pytest fixtures)

## Project Structure Guidelines

### Directory Organization
```
src/secfsdstools/          # Main source code
├── a_utils/               # Utility modules
├── b_setup/               # Setup and configuration
├── c_index/               # Index-related functionality
├── d_container/           # Data container models
├── e_collector/           # Data collection logic
├── f_standardize/         # Data standardization
├── g_pipelines/           # Processing pipelines
└── x_examples/            # Example scripts

tests/                     # Test files (mirror src structure)
sandbox/                   # Development and testing scripts
```

### Module Organization
- Keep modules focused on a single responsibility
- Use clear, descriptive module names
- Group related functionality together
- Avoid circular imports

## Development Workflow

### VSCode Integration
- **Format on save**: Enabled (Black formatter)
- **Auto-fix on save**: Ruff fixes imports and basic issues
- **Testing**: pytest integration enabled
- **Linting**: Both Ruff and Pylint enabled

### Pre-commit Checks
The CI pipeline runs:
1. Pylint on `src/` with main configuration
2. Pylint on `tests/` with relaxed configuration  
3. Pytest test suite

### Dependencies
- **Python version**: >=3.10
- **Core dependencies**: pandas, numpy, requests, pyarrow, etc.
- **Dev dependencies**: pytest, pylint, ruff, black, coverage

## Best Practices

### Performance
- Use pandas operations efficiently
- Avoid unnecessary loops when vectorized operations are available
- Consider memory usage for large datasets
- Use appropriate data types (e.g., category for repeated strings)

### Security
- Validate input parameters
- Use secure methods for file operations
- Be cautious with user-provided paths
- Log security-relevant events

### Maintainability
- Write self-documenting code
- Use meaningful variable and function names
- Keep functions small and focused
- Avoid deep nesting (max 3-4 levels)
- Use early returns to reduce complexity

### Testing
- Follow the testing guidelines in `.augment/rules/testing.md`
- Write tests for all public methods
- Use pytest fixtures for common setup
- Test edge cases and error conditions

## Code Review Guidelines

### What to Look For
- Adherence to these coding standards
- Proper error handling
- Adequate test coverage
- Clear documentation
- Performance considerations
- Security implications

### Automated Checks
- Ruff and Pylint must pass without errors
- All tests must pass
- Code coverage should be maintained
- Black formatting is automatically applied

## Tools and Configuration Files

### Configuration Files
- `pyproject.toml`: Main configuration for tools and project metadata
- `tests/.pylintrc`: Relaxed pylint rules for tests
- `.vscode/settings.json`: VSCode-specific settings
- `.vscode/launch.json`: Debug configurations

### Development Commands
```bash
# Run linting
poetry run pylint src
poetry run pylint --rcfile tests/.pylintrc tests

# Run tests
poetry run pytest tests/ -v

# Format code (automatic in VSCode)
poetry run black src/ tests/

# Fix imports and basic issues
poetry run ruff check --fix src/
```

This document should be reviewed and updated as the project evolves and new requirements emerge.
