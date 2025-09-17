# Contributing to icat-data-quality

Thank you for your interest in contributing to icat-data-quality! This module follows the **OSS bricks** philosophy - small, focused, production-safe utilities designed for reliability and maintainability.

## 🎯 Our Philosophy

- **TDD First**: Write tests, then implementation
- **Small Scope**: Keep changes focused and isolated
- **Production Safe**: No silent failures, clear error modes
- **Well Documented**: Clear APIs with examples
- **Zero Breaking Changes**: Maintain backward compatibility

## 🚀 Quick Start

1. **Fork and clone** the repository
2. **Install dependencies**:
   ```bash
   poetry install
   ```
3. **Run tests** to ensure everything works:
   ```bash
   poetry run pytest
   ```
4. **Make your changes** following the guidelines below
5. **Submit a pull request**

## 📋 Development Workflow

### 1. Environment Setup

```bash
git clone https://github.com/your-username/icat-data-quality.git
cd icat-data-quality
poetry install
```

### 2. Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src/icat_data_quality

# Run specific test file
poetry run pytest tests/test_null_scan.py

# Run with verbose output
poetry run pytest -v
```

### 3. Code Quality Checks

```bash
# Linting
poetry run ruff check .

# Formatting
poetry run ruff format .

# Type checking
poetry run mypy src/

# Security scanning
poetry run bandit -r src/
```

### 4. Pre-commit Hooks

We use pre-commit to ensure code quality:

```bash
# Install pre-commit hooks
poetry run pre-commit install

# Run all hooks on staged files
poetry run pre-commit run

# Run all hooks on all files
poetry run pre-commit run --all-files
```

## 📝 Code Standards

### Python Style

- **Python 3.8+** compatibility required
- **Type hints** for all public APIs
- **Docstrings** following Google style
- **Line length**: 88 characters (Black default)
- **Import sorting**: isort configuration

### Testing Standards

- **TDD approach**: Write tests first
- **Test coverage**: Aim for 90%+ coverage
- **Test naming**: Descriptive test names
- **Fixtures**: Use pytest fixtures for common setup
- **Parametrized tests**: For multiple input scenarios

### Example Test Structure

```python
import pytest
from icat_data_quality import quick_null_scan

def test_quick_null_scan_basic_functionality():
    """Test basic null scanning functionality."""
    # Arrange
    engine = create_test_engine()
    setup_test_data(engine)
    
    # Act
    result = quick_null_scan(engine, table_patterns=["test_table"])
    
    # Assert
    assert "test_table" in result
    assert result["test_table"]["id"] == 0

@pytest.mark.parametrize("table_patterns,expected_tables", [
    (["users"], ["users"]),
    (["prod_*"], ["prod_orders", "prod_users"]),
    (None, ["users", "orders", "products"]),
])
def test_quick_null_scan_table_patterns(table_patterns, expected_tables):
    """Test table pattern filtering."""
    # Test implementation
```

## 🔧 Making Changes

### 1. Feature Development

1. **Create a feature branch**:
   ```bash
   git checkout -b feat/your-feature-name
   ```

2. **Write tests first**:
   ```python
   def test_new_feature():
       # Write failing test
       result = new_feature_function()
       assert result == expected_value
   ```

3. **Implement the feature**:
   ```python
   def new_feature_function():
       # Implementation
       return expected_value
   ```

4. **Update documentation**:
   - Add docstrings
   - Update README if needed
   - Add examples

### 2. Bug Fixes

1. **Create a bug fix branch**:
   ```bash
   git checkout -b fix/bug-description
   ```

2. **Write regression test**:
   ```python
   def test_bug_fix():
       # Test that reproduces the bug
       with pytest.raises(ExpectedException):
           buggy_function()
   ```

3. **Fix the bug** and ensure tests pass

### 3. Documentation Updates

- Update docstrings for API changes
- Add examples for new features
- Update README for significant changes
- Keep installation instructions current

## 📤 Submitting Changes

### Pull Request Guidelines

1. **Clear title**: Use conventional commit format
   - `feat: add new data quality check`
   - `fix: handle edge case in null scanning`
   - `docs: update installation instructions`

2. **Description template**:
   ```markdown
   ## Context
   Brief description of the problem/feature

   ## Changes
   - List of specific changes made
   - Any breaking changes (should be rare)

   ## Tests
   - [ ] Unit tests added/updated
   - [ ] All tests passing
   - [ ] Coverage maintained

   ## Risk Assessment
   - Low/Medium/High risk
   - Potential impact on existing users

   ## Rollback Plan
   How to revert if issues arise

   ## How to Test
   Steps to verify the changes work
   ```

3. **Code review checklist**:
   - [ ] Tests pass locally
   - [ ] Linting passes
   - [ ] Type checking passes
   - [ ] Documentation updated
   - [ ] No breaking changes (unless explicitly needed)

### Commit Message Format

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): description

[optional body]

[optional footer]
```

Examples:
- `feat(null-scan): add support for custom column patterns`
- `fix(mysql): handle connection timeout gracefully`
- `docs: update quickstart examples`
- `test: add edge case coverage for empty tables`

## 🚨 Security Considerations

- **No secrets in code**: Use environment variables
- **Input validation**: Validate all user inputs
- **SQL injection prevention**: Use parameterized queries only
- **Error handling**: Don't expose sensitive information in errors

## 📊 Release Process

### Version Bumping

We follow [Semantic Versioning](https://semver.org/):

- **Patch** (1.0.1): Bug fixes, no breaking changes
- **Minor** (1.1.0): New features, backward compatible
- **Major** (2.0.0): Breaking changes

### Release Checklist

- [ ] All tests passing
- [ ] Documentation updated
- [ ] Version bumped in `pyproject.toml`
- [ ] CHANGELOG updated
- [ ] Release notes written
- [ ] PyPI release created

## 🤝 Community Guidelines

### Code of Conduct

We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

### Getting Help

- **Issues**: Use GitHub issues for bugs and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Security**: Report security issues privately to maintainers

### Recognition

Contributors will be recognized in:
- README contributors section
- Release notes
- GitHub contributors page

## 📚 Additional Resources

- [Agent Playbook V2](../docs/AGENT_PLAYBOOK_V2.md) - Our development philosophy
- [Architecture Documentation](../docs/architecture.md) - System design principles
- [Testing Guide](../docs/testing.md) - Testing best practices

---

Thank you for contributing to icat-data-quality! Your contributions help make data quality tools more accessible and reliable for everyone.
