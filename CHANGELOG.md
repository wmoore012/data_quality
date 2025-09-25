<!-- SPDX-License-Identifier: MIT
Copyright (c) 2024 MusicScope -->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Enhanced documentation with detailed usage examples
- Improved README with vision statement and roadmap
- Comprehensive contributing guidelines

### Changed
- Updated CI/CD pipeline for better reliability
- Enhanced test coverage and quality checks

## [1.0.0] - 2025-01-XX

### Added
- Initial release of icat-data-quality
- `quick_null_scan()` function for database null detection
- Support for MySQL and SQLite databases
- Configurable table patterns and column matching
- Parameterized SQL queries for security
- Comprehensive test suite with 90%+ coverage

### Features
- Fast null value detection in key columns
- Database-agnostic implementation
- Explainable results with clear output format
- Production-ready with proper error handling
- Type-safe implementation with full mypy support

### Security
- SQL injection prevention through parameterized queries
- No string concatenation in SQL paths
- Input validation and sanitization

## [0.1.0] - 2024-12-XX

### Added
- Initial development version
- Basic null scanning functionality
- Core database utilities
- Foundation for data quality framework

---

## Release Process

### Version Bumping

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
- [ ] GitHub release created

---

For detailed information about changes, see the [commit history](https://github.com/your-org/icat-data-quality/commits/main).
