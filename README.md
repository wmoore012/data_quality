# data-quality

[![CI](https://img.shields.io/github/actions/workflow/status/wmoore012/data_quality/ci.yml?branch=main)](https://github.com/wmoore012/data_quality/actions)
[![PyPI](https://img.shields.io/pypi/v/data-quality)](https://pypi.org/project/data-quality/)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

**🔍 Intelligent database quality scanner with AI-powered schema recommendations**

Automatically detect data quality issues, suggest schema improvements, and generate SQL fixes for MySQL, PostgreSQL, and SQLite databases. Get actionable insights about null values, orphaned records, normalization opportunities, and boolean optimization suggestions.

---

## 🚀 Quick Start

```bash
pip install data-quality
```

### Basic Health Check
```bash
# Quick database health scan
data-quality check --database-url mysql://user:pass@host/db

# Scan specific tables only
data-quality check --database-url postgresql://user:pass@host/db --tables "users,orders"

# JSON output for automation
data-quality check --format json > health_report.json
```

### Schema Analysis & Recommendations
```bash
# Analyze table schema with colorful output
data-quality analyze --database-url mysql://user:pass@host/db --table users

# Generate ALTER TABLE statements
data-quality analyze --table songs --generate-sql

# Disable specific recommendation types
data-quality analyze --table products --no-boolean-suggestions --no-normalization
```

### Multi-Table Suggestions
```bash
# Get comprehensive improvement suggestions
data-quality suggest --database-url mysql://user:pass@host/db --tables "users,orders,products"

# Include experimental AI recommendations
data-quality suggest --tables "songs,artists" --use-ai
```

---

## 🎯 Features

### 🔍 **Data Quality Scanning**
- **Null Detection**: Find missing values in critical columns (IDs, keys, ISRCs)
- **Orphan Records**: Detect foreign keys pointing to deleted records
- **Duplicate Detection**: Identify duplicate values in unique columns
- **Severity Prioritization**: Critical → Warning → Info classification

### 🧠 **Intelligent Schema Analysis**
- **Natural Key Detection**: Automatically identify business keys (emails, ISRCs, SKUs)
- **Boolean Optimization**: Suggest `status` → `is_active` conversions
- **Normalization Recommendations**: Extract repeated attributes into separate tables
- **Fact Table Detection**: Identify dimensional modeling opportunities

### 🎨 **Beautiful CLI Interface**
- **Colorful Output**: Priority-coded recommendations with emojis
- **SQL Generation**: Ready-to-run ALTER TABLE statements
- **Flexible Options**: Turn off specific recommendation types
- **Multiple Formats**: Text and JSON output

### 🗄️ **Database Support**
- **MySQL**: Full support with performance optimizations
- **PostgreSQL**: Complete compatibility with advanced features
- **SQLite**: Perfect for development and testing

---

## 📊 Usage Examples

### Python API
```python
from data_quality import health_check, analyze_schema, scan_nulls

# Comprehensive health check
report = health_check("mysql://user:pass@host/db")
if report.all_good:
    print("✅ All good!")
else:
    for issue in report.issues_by_severity:
        print(f"{issue.severity}: {issue.description}")

# Schema analysis with options
analysis = analyze_schema(
    "postgresql://user:pass@host/db", 
    "users",
    include_normalization=True,
    include_boolean_suggestions=True,
    include_fact_analysis=False  # Disable fact table analysis
)

print(f"Natural keys: {analysis.natural_keys}")
print(f"Boolean suggestions: {analysis.suggested_booleans}")

# Targeted null scanning
null_issues = scan_nulls("sqlite:///app.db", table_patterns=["user_%"])
for issue in null_issues:
    print(f"Found {issue.count} nulls in {issue.table}.{issue.column}")
```

### Environment Variables
```bash
# Set once, use everywhere
export DATABASE_URL="mysql://user:pass@host/db"

data-quality check
data-quality analyze --table users
data-quality suggest --tables "songs,artists,albums"
```

### Real-World Music Database Example
```bash
# Analyze music catalog schema
data-quality analyze --table songs --generate-sql

# Output:
# 🔍 Schema Analysis for table: songs
# ==================================================
# 
# 🔑 Natural Keys Found: isrc, spotify_id
# 
# 💡 Suggested Boolean Conversions:
#    • explicit → is_explicit (currently: true/false)
#      SQL: ALTER TABLE songs ADD COLUMN is_explicit BOOLEAN;
#    • fetched_at → is_fetched (currently: NULL/datetime)
#      SQL: ALTER TABLE songs ADD COLUMN is_fetched BOOLEAN;
# 
# 📊 Fact Table Candidate - Consider dimensional modeling
# 
# 🚀 Recommendations:
#    1. 🟡 MEDIUM: Extract artist attributes into separate artists table
#       Benefits: Reduces data redundancy, Improves data consistency
```

---

## ⚙️ Configuration Options

### Disable Recommendations
```bash
# Turn off specific recommendation types
data-quality check --no-recommendations
data-quality analyze --table users --no-normalization --no-boolean-suggestions
data-quality suggest --tables "orders" --no-ai
```

### Database Connection Examples
```bash
# MySQL
data-quality check --database-url "mysql://user:password@localhost:3306/mydb"

# PostgreSQL  
data-quality check --database-url "postgresql://user:password@localhost:5432/mydb"

# SQLite
data-quality check --database-url "sqlite:///path/to/database.db"

# With SSL and options
data-quality check --database-url "mysql://user:pass@host/db?ssl=true&charset=utf8mb4"
```

### Output Formats
```bash
# Human-readable (default)
data-quality check

# JSON for automation/CI
data-quality check --format json | jq '.issues[] | select(.severity=="critical")'

# Generate SQL fixes
data-quality analyze --table users --generate-sql > fixes.sql
```

---

## 🎨 CLI Features

### Colorful Priority System
- 🔴 **Critical**: Primary key nulls, orphaned records
- 🟡 **Warning**: High null percentages, normalization opportunities  
- 🟢 **Info**: Minor suggestions, optimization hints

### Smart Recommendations
- **Boolean Optimization**: `status` → `is_active`, `fetched_at` → `is_fetched`
- **Natural Keys**: Detect emails, ISRCs, SKUs, external IDs
- **Normalization**: Extract `artist_name, artist_country` → `artists` table
- **Fact Tables**: Identify metrics tables for dimensional modeling

### SQL Generation
```sql
-- Example generated fixes
ALTER TABLE songs ADD COLUMN is_explicit BOOLEAN;
UPDATE songs SET is_explicit = (explicit = 'true');
ALTER TABLE songs DROP COLUMN explicit;

-- Normalization example
CREATE TABLE artists (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT
);
ALTER TABLE songs ADD COLUMN artist_id INTEGER REFERENCES artists(id);
```

---

## 🔧 Installation & Setup

### Requirements
- Python 3.8+
- SQLAlchemy 2.0+
- Click for CLI interface

### Database Drivers
```bash
# MySQL
pip install data-quality[mysql]  # Includes PyMySQL

# PostgreSQL  
pip install data-quality[postgresql]  # Includes psycopg2

# All databases
pip install data-quality[all]
```

### Development Setup
```bash
git clone https://github.com/wmoore012/data_quality.git
cd data_quality
pip install -e ".[dev]"
pytest  # Run tests
```

---

## 🤖 AI-Powered Recommendations

Enable experimental AI suggestions for advanced schema optimization:

```bash
data-quality suggest --tables "users,orders" --use-ai
```

**AI Features** (Experimental):
- Index recommendations based on query patterns
- Advanced normalization suggestions
- Performance optimization hints
- Industry-specific best practices

---

## 📈 Performance & Scale

### Benchmarks
- **Null Scanning**: >10,000 rows/second
- **Orphan Detection**: >5,000 foreign key checks/second  
- **Schema Analysis**: <5 seconds for 100+ table databases
- **Memory Usage**: <100MB for million-row scans

### Production Ready
- Connection pooling and retry logic
- Configurable timeouts and limits
- Comprehensive error handling
- Supports read replicas

---

## 🤝 Contributing

We welcome contributions! This tool follows modern Python best practices:

- **Type Safety**: Full mypy compliance
- **Testing**: 95%+ coverage with pytest
- **Code Quality**: Black, isort, ruff
- **CI/CD**: GitHub Actions with automated releases

```bash
# Quick start for contributors
git clone https://github.com/wmoore012/data_quality.git
cd data_quality
pip install -e ".[dev]"
pre-commit install
pytest
```

---

## 📄 License

GPL-3.0 License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

Built for developers who care about data quality. Inspired by real-world database challenges in music, e-commerce, and analytics platforms.

**Perfect for:**
- Data engineers maintaining production databases
- Developers inheriting legacy schemas  
- Teams implementing data governance
- Anyone who wants clean, well-structured data

---

## 🔗 Links

- **PyPI**: https://pypi.org/project/data-quality/
- **Documentation**: https://github.com/wmoore012/data_quality
- **Issues**: https://github.com/wmoore012/data_quality/issues
- **Changelog**: [CHANGELOG.md](CHANGELOG.md)