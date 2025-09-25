# Data Quality Module - Database Setup

This module uses both shared infrastructure tables and module-specific tables.

## Quick Setup

```bash
# Option 1: Use the module setup script
cd oss/data-quality/sql
python setup_module.py "mysql://user:pass@host:port/icatalog_public"

# Option 2: Run SQL directly
mysql -u user -p icatalog_public < sql/create_tables.sql
```

## Tables Created

### Module-Specific Tables

- **`data_quality_rules`** - Validation rules configuration
- **`data_quality_results`** - Scan results and issue tracking  
- **`data_quality_thresholds`** - Quality metric thresholds

### Shared Infrastructure Tables

- **`oss_module_benchmarks`** - Performance benchmarking (shared)

## Usage Examples

```python
from data_quality import QualityScanner

# Scanner automatically uses database tables for rules
scanner = QualityScanner(connection_string="mysql://...")
report = scanner.scan_table("artists")

# Results are stored in data_quality_results table
```

## CI/CD Integration

Add to your workflow:

```yaml
- name: Validate Data Quality Schema
  run: |
    python oss/data-quality/sql/validate_schema.py "$DATABASE_URL"

- name: Test Database Setup
  run: |
    cd oss/data-quality/sql
    python -m pytest test_setup.py -v
```

## Adding Custom Rules

```sql
INSERT INTO data_quality_rules (
    rule_id, rule_name, table_name, rule_type, severity, sql_check
) VALUES (
    'null_check_artist_name',
    'Artist Name Null Check', 
    'artists',
    'nulls',
    'critical',
    'SELECT COUNT(*) FROM artists WHERE artist_name IS NULL'
);
```

## No Alembic Needed

This module uses simple SQL files instead of Alembic migrations:
- ✅ Easy to understand and debug
- ✅ Works with any database setup
- ✅ No migration version conflicts
- ✅ Perfect for OSS distribution