"""
Tests for quality_scanner module.

These tests verify comprehensive database quality scanning including
null detection, orphan identification, and health reporting.
"""

from sqlalchemy import create_engine, text

from data_quality.quality_scanner import (
    scan_nulls,
    scan_orphans,
    health_check,
    QualityIssue,
    HealthReport,
    _get_tables,
    _get_key_columns,
    _get_row_count,
    _get_null_count,
    _determine_null_severity,
)


class TestQualityIssue:
    """Test QualityIssue dataclass."""

    def test_quality_issue_creation(self):
        """Test creating a QualityIssue."""
        issue = QualityIssue(
            table="users",
            column="email",
            issue_type="nulls",
            count=5,
            total=100,
            percent=5.0,
            severity="warning",
            description="5 null emails found",
        )

        assert issue.table == "users"
        assert issue.column == "email"
        assert issue.issue_type == "nulls"
        assert issue.count == 5
        assert issue.total == 100
        assert issue.percent == 5.0
        assert issue.severity == "warning"
        assert issue.description == "5 null emails found"


class TestHealthReport:
    """Test HealthReport dataclass."""

    def test_health_report_creation(self):
        """Test creating a HealthReport."""
        issues = [
            QualityIssue("users", "id", "nulls", 1, 100, 1.0, "critical", "Critical issue"),
            QualityIssue("orders", "user_id", "orphans", 5, 50, 10.0, "warning", "Warning issue"),
        ]

        report = HealthReport(
            all_good=False,
            total_issues=2,
            issues_by_severity=issues,
            summary={"critical": 1, "warning": 1, "info": 0},
            scan_time_ms=150,
        )

        assert report.all_good is False
        assert report.total_issues == 2
        assert len(report.issues_by_severity) == 2
        assert report.summary["critical"] == 1
        assert report.summary["warning"] == 1
        assert report.scan_time_ms == 150


class TestScanNulls:
    """Test null value scanning functionality."""

    def test_scan_nulls_with_sqlite(self):
        """Test null scanning with SQLite database."""
        # Create in-memory SQLite database
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create test table with null values
            conn.execute(
                text(
                    """
                CREATE TABLE test_users (
                    id INTEGER PRIMARY KEY,
                    email TEXT,
                    user_id INTEGER,
                    isrc TEXT
                )
            """
                )
            )

            # Insert test data with nulls
            conn.execute(
                text(
                    """
                INSERT INTO test_users (id, email, user_id, isrc) VALUES 
                (1, 'user1@test.com', 100, 'USRC123'),
                (2, NULL, 101, 'USRC456'),
                (3, 'user3@test.com', NULL, NULL),
                (4, NULL, 102, 'USRC789')
            """
                )
            )

        # Test scan_nulls function
        issues = scan_nulls(str(engine.url))

        # Should find null issues
        assert len(issues) > 0

        # Check that we found the expected null issues
        null_columns = {issue.column for issue in issues}
        assert "email" in null_columns or "user_id" in null_columns or "isrc" in null_columns

        # Verify issue structure
        for issue in issues:
            assert issue.issue_type == "nulls"
            assert issue.count > 0
            assert issue.total == 4  # Total rows
            assert issue.percent > 0
            assert issue.severity in ["critical", "warning", "info"]
            assert "null" in issue.description.lower()

    def test_scan_nulls_no_issues(self):
        """Test null scanning when no issues exist."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE clean_table (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL
                )
            """
                )
            )

            conn.execute(
                text(
                    """
                INSERT INTO clean_table (id, user_id) VALUES (1, 100), (2, 101)
            """
                )
            )

        issues = scan_nulls(str(engine.url))

        # Should find no null issues in key columns
        assert len(issues) == 0

    def test_scan_nulls_with_table_patterns(self):
        """Test null scanning with table pattern filtering."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create multiple tables
            conn.execute(text("CREATE TABLE users (id INTEGER, email TEXT)"))
            conn.execute(text("CREATE TABLE orders (id INTEGER, user_id INTEGER)"))
            conn.execute(text("INSERT INTO users VALUES (1, NULL)"))
            conn.execute(text("INSERT INTO orders VALUES (1, NULL)"))

        # Scan only users table
        issues = scan_nulls(str(engine.url), table_patterns=["users"])

        # Should only find issues in users table
        for issue in issues:
            assert issue.table == "users"


class TestScanOrphans:
    """Test orphaned record scanning functionality."""

    def test_scan_orphans_basic(self):
        """Test basic orphan scanning."""
        # Note: SQLite doesn't have easy foreign key introspection
        # So this test mainly verifies the function doesn't crash
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY)"))
            conn.execute(text("CREATE TABLE orders (id INTEGER, user_id INTEGER)"))
            conn.execute(text("INSERT INTO users VALUES (1)"))
            conn.execute(text("INSERT INTO orders VALUES (1, 999)"))  # Orphaned

        issues = scan_orphans(str(engine.url))

        # Should not crash and return a list
        assert isinstance(issues, list)

    def test_scan_orphans_no_foreign_keys(self):
        """Test orphan scanning when no foreign keys exist."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE simple_table (id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO simple_table VALUES (1, 'test')"))

        issues = scan_orphans(str(engine.url))

        # Should return empty list when no foreign keys
        assert issues == []


class TestHealthCheck:
    """Test comprehensive health check functionality."""

    def test_health_check_all_good(self):
        """Test health check when database is healthy."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE healthy_table (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    isrc TEXT NOT NULL
                )
            """
                )
            )

            conn.execute(
                text(
                    """
                INSERT INTO healthy_table VALUES (1, 100, 'USRC123'), (2, 101, 'USRC456')
            """
                )
            )

        report = health_check(str(engine.url))

        assert isinstance(report, HealthReport)
        assert report.all_good is True
        assert report.total_issues == 0
        assert report.summary["critical"] == 0
        assert report.summary["warning"] == 0
        assert report.summary["info"] == 0
        assert report.scan_time_ms > 0

    def test_health_check_with_issues(self):
        """Test health check when issues exist."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE problematic_table (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    isrc TEXT
                )
            """
                )
            )

            # Insert data with null values
            conn.execute(
                text(
                    """
                INSERT INTO problematic_table VALUES 
                (1, NULL, 'USRC123'),
                (2, 101, NULL)
            """
                )
            )

        report = health_check(str(engine.url))

        assert isinstance(report, HealthReport)
        assert report.all_good is False
        assert report.total_issues > 0
        assert report.scan_time_ms > 0

        # Should have some issues
        assert len(report.issues_by_severity) > 0

        # Issues should be sorted by severity
        if len(report.issues_by_severity) > 1:
            severity_order = {"critical": 0, "warning": 1, "info": 2}
            for i in range(len(report.issues_by_severity) - 1):
                current_severity = severity_order.get(report.issues_by_severity[i].severity, 3)
                next_severity = severity_order.get(report.issues_by_severity[i + 1].severity, 3)
                assert current_severity <= next_severity


class TestHelperFunctions:
    """Test internal helper functions."""

    def test_get_tables_sqlite(self):
        """Test table listing with SQLite."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER)"))
            conn.execute(text("CREATE TABLE orders (id INTEGER)"))

        tables = _get_tables(engine)

        assert "users" in tables
        assert "orders" in tables

    def test_get_tables_with_patterns(self):
        """Test table listing with patterns."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE user_profiles (id INTEGER)"))
            conn.execute(text("CREATE TABLE user_settings (id INTEGER)"))
            conn.execute(text("CREATE TABLE orders (id INTEGER)"))

        # Test pattern matching (simplified for SQLite)
        tables = _get_tables(engine, patterns=["user"])

        # Should include tables with 'user' in the name
        user_tables = [t for t in tables if "user" in t]
        assert len(user_tables) >= 1

    def test_get_key_columns_sqlite(self):
        """Test key column identification with SQLite."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    email TEXT,
                    isrc TEXT,
                    name TEXT
                )
            """
                )
            )

        key_columns = _get_key_columns(engine, "test_table")

        # Should identify key-like columns
        assert "id" in key_columns
        assert "user_id" in key_columns
        assert "isrc" in key_columns
        # Should not include regular columns
        assert "email" not in key_columns
        assert "name" not in key_columns

    def test_get_row_count(self):
        """Test row counting."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE test_table (id INTEGER)"))
            conn.execute(text("INSERT INTO test_table VALUES (1), (2), (3)"))

        count = _get_row_count(engine, "test_table")
        assert count == 3

    def test_get_null_count(self):
        """Test null value counting."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE test_table (id INTEGER, email TEXT)"))
            conn.execute(
                text("INSERT INTO test_table VALUES (1, 'test@example.com'), (2, NULL), (3, NULL)")
            )

        null_count = _get_null_count(engine, "test_table", "email")
        assert null_count == 2

    def test_determine_null_severity(self):
        """Test null severity determination."""
        # Critical: Primary key columns
        assert _determine_null_severity("id", 1.0) == "critical"
        assert _determine_null_severity("user_id", 1.0) == "critical"
        assert _determine_null_severity("isrc", 1.0) == "critical"

        # Warning: High percentage
        assert _determine_null_severity("email", 75.0) == "warning"

        # Info: Low percentage
        assert _determine_null_severity("email", 5.0) == "info"


class TestErrorHandling:
    """Test error handling in quality scanning."""

    def test_scan_nulls_database_error(self):
        """Test null scanning with database connection error."""
        # Use invalid database URL
        issues = scan_nulls("invalid://database/url")

        # Should return error issue instead of crashing
        assert len(issues) == 1
        assert issues[0].issue_type == "error"
        assert issues[0].severity == "critical"
        assert "failed" in issues[0].description.lower()

    def test_scan_orphans_database_error(self):
        """Test orphan scanning with database connection error."""
        issues = scan_orphans("invalid://database/url")

        # Should return error issue instead of crashing
        assert len(issues) == 1
        assert issues[0].issue_type == "error"
        assert issues[0].severity == "critical"
        assert "failed" in issues[0].description.lower()

    def test_health_check_includes_errors(self):
        """Test that health check includes database errors."""
        report = health_check("invalid://database/url")

        assert isinstance(report, HealthReport)
        assert report.all_good is False
        assert report.total_issues > 0
        assert report.summary["critical"] > 0

        # Should have error issues
        error_issues = [issue for issue in report.issues_by_severity if issue.issue_type == "error"]
        assert len(error_issues) > 0


class TestIntegration:
    """Integration tests with realistic scenarios."""

    def test_music_database_scenario(self):
        """Test with a realistic music database scenario."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create music-related tables
            conn.execute(
                text(
                    """
                CREATE TABLE artists (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    spotify_id TEXT
                )
            """
                )
            )

            conn.execute(
                text(
                    """
                CREATE TABLE songs (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    artist_id INTEGER,
                    isrc TEXT,
                    spotify_id TEXT
                )
            """
                )
            )

            # Insert test data with quality issues
            conn.execute(
                text(
                    """
                INSERT INTO artists VALUES 
                (1, 'Artist 1', 'spotify123'),
                (2, 'Artist 2', NULL),
                (3, 'Artist 3', 'spotify456')
            """
                )
            )

            conn.execute(
                text(
                    """
                INSERT INTO songs VALUES 
                (1, 'Song 1', 1, 'USRC123', 'track123'),
                (2, 'Song 2', 2, NULL, 'track456'),
                (3, 'Song 3', NULL, 'USRC789', NULL),
                (4, 'Song 4', 1, 'USRC123', 'track789')  -- Duplicate ISRC
            """
                )
            )

        # Run comprehensive health check
        report = health_check(str(engine.url))

        # Should find various issues
        assert report.all_good is False
        assert report.total_issues > 0

        # Should find null issues
        null_issues = [issue for issue in report.issues_by_severity if issue.issue_type == "nulls"]
        assert len(null_issues) > 0

        # Should find duplicate issues
        duplicate_issues = [
            issue for issue in report.issues_by_severity if issue.issue_type == "duplicates"
        ]
        assert len(duplicate_issues) > 0

        # Verify issue details
        for issue in report.issues_by_severity:
            assert issue.table in ["artists", "songs"]
            assert issue.count > 0
            assert issue.total > 0
            assert issue.percent >= 0
            assert issue.severity in ["critical", "warning", "info"]
            assert len(issue.description) > 0
