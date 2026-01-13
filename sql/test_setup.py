#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Perday CatalogLAB™

"""
Test Data Quality Module Database Setup

Validates that database setup works correctly and follows security practices.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from setup_module import setup_tables


class TestDatabaseSetup:
    """Test database setup functionality."""

    def test_setup_with_invalid_connection_string(self):
        """Test that invalid connection strings are rejected."""
        # Test None
        assert setup_tables(None) is False

        # Test invalid format
        assert setup_tables("invalid://connection") is False
        assert setup_tables("postgresql://user:pass@host / db") is False

    def test_setup_with_missing_pymysql(self):
        """Test graceful handling when pymysql is not available."""
        with patch.dict("sys.modules", {"pymysql": None}):
            result = setup_tables("mysql://user:pass@host / db")
            assert result is False

    def test_connection_string_parsing(self):
        """Test secure connection string parsing."""
        from urllib.parse import urlparse

        test_cases = [
            (
                "mysql://root:pass@localhost:3306 / testdb",
                {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "password": "pass",
                    "database": "testdb",
                },
            ),
            (
                "mysql://user@host / db",
                {
                    "host": "host",
                    "port": 3306,
                    "user": "user",
                    "password": "",
                    "database": "db",
                },
            ),
        ]

        for conn_str, expected in test_cases:
            parsed = urlparse(conn_str)
            assert parsed.hostname == expected["host"]
            assert (parsed.port or 3306) == expected["port"]
            assert parsed.username == expected["user"]
            assert (parsed.password or "") == expected["password"]
            assert parsed.path.lstrip("/") == expected["database"]

    @patch("pymysql.connect")
    def test_successful_setup(self, mock_connect):
        """Test successful database setup."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Mock cursor.fetchall() for table verification
        mock_cursor.fetchall.return_value = [
            ("data_quality_rules",),
            ("data_quality_results",),
            ("data_quality_thresholds",),
        ]

        result = setup_tables("mysql://root:pass@localhost / testdb")

        assert result is True
        mock_connect.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("pymysql.connect")
    def test_database_error_handling(self, mock_connect):
        """Test handling of database errors."""
        import pymysql

        # Simulate connection error
        mock_connect.side_effect = pymysql.Error("Connection failed")

        result = setup_tables("mysql://root:pass@localhost / testdb")
        assert result is False

    @patch("pymysql.connect")
    def test_sql_execution_error_handling(self, mock_connect):
        """Test handling of SQL execution errors."""
        import pymysql

        # Mock connection but simulate SQL error
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = pymysql.Error("SQL error")
        mock_connect.return_value = mock_conn

        result = setup_tables("mysql://root:pass@localhost / testdb")

        assert result is False
        mock_conn.rollback.assert_called_once()

    def test_sql_file_validation(self):
        """Test that SQL file exists and contains expected content."""
        sql_file = Path(__file__).parent / "create_tables.sql"
        assert sql_file.exists(), "create_tables.sql file must exist"

        content = sql_file.read_text()

        # Verify expected tables are defined
        expected_tables = [
            "data_quality_rules",
            "data_quality_results",
            "data_quality_thresholds",
        ]

        for table in expected_tables:
            assert (
                f"CREATE TABLE IF NOT EXISTS {table}" in content
            ), f"Missing table: {table}"

        # Verify security practices
        assert "CREATE TABLE IF NOT EXISTS" in content, "Should use IF NOT EXISTS"
        assert "ENGINE=InnoDB" in content, "Should use InnoDB engine"
        assert (
            "CHARSET=utf8mb4" in content or "utf8mb4" in content
        ), "Should use utf8mb4"

    def test_no_hardcoded_credentials(self):
        """Test that no credentials are hardcoded in setup files."""
        setup_file = Path(__file__).parent / "setup_module.py"
        setup_file.read_text()

        # Check for common credential patterns

        # Filter out safe usage (like 'password': parsed.password)
        unsafe_lines = [
            line
            for line in lines_with_pattern
            if "=" in line and not ("parsed." in line or "connection_params" in line)
        ]

        assert len(unsafe_lines) == 0, f"Potential hardcoded credential: {unsafe_lines}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
