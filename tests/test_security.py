# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Security tests for data quality module.

Tests SQL injection prevention, read-only enforcement, and other security features.
"""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from data_quality.exceptions import ValidationError
from data_quality.secure_db import (
    SecureDatabaseError,
    create_secure_engine,
    read_only_connection,
    safe_read_query,
)
from data_quality.sql_policy import SQLSecurityViolation, check_sql_security


class TestSQLPolicyChecker:
    """Test AST-based SQL security policy checking."""
    
    def test_detects_f_string_injection(self):
        """Test detection of f-string SQL injection."""
        code = 'conn.execute(f"SELECT * FROM users WHERE id = {user_id}")'
        violations = check_sql_security(code)
        
        assert len(violations) == 1
        assert violations[0].code == "SQL001"
        assert "Unsafe SQL construction" in violations[0].message
    
    def test_detects_percent_formatting(self):
        """Test detection of % string formatting."""
        code = 'conn.execute("SELECT * FROM users WHERE id = %s" % user_id)'
        violations = check_sql_security(code)
        
        assert len(violations) == 1
        assert violations[0].code == "SQL001"
    
    def test_detects_format_method(self):
        """Test detection of .format() method."""
        code = 'conn.execute("SELECT * FROM users WHERE id = {}".format(user_id))'
        violations = check_sql_security(code)
        
        assert len(violations) == 1
        assert violations[0].code == "SQL001"
    
    def test_detects_string_concatenation(self):
        """Test detection of string concatenation with variables."""
        code = 'conn.execute("SELECT * FROM users WHERE id = " + str(user_id))'
        violations = check_sql_security(code)
        
        assert len(violations) == 1
        assert violations[0].code == "SQL001"
    
    def test_detects_text_with_variable(self):
        """Test detection of text() with variable argument."""
        code = 'conn.execute(text(query_variable))'
        violations = check_sql_security(code)
        
        assert len(violations) == 1
        assert violations[0].code == "SQL001"
    
    def test_allows_safe_patterns(self):
        """Test that safe SQL patterns are allowed."""
        safe_patterns = [
            'conn.execute(text("SELECT * FROM users WHERE id = :id"), {"id": user_id})',
            'conn.execute("SELECT * FROM users")',
            'conn.execute(text("SELECT COUNT(*) FROM users"))',
        ]
        
        for pattern in safe_patterns:
            violations = check_sql_security(pattern)
            assert len(violations) == 0, f"False positive for: {pattern}"
    
    def test_handles_syntax_errors(self):
        """Test handling of syntax errors in code."""
        code = 'conn.execute(f"SELECT * FROM users WHERE id = {user_id"'  # Missing closing brace
        violations = check_sql_security(code)
        
        assert len(violations) == 1
        assert violations[0].code == "SYNTAX"
    
    def test_multiple_violations(self):
        """Test detection of multiple violations in one code block."""
        code = '''
conn.execute(f"SELECT * FROM users WHERE id = {user_id}")
conn.execute("SELECT * FROM posts WHERE author = %s" % author_id)
        '''
        violations = check_sql_security(code)
        
        assert len(violations) == 2
        assert all(v.code == "SQL001" for v in violations)


class TestSecureDatabase:
    """Test secure database operations."""
    
    @pytest.fixture
    def sqlite_engine(self):
        """Create in-memory SQLite engine for testing."""
        engine = create_engine("sqlite:///:memory:")
        
        # Create test table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE test_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE
                )
            """))
            conn.execute(text("""
                INSERT INTO test_users (name, email) VALUES 
                ('Alice', 'alice@example.com'),
                ('Bob', 'bob@example.com')
            """))
        
        return engine
    
    def test_safe_read_query_success(self, sqlite_engine):
        """Test successful safe read query."""
        result = safe_read_query(
            sqlite_engine,
            "SELECT COUNT(*) as count FROM test_users WHERE name = :name",
            {"name": "Alice"}
        )
        
        row = result.fetchone()
        assert row.count == 1
    
    def test_safe_read_query_rejects_multiple_statements(self, sqlite_engine):
        """Test that multiple statements are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            safe_read_query(
                sqlite_engine,
                "SELECT * FROM test_users; DROP TABLE test_users;",
                {}
            )
        
        assert "single SQL statement" in str(exc_info.value)
    
    def test_safe_read_query_rejects_write_operations(self, sqlite_engine):
        """Test that write operations are rejected."""
        dangerous_queries = [
            "DELETE FROM test_users WHERE id = 1",
            "UPDATE test_users SET name = 'Hacker' WHERE id = 1",
            "INSERT INTO test_users (name, email) VALUES ('Evil', 'evil@example.com')",
            "DROP TABLE test_users",
            "TRUNCATE TABLE test_users",
        ]
        
        for query in dangerous_queries:
            with pytest.raises(ValidationError) as exc_info:
                safe_read_query(sqlite_engine, query, {})
            
            assert "read-only SELECT statement" in str(exc_info.value)
    
    def test_read_only_connection_context(self, sqlite_engine):
        """Test read-only connection context manager."""
        with read_only_connection(sqlite_engine) as conn:
            result = conn.execute(text("SELECT COUNT(*) as count FROM test_users"))
            row = result.fetchone()
            assert row.count == 2
    
    def test_create_secure_engine_with_defaults(self):
        """Test secure engine creation with defaults."""
        engine = create_secure_engine("sqlite:///:memory:")
        
        # Verify secure defaults (SQLite-specific)
        assert engine.pool._pre_ping is True
        assert engine.pool._recycle == 1800
        # SQLite uses SingletonThreadPool which doesn't have size/overflow
        
        # Test that engine works
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
    
    def test_database_url_redaction(self):
        """Test that database URLs are properly redacted in error messages."""
        try:
            create_secure_engine("postgresql://user:secret123@localhost/db")
        except SecureDatabaseError as e:
            # Should not contain the actual password
            assert "secret123" not in str(e)
            assert "***" in str(e)


class TestSecurityIntegration:
    """Integration tests for security features."""
    
    def test_end_to_end_secure_query(self):
        """Test complete secure query workflow."""
        engine = create_secure_engine("sqlite:///:memory:")
        
        # Create test data
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.execute(text("INSERT INTO users (name) VALUES ('Test User')"))
        
        # Execute secure query
        result = safe_read_query(
            engine,
            "SELECT name FROM users WHERE id = :user_id",
            {"user_id": 1}
        )
        
        row = result.fetchone()
        assert row.name == "Test User"
    
    def test_policy_checker_integration(self):
        """Test that policy checker integrates with actual code patterns."""
        # This would be used in CI/CD to check actual source files
        sample_code = '''
def get_user_by_id(conn, user_id):
    # Safe pattern
    return conn.execute(
        text("SELECT * FROM users WHERE id = :id"), 
        {"id": user_id}
    ).fetchone()

def unsafe_get_user(conn, user_id):
    # Unsafe pattern - should be caught
    return conn.execute(f"SELECT * FROM users WHERE id = {user_id}").fetchone()
        '''
        
        violations = check_sql_security(sample_code)
        
        # Should find exactly one violation (the unsafe pattern)
        assert len(violations) == 1
        assert violations[0].line > 5  # Should be in the unsafe function
        assert "Unsafe SQL construction" in violations[0].message


class TestSecurityDocumentation:
    """Test that security features are properly documented."""
    
    def test_security_examples_are_valid(self):
        """Test that documented security examples actually work."""
        # Safe patterns from documentation
        safe_examples = [
            'conn.execute(text("SELECT * FROM users WHERE id = :id"), {"id": user_id})',
            'conn.execute("SELECT COUNT(*) FROM users")',
        ]
        
        for example in safe_examples:
            violations = check_sql_security(example)
            assert len(violations) == 0, f"Documented safe example failed: {example}"
        
        # Unsafe patterns from documentation
        unsafe_examples = [
            'conn.execute(f"SELECT * FROM users WHERE id = {user_id}")',
            'conn.execute("SELECT * FROM users WHERE id = %s" % user_id)',
        ]
        
        for example in unsafe_examples:
            violations = check_sql_security(example)
            assert len(violations) > 0, f"Documented unsafe example not caught: {example}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])