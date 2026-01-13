# SPDX-License-Identifier: MIT
# Copyright (c) 2024 Wilton Moore

"""
Secure database operations with read-only enforcement and server-side timeouts.

This module provides secure database connection patterns that enforce
read-only transactions and server-side timeouts as recommended by OWASP.
"""

import re
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from .exceptions import OperationError, ValidationError


class SecureDatabaseError(OperationError):
    """Raised when secure database operations fail."""

    def __init__(
        self, operation: str, reason: str, suggestion: Optional[Optional[str]] = None
    ) -> None:
        super().__init__(operation, reason, retryable=False, suggestion=suggestion)


@contextmanager
def read_only_connection(
    engine: Engine,
    timeout_ms: Optional[Optional[int]] = None,
    vendor: Optional[Optional[str]] = None,
) -> Generator[Connection, None, None]:
    """
    Create a read-only database connection with server-side timeout enforcement.

    Args:
        engine: SQLAlchemy Engine instance
        timeout_ms: Query timeout in milliseconds (server-enforced)
        vendor: Database vendor ("postgres", "mysql", or auto-detect)

    Yields:
        Connection configured for read-only operations

    Raises:
        SecureDatabaseError: If read-only setup fails

    Example:
        >>> with read_only_connection(engine, timeout_ms=30000) as conn:
        ...     result = conn.execute(text("SELECT COUNT(*) FROM users"))
        ...     count = result.scalar()
    """
    if vendor is None:
        vendor = _detect_vendor(engine)

    with engine.begin() as conn:
        try:
            # Set server-side timeout first
            if timeout_ms is not None:
                _set_server_timeout(conn, vendor, timeout_ms)

            # Enforce read-only transaction
            _set_read_only_transaction(conn, vendor)

            yield conn

        except SQLAlchemyError as e:
            raise SecureDatabaseError(
                operation="read_only_connection",
                reason=f"Failed to configure read-only connection: {e}",
                suggestion="Check database permissions and connection settings",
            ) from e


def safe_read_query(
    engine: Engine,
    sql: str,
    params: Optional[Optional[Dict[str, Any]]] = None,
    timeout_ms: Optional[Optional[int]] = None,
    vendor: Optional[Optional[str]] = None,
) -> Any:
    """
    Execute a read-only query with security validation.

    Args:
        engine: SQLAlchemy Engine instance
        sql: SQL query string (must be a literal, no f-strings)
        params: Named parameters for the query
        timeout_ms: Query timeout in milliseconds
        vendor: Database vendor ("postgres", "mysql", or auto-detect)

    Returns:
        Query result

    Raises:
        ValidationError: If SQL contains unsafe patterns
        SecureDatabaseError: If query execution fails

    Example:
        >>> result = safe_read_query(
        ...     engine,
        ...     "SELECT * FROM users WHERE created_at > :date",
        ...     {"date": "2024-01-01"},
        ...     timeout_ms=10000
        ... )
    """
    # Validate SQL safety
    _validate_sql_safety(sql)

    with read_only_connection(engine, timeout_ms, vendor) as conn:
        try:
            return conn.execute(text(sql), params or {})
        except SQLAlchemyError as e:
            raise SecureDatabaseError(
                operation="safe_read_query",
                reason=f"Query execution failed: {e}",
                suggestion="Check SQL syntax and database connectivity",
            ) from e


def create_secure_engine(
    database_url: str, read_only: bool = True, **kwargs: Any
) -> Engine:
    """
    Create a SQLAlchemy engine with secure defaults.

    Args:
        database_url: Database connection URL
        read_only: Whether to use read-only connection settings
        **kwargs: Additional engine options

    Returns:
        Configured SQLAlchemy Engine

    Example:
        >>> engine = create_secure_engine(
        ...     "postgresql://readonly_user:pass@localhost/db",
        ...     read_only=True
        ... )
    """
    # Detect vendor to apply appropriate defaults
    vendor = database_url.split("://")[0].split("+")[0].lower()

    # Base secure defaults
    secure_defaults = {
        "pool_pre_ping": True,  # Validate connections before use
        "pool_recycle": 1800,  # Recycle connections every 30 minutes
        "future": True,  # Use SQLAlchemy 2.0 patterns
    }

    # Add vendor-specific defaults
    if vendor in ("postgresql", "mysql"):
        # These databases support connection pooling with overflow
        secure_defaults.update(
            {
                "pool_size": 10,  # Reasonable connection pool
                "max_overflow": 20,  # Allow burst capacity
            }
        )
    # SQLite uses SingletonThreadPool which doesn't support max_overflow

    # Merge with user options (user options take precedence)
    engine_options = {**secure_defaults, **kwargs}

    # Validate database URL doesn't contain obvious secrets in logs
    safe_url = _redact_database_url(database_url)

    try:
        engine = create_engine(database_url, **engine_options)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return engine

    except Exception as e:
        raise SecureDatabaseError(
            operation="create_secure_engine",
            reason=f"Failed to create engine for {safe_url}: {e}",
            suggestion="Check database URL and connectivity",
        ) from e


def _detect_vendor(engine: Engine) -> str:
    """Detect database vendor from engine."""
    dialect_name = engine.dialect.name.lower()
    if "postgres" in dialect_name:
        return "postgres"
    elif "mysql" in dialect_name:
        return "mysql"
    elif "sqlite" in dialect_name:
        return "sqlite"
    else:
        return "unknown"


def _set_server_timeout(conn: Connection, vendor: str, timeout_ms: int) -> None:
    """Set server-side query timeout."""
    if vendor == "postgres":
        # PostgreSQL uses statement_timeout in milliseconds
        conn.execute(
            text("SET LOCAL statement_timeout = :timeout"), {"timeout": timeout_ms}
        )
    elif vendor == "mysql":
        # MySQL uses max_execution_time in milliseconds (MySQL 5.7+)
        conn.execute(
            text("SET SESSION max_execution_time = :timeout"), {"timeout": timeout_ms}
        )
    # SQLite doesn't support server-side timeouts


def _set_read_only_transaction(conn: Connection, vendor: str) -> None:
    """Set transaction to read-only mode."""
    if vendor in ("postgres", "mysql"):
        conn.execute(text("SET TRANSACTION READ ONLY"))
    # SQLite doesn't support read-only transactions at SQL level


def _validate_sql_safety(sql: str) -> None:
    """Validate that SQL string is safe (basic checks)."""
    if not isinstance(sql, str):
        raise ValidationError(
            field="sql",
            value=type(sql).__name__,
            expected="string",
            suggestion="Provide SQL as a string literal",
        )

    # Check for multiple statements (basic check)
    # Remove trailing semicolon and whitespace, then check for remaining semicolons
    cleaned_sql = sql.strip().rstrip(";").strip()
    if ";" in cleaned_sql:
        raise ValidationError(
            field="sql",
            value=sql,
            expected="single SQL statement",
            suggestion="Use separate calls for multiple statements",
        )

    # Check for obviously dangerous patterns (defense in depth)
    dangerous_patterns = [
        r"\bDROP\s+TABLE\b",
        r"\bDELETE\s+FROM\b",
        r"\bTRUNCATE\b",
        r"\bALTER\s+TABLE\b",
        r"\bCREATE\s+TABLE\b",
        r"\bINSERT\s+INTO\b",
        r"\bUPDATE\s+\w+\s+SET\b",
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, sql, re.IGNORECASE):
            raise ValidationError(
                field="sql",
                value=sql,
                expected="read-only SELECT statement",
                suggestion="Use read-only queries only. Write operations should use dedicated functions.",
            )


def _redact_database_url(url: str) -> str:
    """Redact sensitive information from database URL for logging."""
    # Replace password with ***
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)


# Example usage and validation
if __name__ == "__main__":
    print("🔐 Secure Database Operations Examples")
    print("=" * 40)

    # Example of safe usage
    print("✅ Safe pattern:")
    print('safe_read_query(engine, "SELECT * FROM users WHERE id = :id", {"id": 123})')

    print("\n❌ Unsafe patterns:")
    unsafe_examples = [
        'conn.execute(f"SELECT * FROM users WHERE id = {user_id}")',
        'conn.execute("DELETE FROM users WHERE id = 1")',
        'conn.execute("SELECT * FROM users; DROP TABLE users;")',
    ]

    for example in unsafe_examples:
        print(f"  {example}")

    print("\n🛡️ Security Features:")
    print(
        "  • Server-side timeouts (PostgreSQL statement_timeout, MySQL max_execution_time)"
    )
    print("  • Read-only transaction enforcement")
    print("  • Single-statement validation")
    print("  • Dangerous pattern detection")
    print("  • Connection pool security defaults")
