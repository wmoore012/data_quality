# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Comprehensive database quality scanning with severity-based reporting.

This module provides database health checks including null detection,
orphan record identification, and comprehensive quality reporting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


@dataclass
class QualityIssue:
    """Represents a data quality issue found during scanning."""

    table: str
    column: str
    issue_type: str  # "nulls", "orphans", "duplicates"
    count: int
    total: int
    percent: float
    severity: str  # "critical", "warning", "info"
    description: str


@dataclass
class HealthReport:
    """Comprehensive database health report."""

    all_good: bool
    total_issues: int
    issues_by_severity: list[QualityIssue]
    summary: dict[str, int]  # {"critical": 2, "warning": 5, "info": 1}
    scan_time_ms: int


def scan_nulls(
    database_url: str, table_patterns: Optional[Optional[list[str]]] = None
) -> list[QualityIssue]:
    """
    Scan database for null values in key columns with detailed reporting.

    Args:
        database_url: Database connection URL
        table_patterns: Optional list of table name patterns to scan

    Returns:
        List of QualityIssue objects for null value problems

    Example:
        >>> issues = scan_nulls("mysql://user:pass@host/db")
        >>> for issue in issues:
        ...     print(f"{issue.severity}: {issue.description}")
    """
    engine = create_engine(database_url)
    issues = []

    try:
        # Test connection first
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))

        # Get all tables or filter by patterns
        tables = _get_tables(engine, table_patterns)

        for table in tables:
            columns = _get_key_columns(engine, table)
            total_rows = _get_row_count(engine, table)

            if total_rows == 0:
                continue

            for column in columns:
                null_count = _get_null_count(engine, table, column)

                if null_count > 0:
                    percent = (null_count / total_rows) * 100
                    severity = _determine_null_severity(column, percent)

                    issue = QualityIssue(
                        table=table,
                        column=column,
                        issue_type="nulls",
                        count=null_count,
                        total=total_rows,
                        percent=percent,
                        severity=severity,
                        description=f"Table '{table}' has {null_count} null values in '{column}' ({percent:.1f}%)",
                    )
                    issues.append(issue)

    except (SQLAlchemyError, OSError) as e:
        # Create an error issue
        issue = QualityIssue(
            table="",
            column="",
            issue_type="error",
            count=0,
            total=0,
            percent=0.0,
            severity="critical",
            description=f"Database scan failed: {str(e)}",
        )
        issues.append(issue)
    except Exception as e:
        # Catch any other exceptions
        issue = QualityIssue(
            table="",
            column="",
            issue_type="error",
            count=0,
            total=0,
            percent=0.0,
            severity="critical",
            description=f"Unexpected error during scan: {str(e)}",
        )
        issues.append(issue)

    return issues


def scan_orphans(
    database_url: str, table_patterns: Optional[Optional[list[str]]] = None
) -> list[QualityIssue]:
    """
    Scan database for orphaned records (foreign keys pointing to missing records).

    Args:
        database_url: Database connection URL
        table_patterns: Optional list of table name patterns to scan

    Returns:
        List of QualityIssue objects for orphaned record problems

    Example:
        >>> orphans = scan_orphans("mysql://user:pass@host/db")
        >>> for orphan in orphans:
        ...     print(f"Found {orphan.count} orphaned records in {orphan.table}.{orphan.column}")
    """
    engine = create_engine(database_url)
    issues = []

    try:
        # Test connection first
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))

        # Get all tables or filter by patterns
        tables = _get_tables(engine, table_patterns)

        for table in tables:
            foreign_keys = _get_foreign_keys(engine, table)

            for fk_column, ref_table, ref_column in foreign_keys:
                orphan_count = _get_orphan_count(
                    engine, table, fk_column, ref_table, ref_column
                )

                if orphan_count > 0:
                    total_rows = _get_row_count(engine, table)
                    percent = (orphan_count / total_rows) * 100 if total_rows > 0 else 0

                    issue = QualityIssue(
                        table=table,
                        column=fk_column,
                        issue_type="orphans",
                        count=orphan_count,
                        total=total_rows,
                        percent=percent,
                        severity="critical",  # Orphans are always critical
                        description=f"Table '{table}' has {orphan_count} orphaned records in '{fk_column}' referencing '{ref_table}.{ref_column}'",
                    )
                    issues.append(issue)

    except (SQLAlchemyError, OSError) as e:
        issue = QualityIssue(
            table="",
            column="",
            issue_type="error",
            count=0,
            total=0,
            percent=0.0,
            severity="critical",
            description=f"Orphan scan failed: {str(e)}",
        )
        issues.append(issue)
    except Exception as e:
        issue = QualityIssue(
            table="",
            column="",
            issue_type="error",
            count=0,
            total=0,
            percent=0.0,
            severity="critical",
            description=f"Unexpected error during orphan scan: {str(e)}",
        )
        issues.append(issue)

    return issues


def health_check(
    database_url: str, table_patterns: Optional[Optional[list[str]]] = None
) -> HealthReport:
    """
    Perform comprehensive database health check covering common issues.

    Args:
        database_url: Database connection URL
        table_patterns: Optional list of table name patterns to scan

    Returns:
        HealthReport with all issues found, prioritized by severity

    Example:
        >>> report = health_check("mysql://user:pass@host/db")
        >>> if report.all_good:
        ...     print("All good!")
        >>> else:
        ...     for issue in report.issues_by_severity:
        ...         print(f"{issue.severity}: {issue.description}")
    """
    import time

    start_time = time.perf_counter()

    # Collect all issues
    all_issues = []

    # Scan for nulls
    null_issues = scan_nulls(database_url, table_patterns)
    all_issues.extend(null_issues)

    # Scan for orphans
    orphan_issues = scan_orphans(database_url, table_patterns)
    all_issues.extend(orphan_issues)

    # Scan for duplicates (basic implementation)
    duplicate_issues = _scan_duplicates(database_url, table_patterns)
    all_issues.extend(duplicate_issues)

    # Sort by severity (critical first)
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    all_issues.sort(
        key=lambda x: (severity_order.get(x.severity, 3), x.table, x.column)
    )

    # Create summary
    summary = {"critical": 0, "warning": 0, "info": 0}
    for issue in all_issues:
        summary[issue.severity] = summary.get(issue.severity, 0) + 1

    scan_time_ms = int((time.perf_counter() - start_time) * 1000)

    return HealthReport(
        all_good=(len(all_issues) == 0),
        total_issues=len(all_issues),
        issues_by_severity=all_issues,
        summary=summary,
        scan_time_ms=scan_time_ms,
    )


def _get_tables(
    engine: Engine, patterns: Optional[Optional[list[str]]] = None
) -> list[str]:
    """Get list of tables, optionally filtered by patterns."""
    try:
        # Try MySQL/PostgreSQL approach
        if patterns:
            pattern_conditions = " OR ".join(
                [f"table_name LIKE :p{i}" for i in range(len(patterns))]
            )
            query = text(
                f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND ({pattern_conditions})
                ORDER BY table_name
            """
            )
            params = {f"p{i}": pattern for i, pattern in enumerate(patterns)}
        else:
            query = text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                ORDER BY table_name
            """
            )
            params = {}

        with engine.begin() as conn:
            result = conn.execute(query, params)
            return [row[0] for row in result]

    except SQLAlchemyError:
        # Fallback for SQLite
        try:
            query = text(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            with engine.begin() as conn:
                result = conn.execute(query)
                tables = [row[0] for row in result]

                if patterns:
                    # Simple pattern matching for SQLite
                    filtered_tables = []
                    for table in tables:
                        for pattern in patterns:
                            if pattern.replace("%", "") in table:
                                filtered_tables.append(table)
                                break
                    return filtered_tables
                return tables
        except SQLAlchemyError:
            return []


def _get_key_columns(engine: Engine, table: str) -> list[str]:
    """Get columns that are likely to be important (IDs, keys, emails, etc.)."""
    try:
        # Try MySQL/PostgreSQL approach first
        query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
            AND table_name = :table
            AND (column_name LIKE '%id%'
                 OR column_name LIKE '%key%'
                 OR column_name = 'isrc'
                 OR column_name LIKE '%email%'
                 OR column_name LIKE '%_code'
                 OR column_name LIKE '%_number')
            ORDER BY ordinal_position
        """
        )

        with engine.begin() as conn:
            result = conn.execute(query, {"table": table})
            return [row[0] for row in result]

    except SQLAlchemyError:
        # Fallback for SQLite
        try:
            query = text(f"PRAGMA table_info({table})")
            with engine.begin() as conn:
                result = conn.execute(query)
                columns = []
                for row in result:
                    column_name = row[1]  # Column name is at index 1
                    if (
                        "id" in column_name.lower()
                        or "key" in column_name.lower()
                        or column_name.lower() == "isrc"
                        or "email" in column_name.lower()
                        or column_name.lower().endswith("_code")
                        or column_name.lower().endswith("_number")
                    ):
                        columns.append(column_name)
                return columns
        except SQLAlchemyError:
            # Last resort - try to get all columns and filter
            try:
                query = text(f"SELECT * FROM {table} LIMIT 0")
                with engine.begin() as conn:
                    result = conn.execute(query)
                    all_columns = list(result.keys())
                    key_columns = []
                    for col in all_columns:
                        if (
                            "id" in col.lower()
                            or "key" in col.lower()
                            or col.lower() == "isrc"
                            or "email" in col.lower()
                            or col.lower().endswith("_code")
                            or col.lower().endswith("_number")
                        ):
                            key_columns.append(col)
                    return key_columns
            except SQLAlchemyError:
                return []


def _get_row_count(engine: Engine, table: str) -> int:
    """Get total row count for a table."""
    try:
        query = text(f"SELECT COUNT(*) FROM {table}")
        with engine.begin() as conn:
            result = conn.execute(query)
            return result.scalar() or 0
    except SQLAlchemyError:
        return 0


def _get_null_count(engine: Engine, table: str, column: str) -> int:
    """Get count of null values in a specific column."""
    try:
        query = text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
        with engine.begin() as conn:
            result = conn.execute(query)
            return result.scalar() or 0
    except SQLAlchemyError:
        return 0


def _get_foreign_keys(engine: Engine, table: str) -> list[tuple[str, str, str]]:
    """Get foreign key relationships for a table."""
    try:
        # Try MySQL approach
        query = text(
            """
            SELECT
                column_name,
                referenced_table_name,
                referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = DATABASE()
            AND table_name = :table
            AND referenced_table_name IS NOT NULL
        """
        )

        with engine.begin() as conn:
            result = conn.execute(query, {"table": table})
            return [(row[0], row[1], row[2]) for row in result]

    except SQLAlchemyError:
        # SQLite doesn't have easy foreign key introspection
        # Return empty list for now
        return []


def _get_orphan_count(
    engine: Engine, table: str, fk_column: str, ref_table: str, ref_column: str
) -> int:
    """Get count of orphaned records (foreign key points to non-existent record)."""
    try:
        query = text(
            f"""
            SELECT COUNT(*)
            FROM {table} t
            LEFT JOIN {ref_table} r ON t.{fk_column} = r.{ref_column}
            WHERE t.{fk_column} IS NOT NULL
            AND r.{ref_column} IS NULL
        """
        )

        with engine.begin() as conn:
            result = conn.execute(query)
            return result.scalar() or 0
    except SQLAlchemyError:
        return 0


def _scan_duplicates(
    database_url: str, table_patterns: Optional[Optional[list[str]]] = None
) -> list[QualityIssue]:
    """Scan for duplicate records in key columns."""
    engine = create_engine(database_url)
    issues = []

    try:
        tables = _get_tables(engine, table_patterns)

        for table in tables:
            # Look for duplicates in columns that should be unique
            unique_columns = _get_unique_candidate_columns(engine, table)

            for column in unique_columns:
                duplicate_count = _get_duplicate_count(engine, table, column)

                if duplicate_count > 0:
                    total_rows = _get_row_count(engine, table)
                    percent = (
                        (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
                    )

                    issue = QualityIssue(
                        table=table,
                        column=column,
                        issue_type="duplicates",
                        count=duplicate_count,
                        total=total_rows,
                        percent=percent,
                        severity="warning",  # Duplicates are usually warnings
                        description=f"Table '{table}' has {duplicate_count} duplicate values in '{column}'",
                    )
                    issues.append(issue)

    except SQLAlchemyError:
        pass  # Ignore errors in duplicate scanning

    return issues


def _get_unique_candidate_columns(engine: Engine, table: str) -> list[str]:
    """Get columns that should probably be unique (like ISRCs, IDs, etc.)."""
    try:
        # Try MySQL/PostgreSQL approach
        query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
            AND table_name = :table
            AND (column_name = 'isrc'
                 OR column_name LIKE '%_code'
                 OR column_name LIKE '%_number'
                 OR column_name LIKE '%_id')
            ORDER BY ordinal_position
        """
        )

        with engine.begin() as conn:
            result = conn.execute(query, {"table": table})
            return [row[0] for row in result]

    except SQLAlchemyError:
        # Fallback for SQLite
        try:
            query = text(f"PRAGMA table_info({table})")
            with engine.begin() as conn:
                result = conn.execute(query)
                columns = []
                for row in result:
                    column_name = row[1]
                    if (
                        column_name.lower() == "isrc"
                        or column_name.lower().endswith("_code")
                        or column_name.lower().endswith("_number")
                        or column_name.lower().endswith("_id")
                    ):
                        columns.append(column_name)
                return columns
        except SQLAlchemyError:
            return []


def _get_duplicate_count(engine: Engine, table: str, column: str) -> int:
    """Get count of duplicate values in a column."""
    try:
        query = text(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT {column})
            FROM {table}
            WHERE {column} IS NOT NULL
        """
        )

        with engine.begin() as conn:
            result = conn.execute(query)
            return max(0, result.scalar() or 0)
    except SQLAlchemyError:
        return 0


def _determine_null_severity(column: str, percent: float) -> str:
    """Determine severity of null values based on column type and percentage."""
    column_lower = column.lower()

    # Critical: Primary keys and ISRCs should never be null
    if ("id" in column_lower and column_lower.endswith("id")) or column_lower == "isrc":
        return "critical"

    # Warning: High percentage of nulls in important columns
    if percent > 50:
        return "warning"

    # Info: Low percentage of nulls
    return "info"
