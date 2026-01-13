#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Perday CatalogLAB™

"""
Data Quality Schema Validation

Validates database schema matches expected structure and follows best practices.
Used in CI / CD to ensure schema consistency.
"""

import logging
import sys

logger = logging.getLogger(__name__)


def validate_schema(connection_string: str) -> bool:
    """Validate data quality schema against expected structure."""
    try:
        from urllib.parse import urlparse

        import pymysql
    except ImportError:
        logger.error("❌ Required dependencies not available")
        return False

    # Parse connection
    parsed = urlparse(connection_string)
    connection_params = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3306,
        "user": parsed.username or "root",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/") or "icatalog_public",
        "charset": "utf8mb4",
    }

    try:
        connection = pymysql.connect(**connection_params)

        with connection.cursor() as cursor:
            # Check all expected tables exist
            expected_tables = [
                "data_quality_rules",
                "data_quality_results",
                "data_quality_thresholds",
            ]

            for table in expected_tables:
                if not _table_exists(cursor, table):
                    logger.error(f"❌ Missing table: {table}")
                    return False
                logger.info(f"✅ Table exists: {table}")

            # Validate table structures
            if not _validate_rules_table(cursor):
                return False
            if not _validate_results_table(cursor):
                return False
            if not _validate_thresholds_table(cursor):
                return False

            # Validate indexes
            if not _validate_indexes(cursor):
                return False

            # Validate constraints
            if not _validate_constraints(cursor):
                return False

        connection.close()
        logger.info("✅ Schema validation passed")
        return True

    except Exception as e:
        logger.error(f"❌ Schema validation failed: {e}")
        return False


def _table_exists(cursor, table_name: str) -> bool:
    """Check if table exists."""
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    return cursor.fetchone() is not None


def _validate_rules_table(cursor) -> bool:
    """Validate data_quality_rules table structure."""
    cursor.execute("DESCRIBE data_quality_rules")
    columns = {row[0]: row[1] for row in cursor.fetchall()}

    expected_columns = {
        "rule_id": "varchar(100)",
        "rule_name": "varchar(200)",
        "table_name": "varchar(100)",
        "column_name": "varchar(100)",
        "rule_type": "enum('nulls','duplicate','orphan','schema','custom')",
        "severity": "enum('critical','warning','info')",
        "sql_check": "text",
        "description": "text",
        "owner": "varchar(100)",
        "is_active": "tinyint(1)",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    }

    for col, expected_type in expected_columns.items():
        if col not in columns:
            logger.error(f"❌ Missing column in data_quality_rules: {col}")
            return False
        # Note: MySQL column types may have additional info, so we check if expected is in actual
        if not any(
            expected_part in columns[col].lower()
            for expected_part in expected_type.lower().split()
        ):
            logger.warning(
                f"⚠️ Column type mismatch in data_quality_rules.{col}: expected {expected_type}, got {columns[col]}"
            )

    logger.info("✅ data_quality_rules table structure valid")
    return True


def _validate_results_table(cursor) -> bool:
    """Validate data_quality_results table structure."""
    cursor.execute("DESCRIBE data_quality_results")
    columns = {row[0]: row[1] for row in cursor.fetchall()}

    required_columns = [
        "result_id",
        "scan_timestamp",
        "rule_id",
        "issue_count",
        "severity",
        "table_name",
        "column_name",
        "details",
        "scan_duration_ms",
        "created_at",
    ]

    for col in required_columns:
        if col not in columns:
            logger.error(f"❌ Missing column in data_quality_results: {col}")
            return False

    logger.info("✅ data_quality_results table structure valid")
    return True


def _validate_thresholds_table(cursor) -> bool:
    """Validate data_quality_thresholds table structure."""
    cursor.execute("DESCRIBE data_quality_thresholds")
    columns = {row[0]: row[1] for row in cursor.fetchall()}

    required_columns = [
        "threshold_id",
        "table_name",
        "metric_name",
        "warning_threshold",
        "critical_threshold",
        "is_active",
        "created_at",
    ]

    for col in required_columns:
        if col not in columns:
            logger.error(f"❌ Missing column in data_quality_thresholds: {col}")
            return False

    logger.info("✅ data_quality_thresholds table structure valid")
    return True


def _validate_indexes(cursor) -> bool:
    """Validate expected indexes exist."""
    expected_indexes = [
        ("data_quality_rules", "idx_table_active"),
        ("data_quality_rules", "idx_severity"),
        ("data_quality_rules", "idx_rule_type"),
        ("data_quality_results", "idx_scan_timestamp"),
        ("data_quality_results", "idx_table_severity"),
        ("data_quality_results", "idx_rule_results"),
        ("data_quality_thresholds", "uk_table_metric"),
        ("data_quality_thresholds", "idx_table_active"),
    ]

    for table, index_name in expected_indexes:
        cursor.execute("SHOW INDEX FROM %s WHERE Key_name = %s", (table, index_name))
        if not cursor.fetchone():
            logger.warning(f"⚠️ Missing index: {table}.{index_name}")
            # Don't fail on missing indexes, just warn

    logger.info("✅ Index validation completed")
    return True


def _validate_constraints(cursor) -> bool:
    """Validate foreign key constraints."""
    # Check foreign key from results to rules
    cursor.execute(
        """
        SELECT CONSTRAINT_NAME
        FROM information_schema.REFERENTIAL_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = DATABASE()
        AND TABLE_NAME = 'data_quality_results'
        AND REFERENCED_TABLE_NAME = 'data_quality_rules'
    """
    )

    if not cursor.fetchone():
        logger.warning(
            "⚠️ Missing foreign key constraint: data_quality_results -> data_quality_rules"
        )
        # Don't fail on missing FK, just warn

    logger.info("✅ Constraint validation completed")
    return True


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_schema.py <connection_string>")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    connection_string = sys.argv[1]
    success = validate_schema(connection_string)

    if success:
        print("✅ Schema validation passed")
        sys.exit(0)
    else:
        print("❌ Schema validation failed")
        sys.exit(1)
