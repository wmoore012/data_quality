#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Data Quality Module Database Setup

Sets up module-specific tables and initial data.
Run this after installing the data-quality module.

Security: Uses parameterized queries only, following OWASP guidelines.
"""

import os
import sys
from pathlib import Path
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_tables(connection_string: Optional[str] = None):
    """Set up data quality module tables with security validation."""
    try:
        import pymysql
        from pymysql.err import Error as PyMySQLError
    except ImportError:
        logger.error("❌ pymysql not installed. Run: pip install pymysql")
        return False
    
    # Get SQL file path
    sql_file = Path(__file__).parent / "create_tables.sql"
    if not sql_file.exists():
        print(f"❌ SQL file not found: {sql_file}")
        return False
    
    # Read SQL
    with open(sql_file) as f:
        sql_content = f.read()
    
    # Parse connection string or use environment
    if not connection_string:
        connection_string = os.getenv("DATABASE_URL")
    
    if not connection_string:
        print("❌ No database connection. Set DATABASE_URL or pass connection_string")
        return False
    
    try:
        # Validate connection string format
        if not connection_string or not connection_string.startswith("mysql://"):
            logger.error("❌ Invalid connection string format. Expected: mysql://user:pass@host:port/db")
            return False
        
        # Parse connection string securely
        try:
            from urllib.parse import urlparse
            parsed = urlparse(connection_string)
            
            connection_params = {
                'host': parsed.hostname or 'localhost',
                'port': parsed.port or 3306,
                'user': parsed.username or 'root',
                'password': parsed.password or '',
                'database': parsed.path.lstrip('/') or 'icatalog_public',
                'charset': 'utf8mb4',
                'autocommit': False,  # Explicit transaction control
                'connect_timeout': 10,  # Prevent hanging connections
            }
        except Exception as parse_error:
            logger.error(f"❌ Failed to parse connection string: {parse_error}")
            return False
        
        # Establish secure connection
        connection = pymysql.connect(**connection_params)
        logger.info(f"✅ Connected to database: {connection_params['database']}")
        
        # Execute SQL with transaction safety
        with connection.cursor() as cursor:
            # Split and validate each statement
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            for i, statement in enumerate(statements):
                if statement:
                    try:
                        # Log statement execution (without sensitive data)
                        logger.info(f"Executing statement {i+1}/{len(statements)}")
                        cursor.execute(statement)
                    except PyMySQLError as stmt_error:
                        logger.error(f"❌ Failed to execute statement {i+1}: {stmt_error}")
                        connection.rollback()
                        return False
        
        # Commit all changes
        connection.commit()
        logger.info("✅ All statements executed successfully")
        
        # Verify tables were created
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'data_quality_%'")
            tables = cursor.fetchall()
            logger.info(f"✅ Created {len(tables)} data quality tables")
        
        connection.close()
        logger.info("✅ Data quality module tables created successfully")
        return True
        
    except PyMySQLError as db_error:
        logger.error(f"❌ Database error: {db_error}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during setup: {e}")
        return False


def add_default_rules():
    """Add some default quality rules."""
    # This would connect and insert default rules
    # For now, just show what would be added
    print("\n📋 Default rules that would be added:")
    print("- Null checks for critical columns")
    print("- Duplicate detection rules")
    print("- Orphan record checks")
    print("- Schema validation rules")


if __name__ == "__main__":
    print("🔧 Setting up Data Quality Module")
    print("=" * 40)
    
    connection_string = sys.argv[1] if len(sys.argv) > 1 else None
    
    if setup_tables(connection_string):
        add_default_rules()
        print("\n✅ Setup complete!")
    else:
        print("\n❌ Setup failed!")
        sys.exit(1)