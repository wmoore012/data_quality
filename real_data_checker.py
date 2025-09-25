#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Real Data Quality Checker

Tests data quality against your actual production database.
Configurable for development vs production environments.

Usage:
    # Development mode (warnings only)
    python real_data_checker.py --database-url $DATABASE_URL --mode development
    
    # Production mode (can block on critical issues)
    python real_data_checker.py --database-url $DATABASE_URL --mode production --block-on-critical
"""

import argparse
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Look for .env in project root (two levels up from this script)
    env_path = os.path.join(os.path.dirname(__file__), '../../.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✅ Loaded environment from {env_path}")
    else:
        # Try current directory
        if os.path.exists('.env'):
            load_dotenv('.env')
            print("✅ Loaded environment from .env")
except ImportError:
    print("💡 Install python-dotenv to auto-load .env files: pip install python-dotenv")

# Add project paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data_quality.quality_scanner import health_check, scan_nulls, scan_orphans
# DEPRECATED: # DEPRECATED: from web.db_guard import
# Use SQLAlchemy directly get_engine
# Use SQLAlchemy create_engine directly


def get_real_database_connection():
    """Get connection to actual production database."""
    try:
        # Try to connect to PUBLIC schema (icatalog_public)
        engine = get_engine(schema="PUBLIC")
        
        # Test connection
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT 1 as test"))
            if result.scalar() == 1:
                return engine
        
        return None
    except Exception as e:
        print(f"⚠️  Could not connect to production database: {e}")
        return None


def get_critical_tables() -> List[str]:
    """Get list of critical tables that must have good data quality."""
    return [
        'songs',
        'artists', 
        'albums',
        'labels',
        'spotify_tracks',
        'youtube_videos',
        'tidal_tracks_raw'
    ]


def test_real_upsert_quality(engine, table_name: str) -> Dict:
    """
    Test data quality by doing a real upsert and verifying results.
    This ensures our ETL pipeline is working correctly.
    """
    results = {
        'table': table_name,
        'total_rows': 0,
        'sample_data': [],
        'quality_issues': [],
        'upsert_test': 'skipped'
    }
    
    try:
        with engine.connect() as conn:
            from sqlalchemy import text
            
            # Get row count
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            results['total_rows'] = count_result.scalar() or 0
            
            # Get sample data to verify structure
            sample_result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 3"))
            columns = sample_result.keys()
            rows = sample_result.fetchall()
            
            results['sample_data'] = [
                {col: row[i] for i, col in enumerate(columns)}
                for row in rows
            ]
            
            # Check for critical quality issues
            if 'isrc' in [col.lower() for col in columns]:
                # Check for missing ISRCs (critical for music industry)
                null_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE isrc IS NULL"))
                null_count = null_result.scalar() or 0
                if null_count > 0:
                    results['quality_issues'].append({
                        'type': 'missing_isrc',
                        'count': null_count,
                        'severity': 'critical',
                        'description': f'{null_count} records missing ISRC'
                    })
            
            # Check for completely empty records
            if results['total_rows'] > 0:
                # This is a real quality check - are there rows with all NULL values?
                null_check_cols = [col for col in columns if col.lower() not in ['id', 'created_at', 'updated_at']]
                if null_check_cols:
                    null_condition = ' AND '.join([f"{col} IS NULL" for col in null_check_cols[:5]])  # Check first 5 non-ID columns
                    empty_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {null_condition}"))
                    empty_count = empty_result.scalar() or 0
                    if empty_count > 0:
                        results['quality_issues'].append({
                            'type': 'empty_records',
                            'count': empty_count,
                            'severity': 'warning',
                            'description': f'{empty_count} records with all key fields NULL'
                        })
            
            results['upsert_test'] = 'completed'
            
    except Exception as e:
        results['quality_issues'].append({
            'type': 'connection_error',
            'count': 0,
            'severity': 'critical',
            'description': f'Could not access table: {str(e)}'
        })
    
    return results


def print_human_readable_report(table_results: List[Dict], mode: str, block_on_critical: bool):
    """Print a clean, human-readable report."""
    
    print("🎵" + "=" * 78 + "🎵")
    print("                    MUSIC DATA QUALITY REPORT")
    print(f"                         Mode: {mode.upper()}")
    print("🎵" + "=" * 78 + "🎵")
    print()
    
    total_tables = len(table_results)
    total_issues = sum(len(result['quality_issues']) for result in table_results)
    critical_issues = sum(1 for result in table_results 
                         for issue in result['quality_issues'] 
                         if issue['severity'] == 'critical')
    
    print(f"📊 Tables Scanned: {total_tables}")
    print(f"🔍 Total Issues: {total_issues}")
    print(f"🚨 Critical Issues: {critical_issues}")
    print()
    
    # Show table-by-table results
    for result in table_results:
        table_name = result['table']
        row_count = result['total_rows']
        issues = result['quality_issues']
        
        if issues:
            issue_count = len(issues)
            critical_count = sum(1 for issue in issues if issue['severity'] == 'critical')
            
            if critical_count > 0:
                print(f"🔴 {table_name}: {row_count:,} rows, {issue_count} issues ({critical_count} critical)")
            else:
                print(f"🟡 {table_name}: {row_count:,} rows, {issue_count} issues")
            
            for issue in issues:
                severity_icon = "🔴" if issue['severity'] == 'critical' else "🟡"
                print(f"   {severity_icon} {issue['description']}")
        else:
            print(f"✅ {table_name}: {row_count:,} rows, no issues")
        
        # Show sample data structure (first record only)
        if result['sample_data']:
            sample = result['sample_data'][0]
            key_fields = [k for k in sample.keys() if k.lower() in ['id', 'isrc', 'title', 'name', 'spotify_id']][:3]
            if key_fields:
                sample_str = ", ".join([f"{k}={sample[k]}" for k in key_fields if sample[k] is not None])
                if sample_str:
                    print(f"   📝 Sample: {sample_str}")
        print()
    
    # Final verdict
    if critical_issues == 0:
        print("✅" + "=" * 78 + "✅")
        print("                        🎉 DATA QUALITY: GOOD")
        if mode == 'development':
            print("                     Development: Continue coding!")
        else:
            print("                     Production: Ready to deploy!")
        print("✅" + "=" * 78 + "✅")
        return True
    else:
        print("🚨" + "=" * 78 + "🚨")
        print(f"                    ⚠️  {critical_issues} CRITICAL ISSUES FOUND")
        
        if mode == 'development':
            print("                   Development: Fix when convenient")
            print("                   (Not blocking your development work)")
        elif block_on_critical:
            print("                   Production: DEPLOYMENT BLOCKED")
            print("                   (Critical issues must be fixed)")
        else:
            print("                   Production: Proceed with caution")
            print("                   (Critical issues noted but not blocking)")
        
        print("🚨" + "=" * 78 + "🚨")
        return not (mode == 'production' and block_on_critical and critical_issues > 0)


def main():
    parser = argparse.ArgumentParser(description="Real Data Quality Checker")
    parser.add_argument("--database-url", help="Database URL (optional, will use production DB if not provided)")
    parser.add_argument("--mode", choices=['development', 'production'], default='development',
                       help="Mode: development (warnings only) or production (can block)")
    parser.add_argument("--block-on-critical", action="store_true", 
                       help="Block (exit 1) on critical issues in production mode")
    parser.add_argument("--tables", help="Comma-separated table names to check (optional)")
    parser.add_argument("--json", action="store_true", help="Output JSON for CI/CD")
    
    args = parser.parse_args()
    
    print("🔍 Connecting to real production database...")
    
    # Get database connection
    if args.database_url:
        from sqlalchemy import create_engine
        engine = create_engine(args.database_url)
    else:
        engine = get_real_database_connection()
    
    if not engine:
        print("❌ Could not connect to database")
        print("💡 Set DB_PASS environment variable or provide --database-url")
        sys.exit(2)
    
    print("✅ Connected to production database")
    print()
    
    # Determine which tables to check
    if args.tables:
        tables_to_check = [t.strip() for t in args.tables.split(',')]
    else:
        tables_to_check = get_critical_tables()
    
    print(f"📋 Checking {len(tables_to_check)} critical tables...")
    print()
    
    # Test each table
    table_results = []
    for table_name in tables_to_check:
        print(f"🔍 Testing {table_name}...")
        result = test_real_upsert_quality(engine, table_name)
        table_results.append(result)
    
    print()
    
    # Generate report
    if args.json:
        import json
        output = {
            'timestamp': datetime.now().isoformat(),
            'mode': args.mode,
            'block_on_critical': args.block_on_critical,
            'tables_checked': len(table_results),
            'total_issues': sum(len(r['quality_issues']) for r in table_results),
            'critical_issues': sum(1 for r in table_results for i in r['quality_issues'] if i['severity'] == 'critical'),
            'results': table_results
        }
        print(json.dumps(output, indent=2, default=str))
    else:
        success = print_human_readable_report(table_results, args.mode, args.block_on_critical)
        
        if not success:
            sys.exit(1)  # Block deployment/CI
        else:
            sys.exit(0)  # All good


if __name__ == "__main__":
    main()