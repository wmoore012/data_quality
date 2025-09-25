#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Deployment Gate - Data Quality Check

This script demonstrates how the data-quality scanner blocks deployments
when critical data quality issues are detected.

Usage in CI/CD:
    python deployment_gate.py --database-url $DATABASE_URL
    
Exit codes:
    0: All good - deployment can proceed
    1: Critical issues found - deployment BLOCKED
    2: Configuration error
"""

import argparse
import json
import sys
import os
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data_quality.quality_scanner import health_check


def print_deployment_status(report, database_url):
    """Print deployment status with dramatic formatting."""
    
    print("🚀" + "=" * 78 + "🚀")
    print("                    DEPLOYMENT QUALITY GATE")
    print("🚀" + "=" * 78 + "🚀")
    print()
    
    # Database info
    db_type = database_url.split("://")[0].upper()
    print(f"📊 Database: {db_type}")
    print(f"⏱️  Scan Time: {report.scan_time_ms}ms")
    print(f"🔍 Issues Found: {report.total_issues}")
    print()
    
    if report.all_good:
        print("✅" + "=" * 78 + "✅")
        print("                        🎉 ALL CLEAR! 🎉")
        print("                   DEPLOYMENT: APPROVED")
        print("✅" + "=" * 78 + "✅")
        return True
    
    # Critical issues found
    critical_count = report.summary.get("critical", 0)
    warning_count = report.summary.get("warning", 0)
    info_count = report.summary.get("info", 0)
    
    print("🚨" + "=" * 78 + "🚨")
    print("                      ⚠️  QUALITY ISSUES DETECTED")
    print(f"                    Issues: {report.total_issues} total ({critical_count} critical)")
    print(f"                    Scan Time: {report.scan_time_ms}ms")
    
    if critical_count > 0:
        print("                    ❌ DEPLOYMENT: BLOCKED")
        print("🚨" + "=" * 78 + "🚨")
        print()
        
        print("🔴 CRITICAL ISSUES (Deployment Blockers):")
        print("-" * 50)
        for issue in report.issues_by_severity:
            if issue.severity == "critical":
                print(f"  • {issue.description}")
                print(f"    Impact: {issue.count:,} records affected ({issue.percent:.1f}%)")
                print(f"    Table: {issue.table}, Column: {issue.column}")
                print()
        
        print("💡 RESOLUTION REQUIRED:")
        print("  1. Fix critical data quality issues above")
        print("  2. Re-run deployment after fixes are applied")
        print("  3. Critical issues must be resolved before deployment")
        
    else:
        print("                    ⚠️  DEPLOYMENT: PROCEED WITH CAUTION")
        print("🚨" + "=" * 78 + "🚨")
    
    print()
    
    if warning_count > 0:
        print("🟡 WARNINGS (Non-blocking but should be addressed):")
        print("-" * 50)
        for issue in report.issues_by_severity:
            if issue.severity == "warning":
                print(f"  • {issue.description}")
        print()
    
    if info_count > 0:
        print("🔵 INFO (Recommendations):")
        print("-" * 50)
        for issue in report.issues_by_severity:
            if issue.severity == "info":
                print(f"  • {issue.description}")
        print()
    
    return critical_count == 0


def main():
    parser = argparse.ArgumentParser(description="Data Quality Deployment Gate")
    parser.add_argument("--database-url", required=True, help="Database connection URL")
    parser.add_argument("--tables", help="Comma-separated table patterns to check")
    parser.add_argument("--json", action="store_true", help="Output JSON for CI/CD integration")
    parser.add_argument("--strict", action="store_true", help="Treat warnings as blockers too")
    
    args = parser.parse_args()
    
    # Parse table patterns
    table_patterns = None
    if args.tables:
        table_patterns = [t.strip() for t in args.tables.split(",")]
    
    try:
        # Run health check
        print("🔍 Running data quality scan...")
        report = health_check(args.database_url, table_patterns)
        
        if args.json:
            # JSON output for CI/CD systems
            output = {
                "timestamp": datetime.now().isoformat(),
                "database_url": args.database_url.split("@")[-1] if "@" in args.database_url else args.database_url,
                "all_good": report.all_good,
                "total_issues": report.total_issues,
                "summary": report.summary,
                "scan_time_ms": report.scan_time_ms,
                "deployment_approved": report.summary.get("critical", 0) == 0,
                "issues": [
                    {
                        "table": issue.table,
                        "column": issue.column,
                        "type": issue.issue_type,
                        "severity": issue.severity,
                        "count": issue.count,
                        "percent": issue.percent,
                        "description": issue.description
                    }
                    for issue in report.issues_by_severity
                ]
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-readable output
            deployment_approved = print_deployment_status(report, args.database_url)
        
        # Exit codes for CI/CD
        critical_count = report.summary.get("critical", 0)
        warning_count = report.summary.get("warning", 0)
        
        if critical_count > 0:
            print(f"\n💥 DEPLOYMENT BLOCKED: {critical_count} critical issues must be fixed")
            sys.exit(1)  # Block deployment
        elif args.strict and warning_count > 0:
            print(f"\n⚠️  DEPLOYMENT BLOCKED (strict mode): {warning_count} warnings found")
            sys.exit(1)  # Block deployment in strict mode
        else:
            if not args.json:
                print("\n🚀 DEPLOYMENT APPROVED: No critical issues found")
            sys.exit(0)  # Allow deployment
            
    except Exception as e:
        print(f"❌ Deployment gate failed: {e}")
        if args.json:
            error_output = {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "deployment_approved": False
            }
            print(json.dumps(error_output, indent=2))
        sys.exit(2)  # Configuration error


if __name__ == "__main__":
    main()