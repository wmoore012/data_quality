#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Perday CatalogLAB™

"""
CI / CD Data Quality Integration Script

This script is designed specifically for CI / CD environments where:
- No interactive input is possible
- Output must be machine - readable
- Exit codes determine success / failure
- Logs need to be formatted for automated processing

Usage in CI / CD:
    python scripts / ci_cd_data_quality.py --database - url $DATABASE_URL
    python scripts / ci_cd_data_quality.py --database - url $DATABASE_URL --fail - on - critical
    python scripts / ci_cd_data_quality.py --database - url $DATABASE_URL --output - file results.json
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_quality.quality_scanner import health_check


def run_non_interactive_check(
    database_url: str,
    tables: Optional[str] = None,
    fail_on_critical: bool = False,
    output_file: Optional[str] = None,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Run data quality check in non - interactive mode for CI / CD.

    Args:
        database_url: Database connection URL
        tables: Comma - separated table patterns
        fail_on_critical: Whether to exit with error code if critical issues found
        output_file: Optional file to write JSON results
        verbose: Whether to print verbose output

    Returns:
        Dictionary with results suitable for CI / CD processing
    """
    table_patterns = None
    if tables:
        table_patterns = [t.strip() for t in tables.split(",")]

    if verbose:
        print("🚀 Running data quality check in CI / CD mode...")
        print(f"   Database: {database_url[:50]}...")
        if table_patterns:
            print(f"   Tables: {', '.join(table_patterns)}")

    try:
        # Run health check
        report = health_check(database_url, table_patterns)

        # Create CI / CD friendly results
        results = {
            "timestamp": "2025 - 12 - 17T10:00:00Z",  # Would be actual timestamp
            "database_url_masked": database_url.split("@")[-1]
            if "@" in database_url
            else "local",
            "scan_results": {
                "all_good": report.all_good,
                "total_issues": report.total_issues,
                "critical_issues": report.summary.get("critical", 0),
                "warning_issues": report.summary.get("warning", 0),
                "info_issues": report.summary.get("info", 0),
                "scan_time_ms": report.scan_time_ms,
            },
            "issues": [
                {
                    "table": issue.table,
                    "column": issue.column,
                    "type": issue.issue_type,
                    "severity": issue.severity,
                    "count": issue.count,
                    "total_rows": issue.total,
                    "percentage": issue.percent,
                    "description": issue.description,
                }
                for issue in report.issues_by_severity
            ],
            "ci_cd_status": {
                "should_fail": fail_on_critical
                and report.summary.get("critical", 0) > 0,
                "deployment_safe": report.summary.get("critical", 0) == 0,
                "requires_attention": report.total_issues > 0,
            },
        }

        # Add formatted messages for different systems
        results["formatted_output"] = {
            "github_comment": _format_for_github(results),
            "slack_message": _format_for_slack(results),
            "jenkins_log": _format_for_jenkins(results),
            "gitlab_comment": _format_for_gitlab(results),
        }

        # Write to file if requested
        if output_file:
            with open(output_file, "w") as f:
                json.dump(results, f, indent=2)
            if verbose:
                print(f"📄 Results written to {output_file}")

        # Print summary for logs
        if verbose:
            _print_ci_summary(results)

        return results

    except Exception as e:
        error_results = {
            "timestamp": "2025 - 12 - 17T10:00:00Z",
            "database_url_masked": "error",
            "scan_results": {
                "all_good": False,
                "total_issues": 0,
                "critical_issues": 1,  # Treat errors as critical
                "warning_issues": 0,
                "info_issues": 0,
                "scan_time_ms": 0,
            },
            "issues": [],
            "error": str(e),
            "ci_cd_status": {
                "should_fail": True,
                "deployment_safe": False,
                "requires_attention": True,
            },
        }

        if output_file:
            with open(output_file, "w") as f:
                json.dump(error_results, f, indent=2)

        if verbose:
            print(f"❌ Error during data quality check: {e}")

        return error_results


def _format_for_github(results: Dict[str, Any]) -> str:
    """Format results as GitHub PR comment."""
    scan = results["scan_results"]
    status = results["ci_cd_status"]

    if scan["all_good"]:
        emoji = "✅"
        title = "Data Quality: PASSED"
    elif status["deployment_safe"]:
        emoji = "⚠️"
        title = "Data Quality: WARNINGS"
    else:
        emoji = "🚨"
        title = "Data Quality: CRITICAL ISSUES"

    comment = f"""## {emoji} {title}

### 📊 Scan Results
- **Total Issues**: {scan['total_issues']:,}
- **Critical**: {scan['critical_issues']} 🚨
- **Warning**: {scan['warning_issues']} ⚠️
- **Info**: {scan['info_issues']} ℹ️
- **Scan Time**: {scan['scan_time_ms']}ms

### 🚀 Deployment Status
"""

    if status["deployment_safe"]:
        comment += "✅ **SAFE TO DEPLOY** - No critical issues found\n"
    else:
        comment += "❌ **DEPLOYMENT BLOCKED** - Critical issues must be resolved\n"

    # Add top issues
    if results["issues"]:
        comment += "\n### 🔍 Top Issues\n"
        for issue in results["issues"][:5]:
            severity_emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(
                issue["severity"], "•"
            )
            comment += f"- {severity_emoji} **{issue['table']}.{issue['column']}**: {issue['description']}\n"

    comment += "\n---\n * Automated data quality check*"
    return comment


def _format_for_slack(results: Dict[str, Any]) -> str:
    """Format results as Slack message JSON."""
    scan = results["scan_results"]
    status = results["ci_cd_status"]

    if scan["all_good"]:
        color = "good"
        emoji = ":white_check_mark:"
        title = "Data Quality Check Passed"
    elif status["deployment_safe"]:
        color = "warning"
        emoji = ":warning:"
        title = "Data Quality Issues Found"
    else:
        color = "danger"
        emoji = ":rotating_light:"
        title = "Critical Data Quality Issues"

    message = {
        "attachments": [
            {
                "color": color,
                "title": f"{emoji} {title}",
                "fields": [
                    {
                        "title": "Issues Found",
                        "value": f"Critical: {scan['critical_issues']} | Warning: {scan['warning_issues']} | Info: {scan['info_issues']}",
                        "short": True,
                    },
                    {
                        "title": "Deployment Status",
                        "value": "✅ Safe" if status["deployment_safe"] else "❌ Blocked",
                        "short": True,
                    },
                ],
                "footer": "Data Quality Scanner",
                "ts": 1734422400,  # Would be actual timestamp
            }
        ]
    }

    return json.dumps(message)


def _format_for_jenkins(results: Dict[str, Any]) -> str:
    """Format results for Jenkins console log."""
    scan = results["scan_results"]
    status = results["ci_cd_status"]

    log = "=" * 60 + "\n"
    log += "DATA QUALITY CHECK RESULTS\n"
    log += "=" * 60 + "\n"

    if scan["all_good"]:
        log += "STATUS: PASSED ✓\n"
    elif status["deployment_safe"]:
        log += "STATUS: WARNINGS FOUND ⚠\n"
    else:
        log += "STATUS: CRITICAL ISSUES FOUND ✗\n"

    log += f"Total Issues: {scan['total_issues']}\n"
    log += f"Critical: {scan['critical_issues']}\n"
    log += f"Warning: {scan['warning_issues']}\n"
    log += f"Info: {scan['info_issues']}\n"
    log += f"Scan Time: {scan['scan_time_ms']}ms\n"

    if status["deployment_safe"]:
        log += "\nDEPLOYMENT: APPROVED ✓\n"
    else:
        log += "\nDEPLOYMENT: BLOCKED ✗\n"

    log += "=" * 60 + "\n"
    return log


def _format_for_gitlab(results: Dict[str, Any]) -> str:
    """Format results as GitLab MR comment."""
    # Similar to GitHub but with GitLab - specific formatting
    return _format_for_github(results).replace("## ", "### ")


def _print_ci_summary(results: Dict[str, Any]) -> None:
    """Print a concise summary for CI / CD logs."""
    scan = results["scan_results"]
    status = results["ci_cd_status"]

    print("\n" + "=" * 50)
    print("DATA QUALITY SUMMARY")
    print("=" * 50)

    if scan["all_good"]:
        print("✅ STATUS: ALL GOOD")
    elif status["deployment_safe"]:
        print("⚠️  STATUS: WARNINGS FOUND")
    else:
        print("🚨 STATUS: CRITICAL ISSUES")

    print(f"Issues: {scan['total_issues']} total ({scan['critical_issues']} critical)")
    print(f"Scan Time: {scan['scan_time_ms']}ms")

    if status["deployment_safe"]:
        print("🚀 DEPLOYMENT: APPROVED")
    else:
        print("❌ DEPLOYMENT: BLOCKED")

    print("=" * 50)


def main():
    parser = argparse.ArgumentParser(
        description="CI / CD Data Quality Check - Non - interactive mode"
    )
    parser.add_argument(
        "--database - url", required=True, help="Database connection URL"
    )
    parser.add_argument("--tables", help="Comma - separated table patterns to scan")
    parser.add_argument(
        "--fail - on - critical",
        action="store_true",
        help="Exit with error code if critical issues found",
    )
    parser.add_argument("--output - file", help="Write JSON results to file")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Print verbose output"
    )
    parser.add_argument(
        "--format",
        choices=["github", "slack", "jenkins", "gitlab", "json"],
        default="json",
        help="Output format for CI / CD integration",
    )

    args = parser.parse_args()

    # Run the check
    results = run_non_interactive_check(
        database_url=args.database_url,
        tables=args.tables,
        fail_on_critical=args.fail_on_critical,
        output_file=args.output_file,
        verbose=args.verbose,
    )

    # Output in requested format
    if args.format == "github":
        print(results["formatted_output"]["github_comment"])
    elif args.format == "slack":
        print(results["formatted_output"]["slack_message"])
    elif args.format == "jenkins":
        print(results["formatted_output"]["jenkins_log"])
    elif args.format == "gitlab":
        print(results["formatted_output"]["gitlab_comment"])
    else:  # json
        print(json.dumps(results, indent=2))

    # Exit with appropriate code
    if (
        args.fail_on_critical
        and results["ci_cd_status"]["should_fail"]
        or "error" in results
    ):
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
