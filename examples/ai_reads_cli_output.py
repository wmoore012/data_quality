#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Example: How your AI can read data-quality CLI output

This shows the pattern you mentioned - your AI reads the beautiful
CLI output and generates intelligent comments for CI/CD.

This is much better than my API-based approach because:
1. Your AI sees exactly what humans see (the colorful output)
2. No API dependencies in the core tool
3. Your AI can be as smart as you want
4. Works with any AI model you choose
"""

import subprocess
import json
from typing import Dict, Any


def run_data_quality_cli(database_url: str, command: str = "check") -> Dict[str, Any]:
    """
    Run the data-quality CLI and capture output for AI analysis.
    
    This is the pattern you described - run the CLI tool and let
    your AI read the output like a human would.
    """
    
    # Run with JSON output for structured data
    json_cmd = ["data-quality", command, "--database-url", database_url, "--format", "json"]
    json_result = subprocess.run(json_cmd, capture_output=True, text=True)
    
    # Run with text output for beautiful formatting
    text_cmd = ["data-quality", command, "--database-url", database_url]
    text_result = subprocess.run(text_cmd, capture_output=True, text=True)
    
    return {
        "json_output": json_result.stdout,
        "json_errors": json_result.stderr,
        "json_exit_code": json_result.returncode,
        "colorful_output": text_result.stdout,
        "colorful_errors": text_result.stderr,
        "text_exit_code": text_result.returncode,
        "success": json_result.returncode == 0
    }


def your_ai_analyzes_output(cli_result: Dict[str, Any]) -> str:
    """
    This is where YOUR AI would analyze the CLI output.
    
    Your AI gets to see:
    1. The beautiful colorful output (like a human)
    2. The structured JSON data
    3. Any error messages
    4. Exit codes
    
    Then it can generate intelligent comments for CI/CD.
    """
    
    # Your AI would read the colorful output and understand it
    colorful_output = cli_result["colorful_output"]
    json_data = None
    
    if cli_result["json_output"]:
        try:
            json_data = json.loads(cli_result["json_output"])
        except json.JSONDecodeError:
            pass
    
    # Example of what your AI might generate
    if cli_result["success"]:
        if json_data and json_data.get("all_good", False):
            return """
## ✅ Data Quality: EXCELLENT

Your database is in great shape! No issues detected.

**Scan Results:**
- 🚀 Lightning-fast scan completed in {}ms
- ✅ All quality checks passed
- 🎯 Ready for deployment

**AI Assessment:**
This database follows excellent data quality practices. The scan found no null values in critical columns, no orphaned records, and no duplicate entries. Your ETL processes are working well.

**Recommendation:** 
Continue current practices. Consider setting up automated monitoring to maintain this excellent quality level.
""".format(json_data.get("scan_time_ms", "unknown"))
        
        else:
            # Parse issues from JSON or colorful output
            issues_found = json_data.get("total_issues", 0) if json_data else "some"
            critical_issues = json_data.get("summary", {}).get("critical", 0) if json_data else 0
            
            if critical_issues > 0:
                return f"""
## 🚨 Data Quality: CRITICAL ISSUES FOUND

Found {critical_issues} critical issues that need immediate attention.

**Scan Results:**
{colorful_output}

**AI Assessment:**
Critical data quality issues detected. These issues could impact:
- Data integrity and reliability
- Application functionality
- Business decision accuracy

**Immediate Actions Required:**
1. Review critical issues listed above
2. Fix data integrity problems before deployment
3. Investigate root cause in ETL processes
4. Consider data validation improvements

**Deployment Status:** ❌ BLOCKED until critical issues resolved
"""
            else:
                return f"""
## ⚠️ Data Quality: MINOR ISSUES FOUND

Found {issues_found} minor issues for review.

**Scan Results:**
{colorful_output}

**AI Assessment:**
Minor data quality issues detected. While not blocking deployment, these should be addressed to maintain data excellence.

**Recommended Actions:**
1. Review issues during next maintenance window
2. Consider adding data validation rules
3. Monitor trends to prevent degradation

**Deployment Status:** ✅ APPROVED (minor issues don't block deployment)
"""
    
    else:
        return f"""
## ❌ Data Quality Check Failed

**Error Output:**
{cli_result['colorful_errors']}

**AI Assessment:**
The data quality scan failed to complete. This could indicate:
- Database connectivity issues
- Permission problems
- Configuration errors

**Action Required:**
1. Check database connectivity
2. Verify credentials and permissions
3. Review configuration settings
4. Re-run scan after fixes

**Deployment Status:** ❌ BLOCKED until scan completes successfully
"""


def generate_ci_cd_comment(database_url: str) -> str:
    """
    Complete workflow: Run CLI → Your AI analyzes → Generate comment
    
    This is the pattern you described and it's much better than
    my API-based approach!
    """
    
    print("🚀 Running data quality CLI...")
    cli_result = run_data_quality_cli(database_url)
    
    print("🧠 Your AI analyzing output...")
    ai_comment = your_ai_analyzes_output(cli_result)
    
    print("📝 Generated CI/CD comment:")
    return ai_comment


if __name__ == "__main__":
    # Example usage
    database_url = "sqlite:///:memory:"  # Replace with real DB
    
    comment = generate_ci_cd_comment(database_url)
    print(comment)
    
    print("\n" + "="*60)
    print("This is the pattern you described:")
    print("1. ⚡ Lightning-fast CLI tool (pure, no AI deps)")
    print("2. 🧠 Your AI reads the beautiful output")
    print("3. 📝 Generate intelligent CI/CD comments")
    print("4. 🚀 Perfect for professional teams!")
    print("="*60)