#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Perday CatalogLAB™

"""
Example: AI - Enhanced Data Quality Analysis

This example shows how to combine the lightning - fast scanner
with AI analysis for intelligent insights.
"""

import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from data_quality.ai_integration import analyze_database_with_ai


def main():
    """Run AI - enhanced data quality analysis example."""

    # Example database URL (replace with your actual database)
    database_url = os.getenv("DATABASE_URL", "sqlite:///example.db")

    print("🚀 AI - Enhanced Data Quality Analysis Example")
    print("=" * 60)
    print(f"Database: {database_url}")
    print()

    try:
        # Run complete analysis (scanner + AI)
        print("🔍 Running lightning - fast scan + AI analysis...")
        analysis = analyze_database_with_ai(
            database_url=database_url,
            ai_provider="openai",  # or "anthropic"
            model="gpt - 4",
            table_patterns=["songs", "albums", "artists"],  # Focus on music tables
        )

        # Display results
        scanner = analysis["scanner_report"]
        ai = analysis["ai_analysis"]
        combined = analysis["combined_assessment"]

        print("📊 SCANNER RESULTS:")
        print(f"  Total Issues: {scanner['total_issues']}")
        print(f"  Critical: {scanner['summary'].get('critical', 0)}")
        print(f"  Warning: {scanner['summary'].get('warning', 0)}")
        print(f"  Info: {scanner['summary'].get('info', 0)}")
        print(f"  Scan Time: {scanner['scan_time_ms']}ms")
        print()

        print("🧠 AI ANALYSIS:")
        print(f"  Summary: {ai['summary']}")
        print(f"  Severity: {ai['severity_assessment']}")
        print(f"  Confidence: {ai['confidence_score']:.0%}")
        print(f"  Business Impact: {ai['business_impact']}")
        print()

        if ai["recommended_actions"]:
            print("🎯 RECOMMENDED ACTIONS:")
            for i, action in enumerate(ai["recommended_actions"], 1):
                print(f"  {i}. {action}")
            print()

        if ai["sql_fixes"]:
            print("🔧 SQL FIXES:")
            for fix in ai["sql_fixes"]:
                print(f"  • {fix}")
            print()

        print("🚀 DEPLOYMENT DECISION:")
        if combined["can_deploy"]:
            print("  ✅ APPROVED - No critical issues blocking deployment")
        else:
            print("  ❌ BLOCKED - Critical issues must be resolved first")

        print()
        print("💡 This analysis took seconds, not minutes!")
        print("   Use in CI / CD for automated quality gates.")

    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        print()
        print("💡 Make sure you have:")
        print("  1. A valid database with data")
        print("  2. OPENAI_API_KEY environment variable set")
        print("  3. OpenAI package installed: pip install openai")


if __name__ == "__main__":
    main()
