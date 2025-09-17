"""
Command-line interface for data-quality.

Provides easy-to-use commands for database quality scanning with colorful output.
"""

import os
import sys
from typing import Optional

import click

from .quality_scanner import health_check, scan_nulls, scan_orphans
from .schema_analyzer import analyze_schema, suggest_improvements


@click.group()
def cli():
    """Database quality scanning tools."""
    pass


@cli.command()
@click.option(
    "--database-url",
    "-d",
    help="Database URL (mysql://user:pass@host/db, postgresql://user:pass@host/db, or set DATABASE_URL env var)",
)
@click.option("--tables", "-t", help="Comma-separated table patterns to scan")
@click.option(
    "--format", "-f", type=click.Choice(["text", "json"]), default="text", help="Output format"
)
@click.option(
    "--no-recommendations", is_flag=True, help="Disable schema improvement recommendations"
)
@click.option("--no-ai", is_flag=True, help="Disable AI-powered suggestions")
def check(
    database_url: Optional[str],
    tables: Optional[str],
    format: str,
    no_recommendations: bool,
    no_ai: bool,
):
    """Run comprehensive database health check."""

    # Get database URL from parameter or environment
    db_url = database_url or os.getenv("DATABASE_URL")
    if not db_url:
        click.echo(
            "Error: Database URL required. Use --database-url or set DATABASE_URL env var.",
            err=True,
        )
        sys.exit(1)

    # Parse table patterns
    table_patterns = None
    if tables:
        table_patterns = [t.strip() for t in tables.split(",")]

    try:
        # Run health check
        report = health_check(db_url, table_patterns)

        if format == "json":
            import json

            output = {
                "all_good": report.all_good,
                "total_issues": report.total_issues,
                "summary": report.summary,
                "scan_time_ms": report.scan_time_ms,
                "issues": [
                    {
                        "table": issue.table,
                        "column": issue.column,
                        "issue_type": issue.issue_type,
                        "count": issue.count,
                        "total": issue.total,
                        "percent": issue.percent,
                        "severity": issue.severity,
                        "description": issue.description,
                    }
                    for issue in report.issues_by_severity
                ],
            }
            click.echo(json.dumps(output, indent=2))
        else:
            # Text format
            if report.all_good:
                click.echo("✅ All good! No data quality issues found.")
            else:
                click.echo(f"❌ Found {report.total_issues} data quality issues:")
                click.echo(f"   Critical: {report.summary.get('critical', 0)}")
                click.echo(f"   Warning:  {report.summary.get('warning', 0)}")
                click.echo(f"   Info:     {report.summary.get('info', 0)}")
                click.echo()

                for issue in report.issues_by_severity:
                    severity_icon = {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(
                        issue.severity, "⚪"
                    )

                    click.echo(f"{severity_icon} {issue.severity.upper()}: {issue.description}")

            click.echo(f"\nScan completed in {report.scan_time_ms}ms")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--database-url", "-d", help="Database URL (or set DATABASE_URL env var)")
@click.option("--tables", "-t", help="Comma-separated table patterns to scan")
def nulls(database_url: Optional[str], tables: Optional[str]):
    """Scan for null values in key columns."""

    db_url = database_url or os.getenv("DATABASE_URL")
    if not db_url:
        click.echo(
            "Error: Database URL required. Use --database-url or set DATABASE_URL env var.",
            err=True,
        )
        sys.exit(1)

    table_patterns = None
    if tables:
        table_patterns = [t.strip() for t in tables.split(",")]

    try:
        issues = scan_nulls(db_url, table_patterns)

        if not issues:
            click.echo("✅ No null value issues found.")
        else:
            click.echo(f"Found {len(issues)} null value issues:")
            for issue in issues:
                severity_icon = {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(
                    issue.severity, "⚪"
                )

                click.echo(f"{severity_icon} {issue.description}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--database-url", "-d", help="Database URL (or set DATABASE_URL env var)")
@click.option("--tables", "-t", help="Comma-separated table patterns to scan")
def orphans(database_url: Optional[str], tables: Optional[str]):
    """Scan for orphaned records (broken foreign key references)."""

    db_url = database_url or os.getenv("DATABASE_URL")
    if not db_url:
        click.echo(
            "Error: Database URL required. Use --database-url or set DATABASE_URL env var.",
            err=True,
        )
        sys.exit(1)

    table_patterns = None
    if tables:
        table_patterns = [t.strip() for t in tables.split(",")]

    try:
        issues = scan_orphans(db_url, table_patterns)

        if not issues:
            click.echo("✅ No orphaned records found.")
        else:
            click.echo(f"Found {len(issues)} orphaned record issues:")
            for issue in issues:
                click.echo(f"🔴 {issue.description}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--database-url",
    "-d",
    help="Database URL (mysql://user:pass@host/db, postgresql://user:pass@host/db)",
)
@click.option("--table", "-t", required=True, help="Table name to analyze")
@click.option("--no-normalization", is_flag=True, help="Disable normalization suggestions")
@click.option("--no-boolean-suggestions", is_flag=True, help="Disable boolean column suggestions")
@click.option("--no-fact-analysis", is_flag=True, help="Disable fact table analysis")
@click.option(
    "--generate-sql", is_flag=True, help="Generate ALTER TABLE statements for recommendations"
)
def analyze(
    database_url: Optional[str],
    table: str,
    no_normalization: bool,
    no_boolean_suggestions: bool,
    no_fact_analysis: bool,
    generate_sql: bool,
):
    """Analyze database schema and suggest improvements."""

    db_url = database_url or os.getenv("DATABASE_URL")
    if not db_url:
        click.echo(
            "❌ Error: Database URL required. Use --database-url or set DATABASE_URL env var.",
            err=True,
        )
        sys.exit(1)

    try:
        # Run schema analysis
        analysis = analyze_schema(
            db_url,
            table,
            include_normalization=not no_normalization,
            include_boolean_suggestions=not no_boolean_suggestions,
            include_fact_analysis=not no_fact_analysis,
        )

        # Display results with colors
        click.echo(f"\n🔍 Schema Analysis for table: {click.style(table, fg='cyan', bold=True)}")
        click.echo("=" * 50)

        # Natural keys
        if analysis.natural_keys:
            click.echo(
                f"\n🔑 Natural Keys Found: {click.style(', '.join(analysis.natural_keys), fg='green')}"
            )
        else:
            click.echo(f"\n🔑 Natural Keys: {click.style('None detected', fg='yellow')}")

        # Boolean columns
        if analysis.boolean_columns:
            click.echo(
                f"\n✅ Boolean Columns: {click.style(', '.join(analysis.boolean_columns), fg='green')}"
            )

        # Boolean suggestions
        if analysis.suggested_booleans:
            click.echo("\n💡 Suggested Boolean Conversions:")
            for col, suggestion in analysis.suggested_booleans.items():
                first_option = suggestion.split("/")[0]
                click.echo(
                    f"   • {click.style(col, fg='yellow')} → {click.style(f'is_{first_option}', fg='green')} (currently: {suggestion})"
                )

                if generate_sql:
                    click.echo(
                        f"     {click.style('SQL:', fg='blue')} ALTER TABLE {table} ADD COLUMN is_{first_option} BOOLEAN;"
                    )

        # Fact table analysis
        if analysis.fact_table_candidate:
            click.echo(
                f"\n📊 {click.style('Fact Table Candidate', fg='magenta', bold=True)} - Consider dimensional modeling"
            )

        # Normalization level
        nf_color = (
            "green"
            if analysis.normalization_level >= 3
            else "yellow" if analysis.normalization_level == 2 else "red"
        )
        click.echo(
            f"\n📐 Normalization Level: {click.style(f'{analysis.normalization_level}NF', fg=nf_color)}"
        )

        # Recommendations
        if analysis.recommendations:
            click.echo("\n🚀 Recommendations:")
            for i, rec in enumerate(analysis.recommendations, 1):
                priority_color = {"high": "red", "medium": "yellow", "low": "green"}.get(
                    rec.priority, "white"
                )
                priority_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(rec.priority, "⚪")

                click.echo(
                    f"\n   {i}. {priority_icon} {click.style(rec.priority.upper(), fg=priority_color, bold=True)}: {rec.description}"
                )

                if rec.benefits:
                    click.echo(f"      Benefits: {', '.join(rec.benefits)}")

                if generate_sql and rec.sql_example:
                    click.echo(f"\n      {click.style('SQL Example:', fg='blue', bold=True)}")
                    for line in rec.sql_example.split("\n"):
                        if line.strip():
                            click.echo(f"      {click.style(line, fg='cyan')}")
        else:
            click.echo(
                f"\n✨ {click.style('No recommendations - schema looks good!', fg='green', bold=True)}"
            )

    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--database-url",
    "-d",
    help="Database URL (mysql://user:pass@host/db, postgresql://user:pass@host/db)",
)
@click.option("--tables", "-t", help="Comma-separated table names to analyze")
@click.option("--use-ai", is_flag=True, help="Include AI-powered recommendations (experimental)")
def suggest(database_url: Optional[str], tables: Optional[str], use_ai: bool):
    """Get comprehensive improvement suggestions for multiple tables."""

    db_url = database_url or os.getenv("DATABASE_URL")
    if not db_url:
        click.echo(
            "❌ Error: Database URL required. Use --database-url or set DATABASE_URL env var.",
            err=True,
        )
        sys.exit(1)

    if not tables:
        click.echo("❌ Error: Table names required. Use --tables option.", err=True)
        sys.exit(1)

    table_list = [t.strip() for t in tables.split(",")]

    try:
        suggestions = suggest_improvements(db_url, table_list, use_ai=use_ai)

        if not suggestions:
            click.echo(
                f"✨ {click.style('No suggestions - your schema looks great!', fg='green', bold=True)}"
            )
            return

        click.echo(f"\n🎯 Improvement Suggestions for {len(table_list)} table(s)")
        click.echo("=" * 50)

        # Group by priority
        high_priority = [s for s in suggestions if s.priority == "high"]
        medium_priority = [s for s in suggestions if s.priority == "medium"]
        low_priority = [s for s in suggestions if s.priority == "low"]

        for priority_group, priority_name, color, icon in [
            (high_priority, "HIGH PRIORITY", "red", "🔴"),
            (medium_priority, "MEDIUM PRIORITY", "yellow", "🟡"),
            (low_priority, "LOW PRIORITY", "green", "🟢"),
        ]:
            if priority_group:
                click.echo(f"\n{icon} {click.style(priority_name, fg=color, bold=True)}")
                click.echo("-" * 30)

                for i, suggestion in enumerate(priority_group, 1):
                    click.echo(f"\n{i}. {suggestion.description}")
                    if suggestion.benefits:
                        click.echo(f"   Benefits: {', '.join(suggestion.benefits)}")
                    click.echo(f"   Effort: {click.style(suggestion.effort_level, fg='cyan')}")

                    if suggestion.sql_example:
                        click.echo(f"   {click.style('SQL Example:', fg='blue', bold=True)}")
                        for line in suggestion.sql_example.split('\n'):
                            if line.strip():
                                click.echo(f"      {click.style(line.strip(), fg='cyan')}")

    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
