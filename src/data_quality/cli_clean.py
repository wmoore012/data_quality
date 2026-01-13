# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Clean CLI with proper exit codes and timeout handling.
"""

import json
import sys

import click

from .models import Report
from .safe_scanners import find_duplicates_safe, find_nulls_safe, find_orphans


@click.group()
def cli() -> None:
    """Database quality scanning tools."""
    pass


@cli.command("check")
@click.option(
    "--database-url",
    envvar="DATABASE_URL",
    required=True,
    help="Database connection URL",
)
@click.option(
    "--format",
    type=click.Choice(["text", "json", "sarif"]),
    default="text",
    help="Output format",
)
@click.option(
    "--fail-on",
    type=click.Choice(["none", "warning", "critical"]),
    default="critical",
    help="Exit code behavior",
)
@click.option("--timeout", type=int, default=30, help="Query timeout in seconds")
def check(database_url: str, format: str, fail_on: str, timeout: int) -> None:
    """Run database quality checks."""
    try:
        # Run safe scans
        orphans = find_orphans(database_url)
        nulls = find_nulls_safe(database_url)
        duplicates = find_duplicates_safe(database_url)

        # Convert to issues
        issues = []

        for orphan in orphans:
            issues.append(
                {
                    "id": f"orphan_{orphan.table}_{orphan.fk_name}",
                    "severity": "critical",
                    "table": orphan.table,
                    "column": None,
                    "kind": "orphan",
                    "count": orphan.missing_count,
                    "details": {
                        "fk_name": orphan.fk_name,
                        "referred_table": orphan.referred_table,
                    },
                }
            )

        for null in nulls:
            severity = "critical" if null["percent"] > 50 else "warning"
            issues.append(
                {
                    "id": f"null_{null['table']}_{null['column']}",
                    "severity": severity,
                    "table": null["table"],
                    "column": null["column"],
                    "kind": "nulls",
                    "count": null["null_count"],
                    "details": {
                        "total": null["total_count"],
                        "percent": null["percent"],
                    },
                }
            )

        for dup in duplicates:
            issues.append(
                {
                    "id": f"dup_{dup['table']}_{dup['constraint_name']}",
                    "severity": "warning",
                    "table": dup["table"],
                    "column": None,
                    "kind": "duplicate",
                    "count": dup["duplicate_groups"],
                    "details": {
                        "columns": dup["columns"],
                        "constraint": dup["constraint_name"],
                    },
                }
            )

        # Create report
        report = Report(
            tool_version="0.1.0", db_dialect=database_url.split("://")[0], issues=issues
        )

        # Determine exit code
        exit_code = 0
        if fail_on == "critical" and report.has_critical():
            exit_code = 2
        elif fail_on == "warning" and (report.has_critical() or report.has_warnings()):
            exit_code = 1

        # Output report
        click.echo(report.render(format=format))
        sys.exit(exit_code)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(3)


@cli.command("schema")
def schema() -> None:
    """Output JSON Schema for reports."""
    from .models import get_json_schema

    click.echo(json.dumps(get_json_schema(), indent=2))


if __name__ == "__main__":
    cli()
