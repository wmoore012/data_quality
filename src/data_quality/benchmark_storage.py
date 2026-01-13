# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Professional benchmark storage for data quality results.

This module handles upserting benchmark results into proper database tables
for tracking performance trends, issue patterns, and CI/CD integration.
"""

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    DECIMAL,
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine

from .quality_scanner import HealthReport, QualityIssue


def create_benchmark_tables(engine: Engine) -> None:
    """
    Create benchmark tables if they don't exist.

    Professional approach: Create tables programmatically with proper schema.
    """
    metadata = MetaData()

    # Main benchmarks table
    _benchmarks_table = Table(
        "data_quality_benchmarks",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("scan_timestamp", DateTime, default=datetime.utcnow),
        Column("database_name", String(100), nullable=False),
        Column("database_host", String(255)),
        Column("scan_type", String(50), default="health_check"),
        Column("scan_duration_ms", Integer, nullable=False),
        Column("total_issues", Integer, nullable=False, default=0),
        Column("critical_issues", Integer, nullable=False, default=0),
        Column("warning_issues", Integer, nullable=False, default=0),
        Column("info_issues", Integer, nullable=False, default=0),
        Column("all_good", Boolean, nullable=False, default=False),
        Column("tables_scanned", Integer, nullable=False, default=0),
        Column("total_rows_scanned", Integer, nullable=False, default=0),
        Column("scan_speed_rows_per_second", Integer),
        Column("memory_usage_mb", DECIMAL(10, 2)),
        Column("deployment_safe", Boolean, nullable=False, default=False),
        Column("ci_cd_pipeline", String(100)),
        Column("git_commit_hash", String(40)),
        Column("git_branch", String(100)),
        Column("issues_json", JSON),
        Column("scan_config_json", JSON),
        Column("performance_metrics_json", JSON),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )

    # Create tables
    metadata.create_all(engine, checkfirst=True)


def upsert_benchmark_results(
    engine: Engine,
    database_name: str,
    report: HealthReport,
    performance_metrics: Dict[str, Any],
    ci_cd_context: Optional[Optional[Dict[str, Any]]] = None,
) -> int:
    """
    Upsert benchmark results into the database like a pro.

    Args:
        engine: SQLAlchemy engine
        database_name: Name of the database that was scanned
        report: Health report from the scan
        performance_metrics: Performance metrics (scan time, memory, etc.)
        ci_cd_context: Optional CI/CD context (git info, pipeline info)

    Returns:
        ID of the inserted/updated record
    """
    # Ensure tables exist
    create_benchmark_tables(engine)

    # Extract database host from engine URL
    database_host = (
        str(engine.url).split("@")[-1].split("/")[0]
        if "@" in str(engine.url)
        else "localhost"
    )

    # Prepare data for upsert
    benchmark_data = {
        "scan_timestamp": datetime.utcnow(),
        "database_name": database_name,
        "database_host": database_host,
        "scan_type": "health_check",
        "scan_duration_ms": report.scan_time_ms,
        "total_issues": report.total_issues,
        "critical_issues": report.summary.get("critical", 0),
        "warning_issues": report.summary.get("warning", 0),
        "info_issues": report.summary.get("info", 0),
        "all_good": report.all_good,
        "tables_scanned": performance_metrics.get("tables_scanned", 0),
        "total_rows_scanned": performance_metrics.get("total_rows_scanned", 0),
        "scan_speed_rows_per_second": performance_metrics.get(
            "scan_speed_rows_per_second"
        ),
        "memory_usage_mb": performance_metrics.get("memory_usage_mb"),
        "deployment_safe": report.summary.get("critical", 0) == 0,
        "ci_cd_pipeline": ci_cd_context.get("pipeline") if ci_cd_context else None,
        "git_commit_hash": ci_cd_context.get("commit_hash") if ci_cd_context else None,
        "git_branch": ci_cd_context.get("branch") if ci_cd_context else None,
        "issues_json": json.dumps(
            [
                {
                    "table": issue.table,
                    "column": issue.column,
                    "type": issue.issue_type,
                    "severity": issue.severity,
                    "count": issue.count,
                    "total": issue.total,
                    "percent": issue.percent,
                    "description": issue.description,
                }
                for issue in report.issues_by_severity
            ]
        ),
        "scan_config_json": json.dumps(performance_metrics.get("scan_config", {})),
        "performance_metrics_json": json.dumps(performance_metrics),
    }

    # Professional upsert using MySQL's ON DUPLICATE KEY UPDATE
    with engine.begin() as conn:
        # Insert the record
        result = conn.execute(
            text(
                """
                INSERT INTO data_quality_benchmarks (
                    scan_timestamp, database_name, database_host, scan_type, scan_duration_ms,
                    total_issues, critical_issues, warning_issues, info_issues, all_good,
                    tables_scanned, total_rows_scanned, scan_speed_rows_per_second, memory_usage_mb,
                    deployment_safe, ci_cd_pipeline, git_commit_hash, git_branch,
                    issues_json, scan_config_json, performance_metrics_json
                ) VALUES (
                    :scan_timestamp, :database_name, :database_host, :scan_type, :scan_duration_ms,
                    :total_issues, :critical_issues, :warning_issues, :info_issues, :all_good,
                    :tables_scanned, :total_rows_scanned, :scan_speed_rows_per_second, :memory_usage_mb,
                    :deployment_safe, :ci_cd_pipeline, :git_commit_hash, :git_branch,
                    :issues_json, :scan_config_json, :performance_metrics_json
                )
            """
            ),
            benchmark_data,
        )

        return result.lastrowid


def upsert_issue_patterns(
    engine: Engine, database_name: str, issues: List[QualityIssue]
) -> None:
    """
    Upsert individual issue patterns for trend analysis.

    This tracks specific issues over time to identify patterns and regressions.
    """
    if not issues:
        return

    with engine.begin() as conn:
        for issue in issues:
            # Check if this issue pattern exists
            existing = conn.execute(
                text(
                    """
                    SELECT id, detection_count, max_count_seen, max_percentage_seen, max_severity_seen
                    FROM data_quality_issue_patterns
                    WHERE database_name = :database_name
                    AND table_name = :table_name
                    AND column_name = :column_name
                    AND issue_type = :issue_type
                """
                ),
                {
                    "database_name": database_name,
                    "table_name": issue.table,
                    "column_name": issue.column,
                    "issue_type": issue.issue_type,
                },
            ).first()

            if existing:
                # Update existing pattern
                new_detection_count = existing[1] + 1
                new_max_count = max(existing[2], issue.count)
                new_max_percentage = max(existing[3], issue.percent)

                # Determine max severity
                severity_order = {"info": 1, "warning": 2, "critical": 3}
                current_severity_level = severity_order.get(issue.severity, 1)
                max_severity_level = severity_order.get(existing[4], 1)
                new_max_severity = (
                    issue.severity
                    if current_severity_level > max_severity_level
                    else existing[4]
                )

                conn.execute(
                    text(
                        """
                        UPDATE data_quality_issue_patterns
                        SET last_detected = NOW(),
                            detection_count = :detection_count,
                            current_severity = :current_severity,
                            max_severity_seen = :max_severity_seen,
                            current_count = :current_count,
                            max_count_seen = :max_count_seen,
                            current_percentage = :current_percentage,
                            max_percentage_seen = :max_percentage_seen,
                            resolved = FALSE
                        WHERE id = :id
                    """
                    ),
                    {
                        "id": existing[0],
                        "detection_count": new_detection_count,
                        "current_severity": issue.severity,
                        "max_severity_seen": new_max_severity,
                        "current_count": issue.count,
                        "max_count_seen": new_max_count,
                        "current_percentage": issue.percent,
                        "max_percentage_seen": new_max_percentage,
                    },
                )
            else:
                # Insert new pattern
                conn.execute(
                    text(
                        """
                        INSERT INTO data_quality_issue_patterns (
                            database_name, table_name, column_name, issue_type,
                            current_severity, max_severity_seen,
                            current_count, max_count_seen,
                            current_percentage, max_percentage_seen
                        ) VALUES (
                            :database_name, :table_name, :column_name, :issue_type,
                            :current_severity, :max_severity_seen,
                            :current_count, :max_count_seen,
                            :current_percentage, :max_percentage_seen
                        )
                    """
                    ),
                    {
                        "database_name": database_name,
                        "table_name": issue.table,
                        "column_name": issue.column,
                        "issue_type": issue.issue_type,
                        "current_severity": issue.severity,
                        "max_severity_seen": issue.severity,
                        "current_count": issue.count,
                        "max_count_seen": issue.count,
                        "current_percentage": issue.percent,
                        "max_percentage_seen": issue.percent,
                    },
                )


def update_daily_trends(engine: Engine, database_name: str) -> None:
    """
    Update daily trend aggregates like a pro.

    This creates daily rollups for dashboard and reporting.
    """
    today = date.today()

    with engine.begin() as conn:
        # Calculate today's aggregates
        daily_stats = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) as scans_performed,
                    AVG(critical_issues) as avg_critical,
                    AVG(warning_issues) as avg_warning,
                    AVG(scan_duration_ms) as avg_scan_time,
                    AVG(CASE WHEN deployment_safe THEN 1 ELSE 0 END) * 100 as deployment_success_rate,
                    AVG(CASE
                        WHEN critical_issues = 0 AND warning_issues = 0 THEN 100
                        WHEN critical_issues = 0 THEN 80 - (warning_issues * 2)
                        ELSE 50 - (critical_issues * 10)
                    END) as quality_score
                FROM data_quality_benchmarks
                WHERE database_name = :database_name
                AND DATE(scan_timestamp) = :today
            """
            ),
            {"database_name": database_name, "today": today},
        ).first()

        if daily_stats and daily_stats[0] > 0:  # If we have scans today
            # Upsert daily trend
            conn.execute(
                text(
                    """
                    INSERT INTO data_quality_trends (
                        database_name, date_recorded, scans_performed,
                        avg_critical_issues, avg_warning_issues, avg_scan_time_ms,
                        deployment_success_rate, quality_score
                    ) VALUES (
                        :database_name, :date_recorded, :scans_performed,
                        :avg_critical_issues, :avg_warning_issues, :avg_scan_time_ms,
                        :deployment_success_rate, :quality_score
                    ) ON DUPLICATE KEY UPDATE
                        scans_performed = VALUES(scans_performed),
                        avg_critical_issues = VALUES(avg_critical_issues),
                        avg_warning_issues = VALUES(avg_warning_issues),
                        avg_scan_time_ms = VALUES(avg_scan_time_ms),
                        deployment_success_rate = VALUES(deployment_success_rate),
                        quality_score = VALUES(quality_score)
                """
                ),
                {
                    "database_name": database_name,
                    "date_recorded": today,
                    "scans_performed": daily_stats[0],
                    "avg_critical_issues": daily_stats[1] or 0,
                    "avg_warning_issues": daily_stats[2] or 0,
                    "avg_scan_time_ms": daily_stats[3] or 0,
                    "deployment_success_rate": daily_stats[4] or 0,
                    "quality_score": daily_stats[5] or 0,
                },
            )


def store_ci_cd_results(
    benchmark_db_url: str,
    scan_results: Dict[str, Any],
    ci_cd_context: Optional[Optional[Dict[str, Any]]] = None,
) -> int:
    """
    Store CI/CD scan results in benchmark database like a pro.

    Args:
        benchmark_db_url: URL for the benchmark storage database
        scan_results: Results from the CI/CD scan
        ci_cd_context: Optional CI/CD context (git info, pipeline info)

    Returns:
        ID of the stored benchmark record
    """
    engine = create_engine(benchmark_db_url)

    # Derive a human-friendly database name for reporting.
    # Prefer an explicit field if callers provide one; otherwise fall back to
    # the last path segment of the masked database URL.
    raw_masked_url = scan_results.get("database_url_masked", "")
    explicit_name = scan_results.get("database_name")
    if explicit_name:
        database_name = explicit_name
    elif "/" in raw_masked_url:
        database_name = raw_masked_url.rsplit("/", 1)[-1] or "unknown"
    else:
        database_name = "unknown"

    # Extract performance metrics
    scan_data = scan_results["scan_results"]
    performance_metrics = {
        "scan_speed_rows_per_second": scan_data.get("total_rows_scanned", 0)
        // max(1, scan_data["scan_time_ms"] // 1000),
        "memory_usage_mb": scan_results.get("memory_usage_mb", 0),
        "tables_scanned": len(scan_results.get("issues", [])),
        "total_rows_scanned": sum(
            issue.get("total_rows", 0) for issue in scan_results.get("issues", [])
        ),
        "scan_config": {
            "database_type": scan_results.get("database_url_masked", "").split("/")[0],
            "timestamp": scan_results.get("timestamp"),
        },
    }

    # Create HealthReport object for compatibility
    issues = []
    for issue_data in scan_results.get("issues", []):
        issue = QualityIssue(
            table=issue_data["table"],
            column=issue_data["column"],
            issue_type=issue_data["type"],
            count=issue_data["count"],
            total=issue_data["total_rows"],
            percent=issue_data["percentage"],
            severity=issue_data["severity"],
            description=issue_data["description"],
        )
        issues.append(issue)

    report = HealthReport(
        all_good=scan_data["all_good"],
        total_issues=scan_data["total_issues"],
        issues_by_severity=issues,
        summary={
            "critical": scan_data["critical_issues"],
            "warning": scan_data["warning_issues"],
            "info": scan_data["info_issues"],
        },
        scan_time_ms=scan_data["scan_time_ms"],
    )

    # Store main benchmark record
    benchmark_id = upsert_benchmark_results(
        engine=engine,
        database_name=database_name,
        report=report,
        performance_metrics=performance_metrics,
        ci_cd_context=ci_cd_context,
    )

    # Store individual issue patterns
    upsert_issue_patterns(engine, database_name, issues)

    # Update daily trends
    update_daily_trends(engine, database_name)

    return benchmark_id


def get_performance_trends(
    benchmark_db_url: str, database_name: str, days: int = 30
) -> Dict[str, Any]:
    """
    Get performance trends for dashboard/reporting like a pro.

    Args:
        benchmark_db_url: URL for the benchmark storage database
        database_name: Database name to get trends for
        days: Number of days to look back

    Returns:
        Dictionary with trend data suitable for dashboards
    """
    engine = create_engine(benchmark_db_url)

    with engine.begin() as conn:
        # Get daily trends
        trends = conn.execute(
            text(
                """
                SELECT
                    date_recorded,
                    scans_performed,
                    avg_critical_issues,
                    avg_warning_issues,
                    avg_scan_time_ms,
                    deployment_success_rate,
                    quality_score
                FROM data_quality_trends
                WHERE database_name = :database_name
                AND date_recorded >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY date_recorded DESC
            """
            ),
            {"database_name": database_name, "days": days},
        ).fetchall()

        # Get recent performance metrics
        recent_performance = conn.execute(
            text(
                """
                SELECT
                    AVG(scan_speed_rows_per_second) as avg_speed,
                    AVG(memory_usage_mb) as avg_memory,
                    AVG(scan_duration_ms) as avg_duration,
                    COUNT(*) as total_scans
                FROM data_quality_benchmarks
                WHERE database_name = :database_name
                AND scan_timestamp >= DATE_SUB(NOW(), INTERVAL :days DAY)
            """
            ),
            {"database_name": database_name, "days": days},
        ).first()

        # Get top recurring issues
        recurring_issues = conn.execute(
            text(
                """
                SELECT
                    table_name,
                    column_name,
                    issue_type,
                    current_severity,
                    detection_count,
                    current_percentage,
                    max_percentage_seen
                FROM data_quality_issue_patterns
                WHERE database_name = :database_name
                AND resolved = FALSE
                ORDER BY
                    CASE current_severity
                        WHEN 'critical' THEN 1
                        WHEN 'warning' THEN 2
                        ELSE 3
                    END,
                    detection_count DESC
                LIMIT 10
            """
            ),
            {"database_name": database_name},
        ).fetchall()

        return {
            "database_name": database_name,
            "period_days": days,
            "daily_trends": [
                {
                    "date": str(trend[0]),
                    "scans_performed": trend[1],
                    "avg_critical_issues": float(trend[2]) if trend[2] else 0,
                    "avg_warning_issues": float(trend[3]) if trend[3] else 0,
                    "avg_scan_time_ms": trend[4],
                    "deployment_success_rate": float(trend[5]) if trend[5] else 0,
                    "quality_score": float(trend[6]) if trend[6] else 0,
                }
                for trend in trends
            ],
            "performance_summary": {
                "avg_speed_rows_per_second": int(recent_performance[0])
                if recent_performance[0]
                else 0,
                "avg_memory_usage_mb": float(recent_performance[1])
                if recent_performance[1]
                else 0,
                "avg_duration_ms": int(recent_performance[2])
                if recent_performance[2]
                else 0,
                "total_scans": recent_performance[3] if recent_performance else 0,
            },
            "recurring_issues": [
                {
                    "table": issue[0],
                    "column": issue[1],
                    "type": issue[2],
                    "severity": issue[3],
                    "detection_count": issue[4],
                    "current_percentage": float(issue[5]) if issue[5] else 0,
                    "max_percentage_seen": float(issue[6]) if issue[6] else 0,
                }
                for issue in recurring_issues
            ],
        }


def generate_performance_report(
    benchmark_db_url: str, database_name: str, format: str = "markdown"
) -> str:
    """
    Generate professional performance report for stakeholders.

    Args:
        benchmark_db_url: URL for the benchmark storage database
        database_name: Database name to report on
        format: Output format ("markdown", "html", "json")

    Returns:
        Formatted performance report
    """
    trends = get_performance_trends(benchmark_db_url, database_name, 30)

    if format == "markdown":
        report = f"""# Data Quality Performance Report

## Database: {database_name}

### 📊 30-Day Performance Summary
- **Total Scans**: {trends['performance_summary']['total_scans']:,}
- **Average Speed**: {trends['performance_summary']['avg_speed_rows_per_second']:,} rows/second
- **Average Memory**: {trends['performance_summary']['avg_memory_usage_mb']:.1f} MB
- **Average Duration**: {trends['performance_summary']['avg_duration_ms']:,} ms

### 🎯 Quality Trends
"""

        if trends["daily_trends"]:
            latest = trends["daily_trends"][0]
            report += f"""
- **Latest Quality Score**: {latest['quality_score']:.1f}/100
- **Deployment Success Rate**: {latest['deployment_success_rate']:.1f}%
- **Average Critical Issues**: {latest['avg_critical_issues']:.1f}
- **Average Warning Issues**: {latest['avg_warning_issues']:.1f}
"""

        if trends["recurring_issues"]:
            report += "\n### 🔍 Top Recurring Issues\n"
            for issue in trends["recurring_issues"][:5]:
                severity_emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(
                    issue["severity"], "•"
                )
                report += f"- {severity_emoji} **{issue['table']}.{issue['column']}** ({issue['type']}): Detected {issue['detection_count']} times, currently {issue['current_percentage']:.1f}%\n"

        report += f"\n---\n*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
        return report

    elif format == "json":
        return json.dumps(trends, indent=2)

    else:
        return str(trends)  # Fallback
