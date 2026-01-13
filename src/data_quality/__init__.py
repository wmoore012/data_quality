# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

from .advanced_analysis import (
    ColumnAnalysis,
    DatabaseAnalysis,
    TableAnalysis,
    analyze_database_completeness,
    identify_impossible_columns,
)
from .benchmarks import (
    BenchmarkResult,
    benchmark_accuracy,
    benchmark_memory_usage,
    benchmark_scan_speed,
    run_comprehensive_benchmarks,
)
from .checkpoints import (
    BronzeToSilverCheckpoint,
    CheckpointResult,
    MedallionCheckpoint,
    SilverToGoldCheckpoint,
    run_medallion_checkpoints,
)
from .null_scan import quick_null_scan
from .quality_scanner import (
    HealthReport,
    QualityIssue,
    health_check,
    scan_nulls,
    scan_orphans,
)
from .schema_analyzer import (
    SchemaAnalysis,
    SchemaRecommendation,
    analyze_schema,
    suggest_improvements,
)

# AI integration (optional import)
try:
    from .ai_integration import (
        AIAnalysis,
        AIDataQualityAnalyzer,
        analyze_database_with_ai,
        format_for_github_comment,
        format_for_slack_message,
    )

    _AI_AVAILABLE = True
except ImportError:
    _AI_AVAILABLE = False

__all__ = [
    "quick_null_scan",
    "scan_nulls",
    "scan_orphans",
    "health_check",
    "QualityIssue",
    "HealthReport",
    "analyze_schema",
    "suggest_improvements",
    "SchemaAnalysis",
    "SchemaRecommendation",
    "benchmark_scan_speed",
    "benchmark_memory_usage",
    "benchmark_accuracy",
    "run_comprehensive_benchmarks",
    "BenchmarkResult",
    "analyze_database_completeness",
    "identify_impossible_columns",
    "ColumnAnalysis",
    "TableAnalysis",
    "DatabaseAnalysis",
    "BronzeToSilverCheckpoint",
    "SilverToGoldCheckpoint",
    "MedallionCheckpoint",
    "CheckpointResult",
    "run_medallion_checkpoints",
]

# Add AI functions to __all__ if available
if _AI_AVAILABLE:
    __all__.extend(
        [
            "analyze_database_with_ai",
            "AIDataQualityAnalyzer",
            "AIAnalysis",
            "format_for_github_comment",
            "format_for_slack_message",
        ]
    )
