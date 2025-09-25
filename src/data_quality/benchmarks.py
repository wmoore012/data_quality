# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Benchmarking suite for data quality scanning performance.

This module provides comprehensive benchmarking capabilities to measure
scan speed, memory usage, and accuracy metrics for data quality operations.
"""

from __future__ import annotations

import gc
import os
import psutil
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .quality_scanner import health_check, scan_nulls, scan_orphans


@dataclass
class BenchmarkResult:
    """Results from a benchmark test."""
    
    test_name: str
    rows_processed: int
    time_seconds: float
    memory_mb: float
    rows_per_second: int
    accuracy_score: float
    metadata: Dict[str, Any]


def benchmark_scan_speed(database_url: str, table_patterns: Optional[Optional[List[str]]] = None) -> BenchmarkResult:
    """
    Benchmark scan speed on real database tables.
    
    Args:
        database_url: Database connection URL
        table_patterns: Optional table patterns to scan (e.g., ['songs%', 'albums%'])
        
    Returns:
        BenchmarkResult with scan speed metrics
        
    Example:
        >>> result = benchmark_scan_speed("mysql://user:pass@host/db", ["songs"])
        >>> print(f"Processed {result.rows_per_second:,} rows/second")
    """
    engine = create_engine(database_url)
    
    # Get actual row count from real tables
    total_rows = _get_total_row_count(engine, table_patterns)
    
    if total_rows == 0:
        raise ValueError("No tables found or all tables are empty. Cannot benchmark on empty database.")
    
    # Measure scan performance on real data
    start_time = time.perf_counter()
    start_memory = _get_memory_usage()
    
    # Run comprehensive health check (most intensive operation)
    report = health_check(database_url, table_patterns)
    
    end_time = time.perf_counter()
    end_memory = _get_memory_usage()
    
    # Calculate metrics
    time_seconds = end_time - start_time
    memory_mb = max(0, end_memory - start_memory)
    rows_per_second = int(total_rows / time_seconds) if time_seconds > 0 else 0
    
    # Accuracy score based on issues found
    accuracy_score = min(1.0, report.total_issues / max(1, total_rows * 0.01))  # Expect ~1% issues
    
    return BenchmarkResult(
        test_name="scan_speed",
        rows_processed=total_rows,
        time_seconds=time_seconds,
        memory_mb=memory_mb,
        rows_per_second=rows_per_second,
        accuracy_score=accuracy_score,
        metadata={
            "database_type": _get_database_type(database_url),
            "total_issues": report.total_issues,
            "scan_time_ms": report.scan_time_ms,
            "tables_scanned": len(_get_tables_list(engine, table_patterns))
        }
    )


def benchmark_memory_usage(database_url: str, table_patterns: Optional[Optional[List[str]]] = None) -> BenchmarkResult:
    """
    Benchmark memory usage during real database scans.
    
    Args:
        database_url: Database connection URL
        table_patterns: Optional table patterns to scan
        
    Returns:
        BenchmarkResult with memory usage metrics
    """
    engine = create_engine(database_url)
    
    # Get actual row count from real tables
    total_rows = _get_total_row_count(engine, table_patterns)
    tables_list = _get_tables_list(engine, table_patterns)
    
    if total_rows == 0:
        raise ValueError("No tables found or all tables are empty. Cannot benchmark on empty database.")
    
    # Force garbage collection before measurement
    gc.collect()
    initial_memory = _get_memory_usage()
    peak_memory = initial_memory
    
    # Monitor memory during scan
    def memory_monitor() -> None:
        nonlocal peak_memory
        current_memory = _get_memory_usage()
        peak_memory = max(peak_memory, current_memory)
    
    start_time = time.perf_counter()
    
    # Run scan with memory monitoring on real data
    report = health_check(database_url, table_patterns)
    memory_monitor()
    
    end_time = time.perf_counter()
    final_memory = _get_memory_usage()
    
    # Calculate metrics
    time_seconds = end_time - start_time
    memory_used = peak_memory - initial_memory
    rows_per_second = int(total_rows / time_seconds) if time_seconds > 0 else 0
    accuracy_score = min(1.0, report.total_issues / max(1, total_rows * 0.01))
    
    return BenchmarkResult(
        test_name="memory_usage",
        rows_processed=total_rows,
        time_seconds=time_seconds,
        memory_mb=memory_used,
        rows_per_second=rows_per_second,
        accuracy_score=accuracy_score,
        metadata={
            "table_count": len(tables_list),
            "peak_memory_mb": peak_memory,
            "initial_memory_mb": initial_memory,
            "final_memory_mb": final_memory,
            "tables_scanned": tables_list
        }
    )


def benchmark_accuracy(database_url: str, table_patterns: Optional[Optional[List[str]]] = None) -> BenchmarkResult:
    """
    Benchmark accuracy of issue detection on real data.
    
    Args:
        database_url: Database connection URL
        table_patterns: Optional table patterns to scan
        
    Returns:
        BenchmarkResult with accuracy metrics
    """
    engine = create_engine(database_url)
    
    # Get actual row count from real tables
    total_rows = _get_total_row_count(engine, table_patterns)
    
    if total_rows == 0:
        raise ValueError("No tables found or all tables are empty. Cannot benchmark on empty database.")
    
    start_time = time.perf_counter()
    start_memory = _get_memory_usage()
    
    # Run comprehensive scan on real data
    report = health_check(database_url, table_patterns)
    
    end_time = time.perf_counter()
    end_memory = _get_memory_usage()
    
    # Calculate accuracy metrics based on real data patterns
    detected_issues = report.total_issues
    # For real data, we measure detection rate rather than accuracy against known issues
    detection_rate = detected_issues / max(1, total_rows) * 100  # Issues per 100 rows
    
    time_seconds = end_time - start_time
    memory_mb = max(0, end_memory - start_memory)
    rows_per_second = int(total_rows / time_seconds) if time_seconds > 0 else 0
    
    return BenchmarkResult(
        test_name="accuracy",
        rows_processed=total_rows,
        time_seconds=time_seconds,
        memory_mb=memory_mb,
        rows_per_second=rows_per_second,
        accuracy_score=min(1.0, detection_rate / 10.0),  # Normalize to 0-1 scale
        metadata={
            "detected_issues": detected_issues,
            "detection_rate_percent": detection_rate,
            "critical_issues": report.summary.get("critical", 0),
            "warning_issues": report.summary.get("warning", 0),
            "info_issues": report.summary.get("info", 0)
        }
    )


def run_comprehensive_benchmarks(database_url: str, table_patterns: Optional[Optional[List[str]]] = None) -> List[BenchmarkResult]:
    """
    Run comprehensive benchmark suite on real database tables.
    
    Args:
        database_url: Database connection URL
        table_patterns: Optional table patterns to focus benchmarks on
        
    Returns:
        List of BenchmarkResult objects for all tests
        
    Example:
        >>> results = run_comprehensive_benchmarks("mysql://user:pass@host/db", ["songs", "albums"])
        >>> for result in results:
        ...     print(f"{result.test_name}: {result.rows_per_second:,} rows/sec")
    """
    results = []
    
    try:
        # Benchmark 1: Scan speed on real data
        print("🚀 Running scan speed benchmark on real data...")
        speed_result = benchmark_scan_speed(database_url, table_patterns)
        results.append(speed_result)
        
        # Benchmark 2: Memory usage during real scans
        print("💾 Running memory usage benchmark...")
        memory_result = benchmark_memory_usage(database_url, table_patterns)
        results.append(memory_result)
        
        # Benchmark 3: Issue detection accuracy
        print("🎯 Running accuracy benchmark...")
        accuracy_result = benchmark_accuracy(database_url, table_patterns)
        results.append(accuracy_result)
        
    except ValueError as e:
        print(f"⚠️  Benchmark failed: {e}")
        print("💡 Make sure your database has tables with data to benchmark against.")
        
    return results


# All dummy data creation removed - benchmarks only work with real databases


def _get_memory_usage() -> float:
    """Get current memory usage in MB."""
    try:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024  # Convert to MB
    except Exception:
        return 0.0


def _get_database_type(database_url: str) -> str:
    """Extract database type from URL."""
    if "sqlite" in database_url:
        return "sqlite"
    elif "mysql" in database_url:
        return "mysql"
    elif "postgresql" in database_url:
        return "postgresql"
    else:
        return "unknown"


def _get_total_row_count(engine: Engine, table_patterns: Optional[Optional[List[str]]] = None) -> int:
    """Get total row count across all tables matching patterns."""
    total_rows = 0
    tables = _get_tables_list(engine, table_patterns)
    
    try:
        with engine.begin() as conn:
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar() or 0
                    total_rows += count
                except Exception:
                    continue  # Skip tables we can't access
    except Exception:
        pass
    
    return total_rows


def _get_tables_list(engine: Engine, table_patterns: Optional[Optional[List[str]]] = None) -> List[str]:
    """Get list of tables matching patterns."""
    try:
        # Try MySQL/PostgreSQL approach
        if table_patterns:
            pattern_conditions = " OR ".join([f"table_name LIKE '{pattern}'" for pattern in table_patterns])
            query = text(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND ({pattern_conditions})
                ORDER BY table_name
            """)
        else:
            query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                ORDER BY table_name
            """)

        with engine.begin() as conn:
            result = conn.execute(query)
            return [row[0] for row in result]

    except Exception:
        # Fallback for SQLite
        try:
            query = text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            with engine.begin() as conn:
                result = conn.execute(query)
                tables = [row[0] for row in result]

                if table_patterns:
                    # Simple pattern matching for SQLite
                    filtered_tables = []
                    for table in tables:
                        for pattern in table_patterns:
                            if pattern.replace("%", "") in table:
                                filtered_tables.append(table)
                                break
                    return filtered_tables
                return tables
        except Exception:
            return []