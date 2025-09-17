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


def benchmark_scan_speed(database_url: str, target_rows: int = 1000000) -> BenchmarkResult:
    """
    Benchmark scan speed per million rows.
    
    Args:
        database_url: Database connection URL
        target_rows: Target number of rows to process for benchmark
        
    Returns:
        BenchmarkResult with scan speed metrics
        
    Example:
        >>> result = benchmark_scan_speed("sqlite:///:memory:", 1000000)
        >>> print(f"Processed {result.rows_per_second:,} rows/second")
    """
    # Create test database with specified number of rows
    engine = create_engine(database_url)
    
    # Setup test data
    _create_benchmark_data(engine, target_rows)
    
    # Measure scan performance
    start_time = time.perf_counter()
    start_memory = _get_memory_usage()
    
    # Run comprehensive health check (most intensive operation)
    report = health_check(database_url)
    
    end_time = time.perf_counter()
    end_memory = _get_memory_usage()
    
    # Calculate metrics
    time_seconds = end_time - start_time
    memory_mb = max(0, end_memory - start_memory)
    rows_per_second = int(target_rows / time_seconds) if time_seconds > 0 else 0
    
    # Accuracy score based on issues found vs expected
    accuracy_score = _calculate_accuracy_score(report, target_rows)
    
    return BenchmarkResult(
        test_name="scan_speed",
        rows_processed=target_rows,
        time_seconds=time_seconds,
        memory_mb=memory_mb,
        rows_per_second=rows_per_second,
        accuracy_score=accuracy_score,
        metadata={
            "database_type": _get_database_type(database_url),
            "total_issues": report.total_issues,
            "scan_time_ms": report.scan_time_ms
        }
    )


def benchmark_memory_usage(database_url: str, table_count: int = 10) -> BenchmarkResult:
    """
    Benchmark memory usage during large database scans.
    
    Args:
        database_url: Database connection URL
        table_count: Number of tables to create for testing
        
    Returns:
        BenchmarkResult with memory usage metrics
    """
    engine = create_engine(database_url)
    
    # Create multiple tables with varying sizes
    total_rows = _create_multiple_tables(engine, table_count)
    
    # Force garbage collection before measurement
    gc.collect()
    initial_memory = _get_memory_usage()
    peak_memory = initial_memory
    
    # Monitor memory during scan
    def memory_monitor():
        nonlocal peak_memory
        current_memory = _get_memory_usage()
        peak_memory = max(peak_memory, current_memory)
    
    start_time = time.perf_counter()
    
    # Run scan with memory monitoring
    report = health_check(database_url)
    memory_monitor()
    
    end_time = time.perf_counter()
    final_memory = _get_memory_usage()
    
    # Calculate metrics
    time_seconds = end_time - start_time
    memory_used = peak_memory - initial_memory
    rows_per_second = int(total_rows / time_seconds) if time_seconds > 0 else 0
    accuracy_score = _calculate_accuracy_score(report, total_rows)
    
    return BenchmarkResult(
        test_name="memory_usage",
        rows_processed=total_rows,
        time_seconds=time_seconds,
        memory_mb=memory_used,
        rows_per_second=rows_per_second,
        accuracy_score=accuracy_score,
        metadata={
            "table_count": table_count,
            "peak_memory_mb": peak_memory,
            "initial_memory_mb": initial_memory,
            "final_memory_mb": final_memory
        }
    )


def benchmark_accuracy(database_url: str, known_issues: int = 50) -> BenchmarkResult:
    """
    Benchmark accuracy of issue detection against known problems.
    
    Args:
        database_url: Database connection URL
        known_issues: Number of known issues to inject for testing
        
    Returns:
        BenchmarkResult with accuracy metrics
    """
    engine = create_engine(database_url)
    
    # Create test data with known issues
    total_rows = _create_test_data_with_issues(engine, known_issues)
    
    start_time = time.perf_counter()
    start_memory = _get_memory_usage()
    
    # Run comprehensive scan
    report = health_check(database_url)
    
    end_time = time.perf_counter()
    end_memory = _get_memory_usage()
    
    # Calculate accuracy metrics
    detected_issues = report.total_issues
    accuracy_score = min(1.0, detected_issues / known_issues) if known_issues > 0 else 1.0
    
    time_seconds = end_time - start_time
    memory_mb = max(0, end_memory - start_memory)
    rows_per_second = int(total_rows / time_seconds) if time_seconds > 0 else 0
    
    return BenchmarkResult(
        test_name="accuracy",
        rows_processed=total_rows,
        time_seconds=time_seconds,
        memory_mb=memory_mb,
        rows_per_second=rows_per_second,
        accuracy_score=accuracy_score,
        metadata={
            "known_issues": known_issues,
            "detected_issues": detected_issues,
            "precision": accuracy_score,
            "false_negatives": max(0, known_issues - detected_issues)
        }
    )


def run_comprehensive_benchmarks(database_url: str) -> List[BenchmarkResult]:
    """
    Run comprehensive benchmark suite covering all performance aspects.
    
    Args:
        database_url: Database connection URL
        
    Returns:
        List of BenchmarkResult objects for all tests
        
    Example:
        >>> results = run_comprehensive_benchmarks("sqlite:///:memory:")
        >>> for result in results:
        ...     print(f"{result.test_name}: {result.rows_per_second:,} rows/sec")
    """
    results = []
    
    # Benchmark 1: Scan speed with 1M rows
    print("🚀 Running scan speed benchmark...")
    speed_result = benchmark_scan_speed(database_url, 1000000)
    results.append(speed_result)
    
    # Benchmark 2: Memory usage with multiple tables
    print("💾 Running memory usage benchmark...")
    memory_result = benchmark_memory_usage(database_url, 15)
    results.append(memory_result)
    
    # Benchmark 3: Accuracy with known issues
    print("🎯 Running accuracy benchmark...")
    accuracy_result = benchmark_accuracy(database_url, 100)
    results.append(accuracy_result)
    
    return results


def _create_benchmark_data(engine: Engine, row_count: int) -> None:
    """Create test data for benchmarking."""
    with engine.begin() as conn:
        # Drop existing tables
        conn.execute(text("DROP TABLE IF EXISTS benchmark_users"))
        conn.execute(text("DROP TABLE IF EXISTS benchmark_songs"))
        
        # Create tables
        conn.execute(text("""
            CREATE TABLE benchmark_users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                email TEXT,
                isrc TEXT
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE benchmark_songs (
                id INTEGER PRIMARY KEY,
                title TEXT,
                artist_id INTEGER,
                isrc TEXT,
                user_id INTEGER
            )
        """))
        
        # Insert test data in batches
        batch_size = 10000
        for i in range(0, row_count, batch_size):
            batch_end = min(i + batch_size, row_count)
            
            # Insert users
            user_values = []
            for j in range(i, batch_end):
                # Inject some nulls for testing
                user_id = j if j % 10 != 0 else None  # 10% nulls
                email = f"user{j}@test.com" if j % 20 != 0 else None  # 5% nulls
                isrc = f"USRC{j:06d}" if j % 50 != 0 else None  # 2% nulls
                user_values.append(f"({j}, {user_id or 'NULL'}, {repr(email) if email else 'NULL'}, {repr(isrc) if isrc else 'NULL'})")
            
            if user_values:
                conn.execute(text(f"INSERT INTO benchmark_users (id, user_id, email, isrc) VALUES {', '.join(user_values)}"))
            
            # Insert songs
            song_values = []
            for j in range(i, batch_end):
                artist_id = j % 1000 if j % 15 != 0 else None  # Some nulls
                isrc = f"SONG{j:06d}" if j % 30 != 0 else None
                user_id = j % 500 if j % 25 != 0 else None
                song_values.append(f"({j}, 'Song {j}', {artist_id or 'NULL'}, {repr(isrc) if isrc else 'NULL'}, {user_id or 'NULL'})")
            
            if song_values:
                conn.execute(text(f"INSERT INTO benchmark_songs (id, title, artist_id, isrc, user_id) VALUES {', '.join(song_values)}"))


def _create_multiple_tables(engine: Engine, table_count: int) -> int:
    """Create multiple tables for memory testing."""
    total_rows = 0
    
    with engine.begin() as conn:
        for i in range(table_count):
            table_name = f"memory_test_table_{i}"
            row_count = 10000 + (i * 5000)  # Varying sizes
            
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    data_id INTEGER,
                    isrc TEXT,
                    value TEXT
                )
            """))
            
            # Insert data with some nulls
            values = []
            for j in range(row_count):
                data_id = j if j % 8 != 0 else None
                isrc = f"TEST{i}_{j:04d}" if j % 12 != 0 else None
                values.append(f"({j}, {data_id or 'NULL'}, {repr(isrc) if isrc else 'NULL'}, 'data_{j}')")
            
            if values:
                # Insert in smaller batches to avoid memory issues
                batch_size = 1000
                for k in range(0, len(values), batch_size):
                    batch = values[k:k + batch_size]
                    conn.execute(text(f"INSERT INTO {table_name} (id, data_id, isrc, value) VALUES {', '.join(batch)}"))
            
            total_rows += row_count
    
    return total_rows


def _create_test_data_with_issues(engine: Engine, issue_count: int) -> int:
    """Create test data with known issues for accuracy testing."""
    total_rows = issue_count * 10  # 10x more rows than issues
    
    with engine.begin() as conn:
        # Drop existing tables
        conn.execute(text("DROP TABLE IF EXISTS accuracy_test_users"))
        conn.execute(text("DROP TABLE IF EXISTS accuracy_test_orders"))
        
        # Create tables
        conn.execute(text("""
            CREATE TABLE accuracy_test_users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                email TEXT,
                isrc TEXT
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE accuracy_test_orders (
                id INTEGER PRIMARY KEY,
                order_id INTEGER,
                user_id INTEGER,
                isrc TEXT
            )
        """))
        
        # Insert data with exactly known number of issues
        null_issues = issue_count // 2
        duplicate_issues = issue_count - null_issues
        
        # Insert users with null issues
        user_values = []
        for i in range(total_rows):
            user_id = i if i >= null_issues else None  # First N have nulls
            email = f"user{i}@test.com"
            isrc = f"USER{i:06d}"
            user_values.append(f"({i}, {user_id or 'NULL'}, {repr(email)}, {repr(isrc)})")
        
        conn.execute(text(f"INSERT INTO accuracy_test_users (id, user_id, email, isrc) VALUES {', '.join(user_values)}"))
        
        # Insert orders with duplicate issues
        order_values = []
        for i in range(total_rows):
            order_id = i
            user_id = i % 100  # Create some valid references
            # Create duplicates for first N ISRCs
            isrc = f"ORDER{min(i, duplicate_issues):06d}"
            order_values.append(f"({i}, {order_id}, {user_id}, {repr(isrc)})")
        
        conn.execute(text(f"INSERT INTO accuracy_test_orders (id, order_id, user_id, isrc) VALUES {', '.join(order_values)}"))
    
    return total_rows


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


def _calculate_accuracy_score(report, total_rows: int) -> float:
    """Calculate accuracy score based on scan results."""
    # Simple accuracy calculation - can be enhanced
    if total_rows == 0:
        return 1.0
    
    # Score based on reasonable issue detection rate
    expected_issue_rate = 0.1  # Expect ~10% of rows to have issues
    expected_issues = int(total_rows * expected_issue_rate)
    
    if expected_issues == 0:
        return 1.0
    
    # Calculate accuracy as how close we are to expected
    actual_issues = report.total_issues
    accuracy = 1.0 - abs(actual_issues - expected_issues) / expected_issues
    return max(0.0, min(1.0, accuracy))