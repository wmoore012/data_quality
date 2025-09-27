# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Perday CatalogLAB™

"""
Tests for data quality benchmarking functionality.

These tests verify performance measurement capabilities including
scan speed, memory usage, and accuracy metrics.
"""

import time
from unittest.mock import Mock, patch

import pytest

from data_quality.benchmarks import (
    BenchmarkResult,
    benchmark_accuracy,
    benchmark_memory_usage,
    benchmark_scan_speed,
    run_comprehensive_benchmarks,
)


class TestBenchmarkResult:
    """Test BenchmarkResult dataclass."""

    def test_benchmark_result_creation(self):
        """Test creating a BenchmarkResult."""
        result = BenchmarkResult(
            test_name="scan_speed",
            rows_processed=1000000,
            time_seconds=2.5,
            memory_mb=150.0,
            rows_per_second=400000,
            accuracy_score=0.95,
            metadata={"database": "test", "table_count": 5}
        )

        assert result.test_name == "scan_speed"
        assert result.rows_processed == 1000000
        assert result.time_seconds == 2.5
        assert result.memory_mb == 150.0
        assert result.rows_per_second == 400000
        assert result.accuracy_score == 0.95
        assert result.metadata["database"] == "test"


class TestScanSpeedBenchmark:
    """Test scan speed benchmarking."""

    def test_benchmark_scan_speed_small_dataset(self):
        """Test scan speed benchmark with small dataset."""
        result = benchmark_scan_speed("sqlite+pysqlite:///:memory:", 1000)

        assert result.test_name == "scan_speed"
        assert result.rows_processed == 1000
        assert result.time_seconds > 0
        assert result.rows_per_second > 0
        assert 0 <= result.accuracy_score <= 1.0
        assert "database_type" in result.metadata

    def test_benchmark_scan_speed_large_dataset(self):
        """Test scan speed benchmark with large dataset."""
        result = benchmark_scan_speed("sqlite+pysqlite:///:memory:", 10000)

        assert result.test_name == "scan_speed"
        assert result.rows_processed == 10000
        assert result.time_seconds > 0
        assert result.rows_per_second > 0

    def test_benchmark_scan_speed_calculates_rows_per_second(self):
        """Test that scan speed benchmark calculates rows per second correctly."""
        result = benchmark_scan_speed("sqlite+pysqlite:///:memory:", 5000)

        # Verify calculation: rows_per_second = rows_processed / time_seconds
        expected_rps = int(result.rows_processed / result.time_seconds)
        assert abs(result.rows_per_second - expected_rps) <= 1  # Allow for rounding


class TestMemoryUsageBenchmark:
    """Test memory usage benchmarking."""

    def test_benchmark_memory_usage_tracks_peak(self):
        """Test that memory benchmark tracks peak usage."""
        result = benchmark_memory_usage("sqlite+pysqlite:///:memory:", 3)

        assert result.test_name == "memory_usage"
        assert result.memory_mb >= 0
        assert "peak_memory_mb" in result.metadata
        assert result.metadata["peak_memory_mb"] >= result.metadata["initial_memory_mb"]

    def test_benchmark_memory_usage_large_scan(self):
        """Test memory usage during large database scan."""
        result = benchmark_memory_usage("sqlite+pysqlite:///:memory:", 5)

        assert result.test_name == "memory_usage"
        assert result.memory_mb >= 0
        assert result.metadata["table_count"] == 5


class TestAccuracyBenchmark:
    """Test accuracy benchmarking."""

    def test_benchmark_accuracy_known_dataset(self):
        """Test accuracy benchmark with known issues."""
        result = benchmark_accuracy("sqlite+pysqlite:///:memory:", 20)

        assert result.test_name == "accuracy"
        assert 0 <= result.accuracy_score <= 1.0
        assert result.metadata["known_issues"] == 20
        assert "detected_issues" in result.metadata

    def test_benchmark_accuracy_perfect_dataset(self):
        """Test accuracy benchmark with perfect dataset."""
        result = benchmark_accuracy("sqlite+pysqlite:///:memory:", 0)  # No known issues

        assert result.test_name == "accuracy"
        assert result.accuracy_score >= 0.0
        assert result.metadata["known_issues"] == 0


class TestComprehensiveBenchmarks:
    """Test comprehensive benchmark suite."""

    def test_run_comprehensive_benchmarks(self):
        """Test running all benchmarks together."""
        results = run_comprehensive_benchmarks("sqlite+pysqlite:///:memory:")

        assert len(results) == 3  # scan_speed, memory_usage, accuracy
        test_names = {r.test_name for r in results}
        assert test_names == {"scan_speed", "memory_usage", "accuracy"}

    def test_comprehensive_benchmarks_returns_results(self):
        """Test that comprehensive benchmarks return structured results."""
        results = run_comprehensive_benchmarks("sqlite+pysqlite:///:memory:")

        assert len(results) == 3
        for result in results:
            assert isinstance(result, BenchmarkResult)
            assert result.rows_processed > 0
            assert result.time_seconds > 0


class TestBenchmarkRequirements:
    """Test specific benchmark requirements from the spec."""

    def test_scan_speed_per_million_rows(self):
        """Test measuring scan speed per million rows."""
        # Requirement: measuring scan speed per million rows
        from data_quality.benchmarks import benchmark_scan_speed

        # Test with small dataset for speed
        result = benchmark_scan_speed("sqlite:///:memory:", 1000)

        assert result.test_name == "scan_speed"
        assert result.rows_processed == 1000
        assert result.time_seconds > 0
        assert result.rows_per_second > 0
        assert 0 <= result.accuracy_score <= 1.0

    def test_memory_usage_during_large_scans(self):
        """Test measuring memory usage during large scans."""
        # Requirement: memory usage during large scans
        from data_quality.benchmarks import benchmark_memory_usage

        # Test with small dataset for speed
        result = benchmark_memory_usage("sqlite:///:memory:", 3)

        assert result.test_name == "memory_usage"
        assert result.rows_processed > 0
        assert result.time_seconds > 0
        assert result.memory_mb >= 0  # Memory usage can be 0 in test environments

    def test_accuracy_metrics(self):
        """Test measuring accuracy metrics."""
        # Requirement: accuracy metrics
        from data_quality.benchmarks import benchmark_accuracy

        # Test with small dataset for speed
        result = benchmark_accuracy("sqlite:///:memory:", 10)

        assert result.test_name == "accuracy"
        assert result.rows_processed > 0
        assert result.time_seconds > 0
        assert 0 <= result.accuracy_score <= 1.0
        assert "known_issues" in result.metadata
        assert "detected_issues" in result.metadata


class TestBenchmarkIntegration:
    """Integration tests for benchmarking with real scenarios."""

    def test_benchmark_with_sqlite_database(self):
        """Test benchmarking with SQLite database."""
        # Mock the comprehensive benchmarks to run faster
        import unittest.mock

        from data_quality.benchmarks import run_comprehensive_benchmarks
        with unittest.mock.patch('data_quality.benchmarks.benchmark_scan_speed') as mock_speed, \
             unittest.mock.patch('data_quality.benchmarks.benchmark_memory_usage') as mock_memory, \
             unittest.mock.patch('data_quality.benchmarks.benchmark_accuracy') as mock_accuracy:

            # Mock return values
            from data_quality.benchmarks import BenchmarkResult
            mock_speed.return_value = BenchmarkResult("scan_speed", 1000, 0.1, 5.0, 10000, 0.9, {})
            mock_memory.return_value = BenchmarkResult("memory_usage", 1000, 0.1, 10.0, 10000, 0.8, {})
            mock_accuracy.return_value = BenchmarkResult("accuracy", 1000, 0.1, 2.0, 10000, 0.95, {})

            results = run_comprehensive_benchmarks("sqlite:///:memory:")

            assert len(results) == 3
            assert all(isinstance(r, BenchmarkResult) for r in results)

    def test_benchmark_performance_regression(self):
        """Test that benchmarks can detect performance regressions."""
        from data_quality.benchmarks import benchmark_scan_speed

        # Run two benchmarks and compare (simplified test)
        result1 = benchmark_scan_speed("sqlite:///:memory:", 100)
        result2 = benchmark_scan_speed("sqlite:///:memory:", 100)

        # Both should complete successfully
        assert result1.rows_per_second > 0
        assert result2.rows_per_second > 0

        # Performance should be reasonably consistent (within 10x factor)
        ratio = max(result1.rows_per_second, result2.rows_per_second) / min(result1.rows_per_second, result2.rows_per_second)
        assert ratio < 10.0  # Performance shouldn't vary by more than 10x
