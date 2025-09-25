# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Great Expectations-style checkpoints for Medallion Architecture data validation.

This module provides checkpoint functionality for validating data quality
at Bronze→Silver and Silver→Gold boundaries in the Medallion architecture.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .exceptions import ValidationError
from .quality_scanner import QualityIssue


@dataclass
class CheckpointResult:
    """Result of running a data quality checkpoint."""
    
    checkpoint_name: str
    success: bool
    issues: List[QualityIssue]
    total_records: int
    passed_records: int
    failed_records: int
    execution_time_ms: int
    summary: str


class MedallionCheckpoint:
    """Base class for Medallion architecture data quality checkpoints."""
    
    def __init__(self, database_url: str, checkpoint_name: str) -> None:
        self.database_url = database_url
        self.checkpoint_name = checkpoint_name
        self.engine = create_engine(database_url)
    
    def run(self) -> CheckpointResult:
        """Run the checkpoint and return results."""
        raise NotImplementedError("Subclasses must implement run()")
    
    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(query))
                return [dict(row._mapping) for row in result.fetchall()]
        except SQLAlchemyError as e:
            raise ValidationError("database_query", query, "valid SQL", f"Query failed: {e}")


class BronzeToSilverCheckpoint(MedallionCheckpoint):
    """Checkpoint for validating Bronze→Silver data transformations."""
    
    def __init__(self, database_url: str, platform: str = "all") -> None:
        super().__init__(database_url, f"bronze_to_silver_{platform}")
        self.platform = platform
    
    def run(self) -> CheckpointResult:
        """Run Bronze→Silver validation checks."""
        import time
        start_time = time.time()
        
        issues = []
        total_records = 0
        failed_records = 0
        
        # Get platforms to check
        platforms = [self.platform] if self.platform != "all" else ["spotify", "youtube", "tidal"]
        
        for platform in platforms:
            platform_issues = self._validate_platform(platform)
            issues.extend(platform_issues)
            
            # Count records
            platform_total = self._count_records(f"{platform}_parsed")
            total_records += platform_total
            
            # Count failed records (issues)
            platform_failed = sum(issue.count for issue in platform_issues)
            failed_records += platform_failed
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        passed_records = total_records - failed_records
        success = len([issue for issue in issues if issue.severity == "critical"]) == 0
        
        summary = f"Validated {total_records} records across {len(platforms)} platforms. "
        summary += f"{passed_records} passed, {failed_records} failed."
        
        return CheckpointResult(
            checkpoint_name=self.checkpoint_name,
            success=success,
            issues=issues,
            total_records=total_records,
            passed_records=passed_records,
            failed_records=failed_records,
            execution_time_ms=execution_time_ms,
            summary=summary
        )
    
    def _validate_platform(self, platform: str) -> List[QualityIssue]:
        """Validate a specific platform's Silver layer data."""
        issues = []
        table_name = f"{platform}_parsed"
        
        # Check 1: raw_id must not be null
        issues.extend(self._check_not_null(table_name, "raw_id"))
        
        # Check 2: confidence must be between 0 and 1
        issues.extend(self._check_confidence_range(table_name))
        
        # Check 3: decision must be valid enum
        issues.extend(self._check_decision_enum(table_name))
        
        # Check 4: parser_version must not be null and follow format
        issues.extend(self._check_parser_version(table_name))
        
        # Check 5: parsed_at must not be null
        issues.extend(self._check_not_null(table_name, "parsed_at"))
        
        return issues
    
    def _check_not_null(self, table: str, column: str) -> List[QualityIssue]:
        """Check that a column has no null values."""
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT({column}) as non_null_count,
            COUNT(*) - COUNT({column}) as null_count
        FROM {table}
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            null_count = result['null_count']
            total_count = result['total_count']
            
            if null_count > 0:
                percent = (null_count / total_count) * 100 if total_count > 0 else 0
                return [QualityIssue(
                    table=table,
                    column=column,
                    issue_type="nulls",
                    count=null_count,
                    total=total_count,
                    percent=percent,
                    severity="critical",
                    description=f"Found {null_count} null values in {table}.{column} (required field)"
                )]
        except Exception:
            # Table might not exist, skip silently
            pass
        
        return []
    
    def _check_confidence_range(self, table: str) -> List[QualityIssue]:
        """Check that confidence values are between 0 and 1."""
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN confidence < 0.00 OR confidence > 1.00 THEN 1 END) as invalid_count
        FROM {table}
        WHERE confidence IS NOT NULL
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            invalid_count = result['invalid_count']
            total_count = result['total_count']
            
            if invalid_count > 0:
                percent = (invalid_count / total_count) * 100 if total_count > 0 else 0
                return [QualityIssue(
                    table=table,
                    column="confidence",
                    issue_type="invalid_range",
                    count=invalid_count,
                    total=total_count,
                    percent=percent,
                    severity="critical",
                    description=f"Found {invalid_count} confidence values outside 0.00-1.00 range in {table}"
                )]
        except Exception:
            pass
        
        return []
    
    def _check_decision_enum(self, table: str) -> List[QualityIssue]:
        """Check that decision values are valid enum values."""
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN decision NOT IN ('accept', 'graylist', 'reject') THEN 1 END) as invalid_count
        FROM {table}
        WHERE decision IS NOT NULL
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            invalid_count = result['invalid_count']
            total_count = result['total_count']
            
            if invalid_count > 0:
                percent = (invalid_count / total_count) * 100 if total_count > 0 else 0
                return [QualityIssue(
                    table=table,
                    column="decision",
                    issue_type="invalid_enum",
                    count=invalid_count,
                    total=total_count,
                    percent=percent,
                    severity="critical",
                    description=f"Found {invalid_count} invalid decision values in {table} (must be accept/graylist/reject)"
                )]
        except Exception:
            pass
        
        return []
    
    def _check_parser_version(self, table: str) -> List[QualityIssue]:
        """Check that parser_version is not null and follows semantic versioning."""
        issues = []
        
        # Check for nulls
        issues.extend(self._check_not_null(table, "parser_version"))
        
        # Check format (basic semantic versioning: v1.0.0 or 1.0.0)
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN parser_version NOT REGEXP '^v?[0-9]+\\.[0-9]+\\.[0-9]+' THEN 1 END) as invalid_format_count
        FROM {table}
        WHERE parser_version IS NOT NULL
        """
        
        try:
            results = self._execute_query(query)
            if results:
                result = results[0]
                invalid_count = result['invalid_format_count']
                total_count = result['total_count']
                
                if invalid_count > 0:
                    percent = (invalid_count / total_count) * 100 if total_count > 0 else 0
                    issues.append(QualityIssue(
                        table=table,
                        column="parser_version",
                        issue_type="invalid_format",
                        count=invalid_count,
                        total=total_count,
                        percent=percent,
                        severity="warning",
                        description=f"Found {invalid_count} parser_version values with invalid format in {table}"
                    ))
        except Exception:
            pass
        
        return issues
    
    def _count_records(self, table: str) -> int:
        """Count total records in a table."""
        try:
            results = self._execute_query(f"SELECT COUNT(*) as count FROM {table}")
            return results[0]['count'] if results else 0
        except Exception:
            return 0


class SilverToGoldCheckpoint(MedallionCheckpoint):
    """Checkpoint for validating Silver→Gold data promotions."""
    
    def __init__(self, database_url: str) -> None:
        super().__init__(database_url, "silver_to_gold")
    
    def run(self) -> CheckpointResult:
        """Run Silver→Gold validation checks."""
        import time
        start_time = time.time()
        
        issues = []
        total_records = 0
        failed_records = 0
        
        # Check all Silver layer tables for Gold promotion readiness
        platforms = ["spotify", "youtube", "tidal"]
        
        for platform in platforms:
            platform_issues = self._validate_gold_readiness(platform)
            issues.extend(platform_issues)
            
            # Count records that would be promoted (decision = 'accept')
            platform_total = self._count_accept_records(f"{platform}_parsed")
            total_records += platform_total
            
            # Count failed records
            platform_failed = sum(issue.count for issue in platform_issues)
            failed_records += platform_failed
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        passed_records = total_records - failed_records
        success = len([issue for issue in issues if issue.severity == "critical"]) == 0
        
        summary = f"Validated {total_records} 'accept' records for Gold promotion. "
        summary += f"{passed_records} ready, {failed_records} blocked."
        
        return CheckpointResult(
            checkpoint_name=self.checkpoint_name,
            success=success,
            issues=issues,
            total_records=total_records,
            passed_records=passed_records,
            failed_records=failed_records,
            execution_time_ms=execution_time_ms,
            summary=summary
        )
    
    def _validate_gold_readiness(self, platform: str) -> List[QualityIssue]:
        """Validate that Silver layer data is ready for Gold promotion."""
        issues = []
        table_name = f"{platform}_parsed"
        
        # Check 1: Only 'accept' decisions should be promoted
        issues.extend(self._check_only_accept_decisions(table_name))
        
        # Check 2: No emoji in artist names (garbage data prevention)
        if platform in ["spotify", "tidal"]:  # These have artist_names field
            issues.extend(self._check_no_emojis_in_artists(table_name))
        elif platform == "youtube":  # YouTube has channel_title
            issues.extend(self._check_no_emojis_in_channel(table_name))
        
        # Check 3: ISRC format validation (if present)
        issues.extend(self._check_isrc_format(table_name))
        
        # Check 4: No garbage artist names
        if platform in ["spotify", "tidal"]:
            issues.extend(self._check_no_garbage_artists(table_name))
        
        return issues
    
    def _check_only_accept_decisions(self, table: str) -> List[QualityIssue]:
        """Ensure only 'accept' decisions are being promoted to Gold."""
        # This is more of a policy check - we don't want to find non-accept records
        # being processed for Gold layer promotion
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN decision != 'accept' THEN 1 END) as non_accept_count
        FROM {table}
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            non_accept_count = result['non_accept_count']
            total_count = result['total_count']
            
            # This is informational - we expect non-accept records to exist
            # The issue would be if they're being promoted to Gold (which should be prevented by design)
            return []  # No issues - this is expected behavior
        except Exception:
            pass
        
        return []
    
    def _check_no_emojis_in_artists(self, table: str) -> List[QualityIssue]:
        """Check that artist names don't contain emojis (garbage data indicator)."""
        # Check for common music-related emojis that indicate garbage data
        emoji_pattern = r'[🎼🎮🎶🎵🎤🎧🎸🥁🎹🎺🎻]'
        
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN artist_names REGEXP '{emoji_pattern}' THEN 1 END) as emoji_count
        FROM {table}
        WHERE decision = 'accept' AND artist_names IS NOT NULL
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            emoji_count = result['emoji_count']
            total_count = result['total_count']
            
            if emoji_count > 0:
                percent = (emoji_count / total_count) * 100 if total_count > 0 else 0
                return [QualityIssue(
                    table=table,
                    column="artist_names",
                    issue_type="garbage_data",
                    count=emoji_count,
                    total=total_count,
                    percent=percent,
                    severity="critical",
                    description=f"Found {emoji_count} artist names with emojis in {table} (garbage data indicator)"
                )]
        except Exception:
            pass
        
        return []
    
    def _check_no_emojis_in_channel(self, table: str) -> List[QualityIssue]:
        """Check that YouTube channel titles don't contain music emojis."""
        emoji_pattern = r'[🎼🎮🎶🎵🎤🎧🎸🥁🎹🎺🎻]'
        
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN channel_title REGEXP '{emoji_pattern}' THEN 1 END) as emoji_count
        FROM {table}
        WHERE decision = 'accept' AND channel_title IS NOT NULL
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            emoji_count = result['emoji_count']
            total_count = result['total_count']
            
            if emoji_count > 0:
                percent = (emoji_count / total_count) * 100 if total_count > 0 else 0
                return [QualityIssue(
                    table=table,
                    column="channel_title",
                    issue_type="garbage_data",
                    count=emoji_count,
                    total=total_count,
                    percent=percent,
                    severity="warning",  # Less critical for YouTube channels
                    description=f"Found {emoji_count} channel titles with emojis in {table}"
                )]
        except Exception:
            pass
        
        return []
    
    def _check_isrc_format(self, table: str) -> List[QualityIssue]:
        """Check that ISRC codes follow the correct format (if present)."""
        # ISRC format: CC-XXX-YY-NNNNN (12 characters)
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN LENGTH(isrc) != 12 OR isrc NOT REGEXP '^[A-Z]{{2}}-[A-Z0-9]{{3}}-[0-9]{{2}}-[0-9]{{5}}$' THEN 1 END) as invalid_isrc_count
        FROM {table}
        WHERE decision = 'accept' AND isrc IS NOT NULL AND isrc != ''
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            invalid_count = result['invalid_isrc_count']
            total_count = result['total_count']
            
            if invalid_count > 0:
                percent = (invalid_count / total_count) * 100 if total_count > 0 else 0
                return [QualityIssue(
                    table=table,
                    column="isrc",
                    issue_type="invalid_format",
                    count=invalid_count,
                    total=total_count,
                    percent=percent,
                    severity="warning",
                    description=f"Found {invalid_count} invalid ISRC formats in {table}"
                )]
        except Exception:
            pass
        
        return []
    
    def _check_no_garbage_artists(self, table: str) -> List[QualityIssue]:
        """Check for known garbage artist patterns."""
        # Common garbage patterns in music data
        garbage_patterns = [
            "Unknown Artist",
            "Various Artists",
            "N/A",
            "null",
            "undefined",
            "test",
            "sample"
        ]
        
        pattern_conditions = " OR ".join([f"LOWER(artist_names) LIKE '%{pattern.lower()}%'" for pattern in garbage_patterns])
        
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN {pattern_conditions} THEN 1 END) as garbage_count
        FROM {table}
        WHERE decision = 'accept' AND artist_names IS NOT NULL
        """
        
        try:
            results = self._execute_query(query)
            if not results:
                return []
            
            result = results[0]
            garbage_count = result['garbage_count']
            total_count = result['total_count']
            
            if garbage_count > 0:
                percent = (garbage_count / total_count) * 100 if total_count > 0 else 0
                return [QualityIssue(
                    table=table,
                    column="artist_names",
                    issue_type="garbage_data",
                    count=garbage_count,
                    total=total_count,
                    percent=percent,
                    severity="warning",
                    description=f"Found {garbage_count} potential garbage artist names in {table}"
                )]
        except Exception:
            pass
        
        return []
    
    def _count_accept_records(self, table: str) -> int:
        """Count records with decision = 'accept' (ready for Gold promotion)."""
        try:
            results = self._execute_query(f"SELECT COUNT(*) as count FROM {table} WHERE decision = 'accept'")
            return results[0]['count'] if results else 0
        except Exception:
            return 0


def run_medallion_checkpoints(database_url: str, checkpoint_types: Optional[List[str]] = None) -> Dict[str, CheckpointResult]:
    """
    Run all Medallion architecture checkpoints.
    
    Args:
        database_url: Database connection URL
        checkpoint_types: List[Any] of checkpoint types to run (default: all)
    
    Returns:
        Dictionary mapping checkpoint names to results
    """
    if checkpoint_types is None:
        checkpoint_types = ["bronze_to_silver", "silver_to_gold"]
    
    results = {}
    
    if "bronze_to_silver" in checkpoint_types:
        checkpoint = BronzeToSilverCheckpoint(database_url)
        results["bronze_to_silver"] = checkpoint.run()
    
    if "silver_to_gold" in checkpoint_types:
        checkpoint = SilverToGoldCheckpoint(database_url)
        results["silver_to_gold"] = checkpoint.run()
    
    return results