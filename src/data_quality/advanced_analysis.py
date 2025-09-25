# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Advanced database analysis functionality.

This module provides enhanced analysis capabilities including impossible-to-fill
column detection, comprehensive null analysis, and data completeness scoring.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


@dataclass
class ColumnAnalysis:
    """Detailed analysis of a single column."""
    
    name: str
    data_type: str
    total_rows: int
    filled_count: int
    null_count: int
    empty_string_count: int
    null_percentage: float
    fill_percentage: float
    category: str  # "perfect", "good", "poor", "critical", "impossible"
    is_likely_impossible: bool
    recommendations: List[str]


@dataclass
class TableAnalysis:
    """Comprehensive analysis of a table."""
    
    name: str
    total_rows: int
    columns: List[ColumnAnalysis]
    perfect_columns: List[str]
    critical_columns: List[str]
    impossible_columns: List[str]
    completeness_score: float  # 0-100
    recommendations: List[str]


@dataclass
class DatabaseAnalysis:
    """Complete database analysis report."""
    
    tables: List[TableAnalysis]
    overall_completeness_score: float
    total_tables: int
    total_columns: int
    perfect_columns_count: int
    critical_columns_count: int
    impossible_columns_count: int
    summary_recommendations: List[str]


def analyze_database_completeness(
    database_url: str, 
    table_patterns: Optional[Optional[List[str]]] = None,
    include_impossible_detection: bool = True
) -> DatabaseAnalysis:
    """
    Perform comprehensive database completeness analysis.
    
    Args:
        database_url: Database connection URL
        table_patterns: Optional table patterns to analyze
        include_impossible_detection: Whether to detect impossible-to-fill columns
        
    Returns:
        DatabaseAnalysis with detailed completeness information
        
    Example:
        >>> analysis = analyze_database_completeness("mysql://user:pass@host/db")
        >>> print(f"Overall completeness: {analysis.overall_completeness_score:.1f}%")
        >>> for table in analysis.tables:
        ...     print(f"{table.name}: {table.completeness_score:.1f}%")
    """
    engine = create_engine(database_url)
    table_analyses = []
    
    try:
        tables = _get_tables_for_analysis(engine, table_patterns)
        
        for table_name in tables:
            table_analysis = _analyze_table_completeness(
                engine, table_name, include_impossible_detection
            )
            if table_analysis:
                table_analyses.append(table_analysis)
    
    except SQLAlchemyError as e:
        # Return empty analysis on database errors
        return DatabaseAnalysis(
            tables=[],
            overall_completeness_score=0.0,
            total_tables=0,
            total_columns=0,
            perfect_columns_count=0,
            critical_columns_count=0,
            impossible_columns_count=0,
            summary_recommendations=[f"Database connection error: {str(e)}"]
        )
    
    # Calculate overall statistics
    total_columns = sum(len(table.columns) for table in table_analyses)
    perfect_columns = sum(len(table.perfect_columns) for table in table_analyses)
    critical_columns = sum(len(table.critical_columns) for table in table_analyses)
    impossible_columns = sum(len(table.impossible_columns) for table in table_analyses)
    
    # Calculate overall completeness score
    if total_columns > 0:
        overall_score = sum(
            table.completeness_score * len(table.columns) for table in table_analyses
        ) / total_columns
    else:
        overall_score = 0.0
    
    # Generate summary recommendations
    summary_recommendations = _generate_summary_recommendations(
        table_analyses, overall_score, critical_columns, impossible_columns
    )
    
    return DatabaseAnalysis(
        tables=table_analyses,
        overall_completeness_score=overall_score,
        total_tables=len(table_analyses),
        total_columns=total_columns,
        perfect_columns_count=perfect_columns,
        critical_columns_count=critical_columns,
        impossible_columns_count=impossible_columns,
        summary_recommendations=summary_recommendations
    )


def identify_impossible_columns(database_url: str) -> Dict[str, List[str]]:
    """
    Identify columns that are likely impossible to fill based on naming patterns.
    
    Args:
        database_url: Database connection URL
        
    Returns:
        Dictionary mapping table names to lists of impossible column names
        
    Example:
        >>> impossible = identify_impossible_columns("mysql://user:pass@host/db")
        >>> for table, columns in impossible.items():
        ...     print(f"{table}: {', '.join(columns)}")
    """
    # Patterns that indicate impossible-to-fill columns
    impossible_patterns = [
        r"sentiment_score",
        r"analysis_.*",
        r"processed_.*",
        r"cached_.*",
        r"external_.*",
        r"metadata_.*",
        r"raw_response",
        r"additional_.*",
        r"extended_.*",
        r"custom_.*",
        r"user_.*",
        r"recommendation_.*",
        r"trend_.*",
        r"updated_by",
        r"approved_by",
        r"notes",
        r"description",
        r"biography",
        r"website",
        r"social_.*",
        r"label_.*",
        r"contract_.*",
        r".*_hash",
        r".*_token",
        r".*_secret",
        r".*_key",
        r"temp_.*",
        r"debug_.*",
        r"test_.*"
    ]
    
    compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in impossible_patterns]
    
    engine = create_engine(database_url)
    impossible_columns = {}
    
    try:
        tables = _get_tables_for_analysis(engine)
        
        for table_name in tables:
            columns = _get_table_columns(engine, table_name)
            table_impossible = []
            
            for column_name, _ in columns:
                for pattern in compiled_patterns:
                    if pattern.search(column_name):
                        table_impossible.append(column_name)
                        break
            
            if table_impossible:
                impossible_columns[table_name] = table_impossible
    
    except SQLAlchemyError:
        pass  # Return empty dict on errors
    
    return impossible_columns


def _analyze_table_completeness(
    engine: Engine, 
    table_name: str, 
    include_impossible_detection: bool = True
) -> Optional[TableAnalysis]:
    """Analyze completeness of a single table."""
    try:
        # Get total row count
        with engine.begin() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_rows = result.scalar() or 0
        
        if total_rows == 0:
            return None  # Skip empty tables
        
        # Get column information
        columns = _get_table_columns(engine, table_name)
        column_analyses = []
        perfect_columns = []
        critical_columns = []
        impossible_columns = []
        
        for column_name, data_type in columns:
            column_analysis = _analyze_column_completeness(
                engine, table_name, column_name, data_type, total_rows
            )
            
            if column_analysis:
                column_analyses.append(column_analysis)
                
                # Categorize columns
                if column_analysis.fill_percentage == 100.0:
                    perfect_columns.append(column_name)
                elif column_analysis.null_percentage >= 95.0:
                    critical_columns.append(column_name)
                
                # Check if impossible to fill
                if include_impossible_detection and column_analysis.is_likely_impossible:
                    impossible_columns.append(column_name)
        
        # Calculate table completeness score
        if column_analyses:
            completeness_score = sum(col.fill_percentage for col in column_analyses) / len(column_analyses)
        else:
            completeness_score = 0.0
        
        # Generate table recommendations
        recommendations = _generate_table_recommendations(
            table_name, column_analyses, perfect_columns, critical_columns, impossible_columns
        )
        
        return TableAnalysis(
            name=table_name,
            total_rows=total_rows,
            columns=column_analyses,
            perfect_columns=perfect_columns,
            critical_columns=critical_columns,
            impossible_columns=impossible_columns,
            completeness_score=completeness_score,
            recommendations=recommendations
        )
    
    except SQLAlchemyError:
        return None


def _analyze_column_completeness(
    engine: Engine, 
    table_name: str, 
    column_name: str, 
    data_type: str, 
    total_rows: int
) -> Optional[ColumnAnalysis]:
    """Analyze completeness of a single column."""
    try:
        with engine.begin() as conn:
            # Count non-null, non-empty values
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {column_name} IS NOT NULL AND {column_name} != ''"
            ))
            filled_count = result.scalar() or 0
            
            # Count null values
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
            ))
            null_count = result.scalar() or 0
            
            # Count empty strings (for string columns)
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = ''"
            ))
            empty_string_count = result.scalar() or 0
        
        # Calculate percentages
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        fill_percentage = (filled_count / total_rows) * 100 if total_rows > 0 else 0
        
        # Categorize column
        if fill_percentage == 100.0:
            category = "perfect"
        elif fill_percentage >= 90.0:
            category = "good"
        elif fill_percentage >= 50.0:
            category = "poor"
        else:
            category = "critical"
        
        # Check if likely impossible to fill
        is_likely_impossible = _is_column_likely_impossible(column_name, data_type)
        
        # Generate recommendations
        recommendations = _generate_column_recommendations(
            column_name, fill_percentage, null_percentage, category, is_likely_impossible
        )
        
        return ColumnAnalysis(
            name=column_name,
            data_type=data_type,
            total_rows=total_rows,
            filled_count=filled_count,
            null_count=null_count,
            empty_string_count=empty_string_count,
            null_percentage=null_percentage,
            fill_percentage=fill_percentage,
            category=category,
            is_likely_impossible=is_likely_impossible,
            recommendations=recommendations
        )
    
    except SQLAlchemyError:
        return None


def _is_column_likely_impossible(column_name: str, data_type: str) -> bool:
    """Check if a column is likely impossible to fill based on naming patterns."""
    impossible_patterns = [
        r"sentiment_score", r"analysis_.*", r"processed_.*", r"cached_.*",
        r"external_.*", r"metadata_.*", r"raw_response", r"additional_.*",
        r"extended_.*", r"custom_.*", r"user_.*", r"recommendation_.*",
        r"trend_.*", r"updated_by", r"approved_by", r"notes", r"description",
        r"biography", r"website", r"social_.*", r"label_.*", r"contract_.*",
        r".*_hash", r".*_token", r".*_secret", r".*_key", r"temp_.*",
        r"debug_.*", r"test_.*"
    ]
    
    for pattern in impossible_patterns:
        if re.search(pattern, column_name, re.IGNORECASE):
            return True
    
    return False


def _generate_column_recommendations(
    column_name: str, 
    fill_percentage: float, 
    null_percentage: float, 
    category: str, 
    is_likely_impossible: bool
) -> List[str]:
    """Generate recommendations for a column."""
    recommendations = []
    
    if is_likely_impossible:
        recommendations.append(f"Consider if '{column_name}' is necessary - may be impossible to fill")
    elif category == "critical":
        recommendations.append(f"URGENT: '{column_name}' has {null_percentage:.1f}% null values")
        recommendations.append(f"Investigate data sources for '{column_name}'")
    elif category == "poor":
        recommendations.append(f"Improve data collection for '{column_name}' ({fill_percentage:.1f}% filled)")
    elif category == "perfect":
        recommendations.append(f"'{column_name}' has excellent data quality - maintain current processes")
    
    return recommendations


def _generate_table_recommendations(
    table_name: str,
    column_analyses: List[ColumnAnalysis],
    perfect_columns: List[str],
    critical_columns: List[str],
    impossible_columns: List[str]
) -> List[str]:
    """Generate recommendations for a table."""
    recommendations = []
    
    if critical_columns:
        recommendations.append(
            f"PRIORITY: Fix {len(critical_columns)} critical columns with >95% null values"
        )
    
    if impossible_columns:
        recommendations.append(
            f"Review {len(impossible_columns)} columns that may be impossible to fill"
        )
    
    if perfect_columns:
        recommendations.append(
            f"Maintain excellent data quality in {len(perfect_columns)} perfect columns"
        )
    
    # Calculate overall table health
    avg_completeness = sum(col.fill_percentage for col in column_analyses) / len(column_analyses)
    if avg_completeness < 70:
        recommendations.append(f"Table '{table_name}' needs significant data quality improvement")
    elif avg_completeness > 90:
        recommendations.append(f"Table '{table_name}' has excellent overall data quality")
    
    return recommendations


def _generate_summary_recommendations(
    table_analyses: List[TableAnalysis],
    overall_score: float,
    critical_columns: int,
    impossible_columns: int
) -> List[str]:
    """Generate database-wide recommendations."""
    recommendations = []
    
    if overall_score < 70:
        recommendations.append("DATABASE HEALTH: Poor - Immediate attention required")
    elif overall_score < 85:
        recommendations.append("DATABASE HEALTH: Fair - Improvement needed")
    else:
        recommendations.append("DATABASE HEALTH: Good - Maintain current quality")
    
    if critical_columns > 0:
        recommendations.append(f"URGENT: Address {critical_columns} critical columns with >95% null values")
    
    if impossible_columns > 0:
        recommendations.append(f"REVIEW: {impossible_columns} columns may be impossible to fill")
    
    # Find best and worst tables
    if table_analyses:
        best_table = max(table_analyses, key=lambda t: t.completeness_score)
        worst_table = min(table_analyses, key=lambda t: t.completeness_score)
        
        recommendations.append(f"Best table: '{best_table.name}' ({best_table.completeness_score:.1f}%)")
        recommendations.append(f"Worst table: '{worst_table.name}' ({worst_table.completeness_score:.1f}%)")
    
    return recommendations


def _get_tables_for_analysis(engine: Engine, patterns: Optional[Optional[List[str]]] = None) -> List[str]:
    """Get list of tables for analysis."""
    try:
        # Try MySQL/PostgreSQL approach
        if patterns:
            pattern_conditions = " OR ".join([f"table_name LIKE '{pattern}'" for pattern in patterns])
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

    except SQLAlchemyError:
        # Fallback for SQLite
        try:
            query = text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            with engine.begin() as conn:
                result = conn.execute(query)
                tables = [row[0] for row in result]

                if patterns:
                    # Simple pattern matching for SQLite
                    filtered_tables = []
                    for table in tables:
                        for pattern in patterns:
                            if pattern.replace("%", "") in table:
                                filtered_tables.append(table)
                                break
                    return filtered_tables
                return tables
        except SQLAlchemyError:
            return []


def _get_table_columns(engine: Engine, table_name: str) -> List[Tuple[str, str]]:
    """Get column names and types for a table."""
    try:
        # Try MySQL/PostgreSQL approach
        query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            AND table_schema = DATABASE()
            ORDER BY ordinal_position
        """)
        
        with engine.begin() as conn:
            result = conn.execute(query, {"table_name": table_name})
            return [(row[0], row[1]) for row in result]
    
    except SQLAlchemyError:
        # Fallback for SQLite
        try:
            query = text(f"PRAGMA table_info({table_name})")
            with engine.begin() as conn:
                result = conn.execute(query)
                return [(row[1], row[2]) for row in result]  # name, type
        except SQLAlchemyError:
            return []