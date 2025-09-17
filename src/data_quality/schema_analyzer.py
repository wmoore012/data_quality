"""
Advanced database schema analysis with AI-powered recommendations.

This module provides intelligent analysis of database schemas including:
- Natural key detection
- Boolean column analysis and suggestions
- Normalization recommendations (fact tables, snowflake schema)
- AI-powered schema improvement suggestions
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


@dataclass
class SchemaAnalysis:
    """Analysis results for a database table schema."""

    table: str
    natural_keys: List[str]
    boolean_columns: List[str]
    suggested_booleans: Dict[str, str]  # column -> suggested values
    normalization_level: int  # 1NF, 2NF, 3NF, etc.
    fact_table_candidate: bool
    dimension_tables: List[str]
    recommendations: List["SchemaRecommendation"]


@dataclass
class SchemaRecommendation:
    """A specific recommendation for schema improvement."""

    type: str  # "normalization", "boolean", "indexing", "ai_suggestion"
    priority: str  # "high", "medium", "low"
    description: str
    sql_example: Optional[str] = None
    benefits: List[str] = None
    effort_level: str = "medium"  # "low", "medium", "high"


def analyze_schema(
    database_url: str,
    table: str,
    include_normalization: bool = True,
    include_boolean_suggestions: bool = True,
    include_fact_analysis: bool = True,
) -> SchemaAnalysis:
    """
    Perform comprehensive schema analysis on a database table.

    Args:
        database_url: Database connection URL
        table: Table name to analyze
        include_normalization: Whether to include normalization suggestions
        include_boolean_suggestions: Whether to suggest boolean improvements
        include_fact_analysis: Whether to analyze for fact/dimension patterns

    Returns:
        SchemaAnalysis with detailed recommendations

    Example:
        >>> analysis = analyze_schema("mysql://user:pass@host/db", "songs")
        >>> print(f"Natural keys: {analysis.natural_keys}")
        >>> for rec in analysis.recommendations:
        ...     print(f"{rec.priority}: {rec.description}")
    """
    try:
        engine = create_engine(database_url)

        # Detect natural keys
        natural_keys = _detect_natural_keys(engine, table)

        # Analyze boolean columns
        boolean_columns = _analyze_boolean_columns(engine, table)

        # Suggest boolean replacements
        suggested_booleans = {}
        if include_boolean_suggestions:
            suggested_booleans = _suggest_boolean_replacements(engine, table)

        # Analyze normalization
        recommendations = []
        normalization_level = 3  # Assume 3NF by default

        if include_normalization:
            norm_recs = _suggest_normalization(engine, table)
            recommendations.extend(norm_recs)
            normalization_level = _estimate_normalization_level(engine, table)

        # Fact table analysis
        fact_table_candidate = False
        dimension_tables = []

        if include_fact_analysis:
            fact_table_candidate = _detect_fact_table_candidates(engine, table)
            if fact_table_candidate:
                dimension_tables = _suggest_dimension_tables(engine, table)

        return SchemaAnalysis(
            table=table,
            natural_keys=natural_keys,
            boolean_columns=boolean_columns,
            suggested_booleans=suggested_booleans,
            normalization_level=normalization_level,
            fact_table_candidate=fact_table_candidate,
            dimension_tables=dimension_tables,
            recommendations=recommendations,
        )

    except SQLAlchemyError as e:
        # Return analysis with error information
        return SchemaAnalysis(
            table=table,
            natural_keys=[],
            boolean_columns=[],
            suggested_booleans={},
            normalization_level=1,
            fact_table_candidate=False,
            dimension_tables=[],
            recommendations=[
                SchemaRecommendation(
                    type="error",
                    priority="high",
                    description=f"Database analysis failed: {str(e)}",
                    benefits=["Fix database connection to get recommendations"],
                )
            ],
        )


def suggest_improvements(
    database_url: str,
    tables: List[str],
    use_ai: bool = False,
    user_preferences: Optional[Dict[str, str]] = None,
) -> List[SchemaRecommendation]:
    """
    Generate comprehensive improvement suggestions for multiple tables.

    Args:
        database_url: Database connection URL
        tables: List of table names to analyze
        use_ai: Whether to include AI-powered recommendations
        user_preferences: User preferences for recommendations

    Returns:
        List of prioritized schema recommendations

    Example:
        >>> suggestions = suggest_improvements("mysql://...", ["users", "orders"])
        >>> for suggestion in suggestions:
        ...     print(f"{suggestion.priority}: {suggestion.description}")
    """
    all_recommendations = []

    for table in tables:
        analysis = analyze_schema(database_url, table)
        all_recommendations.extend(analysis.recommendations)

    # Add AI recommendations if requested
    if use_ai:
        try:
            ai_recs = _get_ai_recommendations(database_url, tables, user_preferences)
            all_recommendations.extend(ai_recs)
        except Exception:
            # AI recommendations are optional, don't fail if they don't work
            pass

    # Sort by priority
    priority_order = {"high": 0, "medium": 1, "low": 2}
    all_recommendations.sort(key=lambda x: priority_order.get(x.priority, 3))

    return all_recommendations


def _detect_natural_keys(engine: Engine, table: str) -> List[str]:
    """Detect columns that could serve as natural keys."""
    natural_keys = []

    try:
        # Get column information with constraints
        columns_info = _get_column_constraints(engine, table)

        for column, constraints in columns_info.items():
            # Check if column has natural key characteristics
            if _is_natural_key_candidate(column, constraints):
                natural_keys.append(column)

    except SQLAlchemyError:
        pass

    return natural_keys


def _analyze_boolean_columns(engine: Engine, table: str) -> List[str]:
    """Identify existing boolean columns and potential boolean candidates."""
    boolean_columns = []

    try:
        # Get column types
        column_types = _get_column_types(engine, table)

        for column, col_type in column_types.items():
            if _is_boolean_column(column, col_type):
                boolean_columns.append(column)

    except SQLAlchemyError:
        pass

    return boolean_columns


def _suggest_boolean_replacements(engine: Engine, table: str) -> Dict[str, str]:
    """Suggest columns that could be replaced with booleans."""
    suggestions = {}

    try:
        # Analyze column values for binary patterns
        columns = _get_table_columns(engine, table)

        for column in columns:
            suggestion = _analyze_column_for_boolean(engine, table, column)
            if suggestion:
                suggestions[column] = suggestion

    except SQLAlchemyError:
        pass

    return suggestions


def _suggest_normalization(engine: Engine, table: str) -> List[SchemaRecommendation]:
    """Suggest normalization improvements."""
    recommendations = []

    try:
        # Detect denormalization patterns
        denorm_patterns = _detect_denormalization(engine, table)

        for pattern in denorm_patterns:
            rec = _create_normalization_recommendation(pattern)
            recommendations.append(rec)

        # Suggest fact/dimension modeling if applicable
        if _detect_fact_table_candidates(engine, table):
            dim_recs = _suggest_dimensional_modeling(engine, table)
            recommendations.extend(dim_recs)

    except SQLAlchemyError:
        pass

    return recommendations


def _detect_fact_table_candidates(engine: Engine, table: str) -> bool:
    """Detect if table could be a fact table in dimensional model."""
    try:
        # Check for metrics/measures
        columns = _get_table_columns(engine, table)
        metric_columns = [col for col in columns if _is_metric_column(col)]

        # Check for foreign keys (dimension references)
        fk_columns = _get_foreign_key_columns(engine, table)

        # Fact tables typically have metrics and multiple foreign keys
        return len(metric_columns) >= 2 and len(fk_columns) >= 2

    except SQLAlchemyError:
        return False


def _suggest_dimension_tables(engine: Engine, table: str) -> List[str]:
    """Suggest dimension tables for a fact table."""
    dimensions = []

    try:
        # Look for repeated attribute patterns that could be dimensions
        denorm_patterns = _detect_denormalization(engine, table)

        for pattern in denorm_patterns:
            if pattern["type"] == "repeated_attributes":
                dimensions.append(pattern["suggested_table"])

    except SQLAlchemyError:
        pass

    return dimensions


def _get_ai_recommendations(
    database_url: str, tables: List[str], user_preferences: Optional[Dict[str, str]] = None
) -> List[SchemaRecommendation]:
    """
    Generate AI-powered schema recommendations.

    This is a placeholder for future AI integration.
    Could integrate with OpenAI, local models, or rule-based AI.
    """
    # Placeholder AI recommendations
    ai_recommendations = [
        SchemaRecommendation(
            type="ai_suggestion",
            priority="medium",
            description="Consider adding indexes on frequently queried columns",
            sql_example="CREATE INDEX idx_email ON users(email);",
            benefits=["Faster queries", "Better performance"],
            effort_level="low",
        )
    ]

    return ai_recommendations


# Helper functions


def _get_column_constraints(engine: Engine, table: str) -> Dict[str, Dict[str, bool]]:
    """Get column constraint information."""
    constraints = {}

    try:
        # Try MySQL/PostgreSQL approach
        query = text(
            """
            SELECT 
                column_name,
                is_nullable,
                column_key,
                column_type
            FROM information_schema.columns 
            WHERE table_schema = DATABASE() 
            AND table_name = :table
        """
        )

        with engine.begin() as conn:
            result = conn.execute(query, {"table": table})
            for row in result:
                constraints[row[0]] = {
                    "nullable": row[1] == "YES",
                    "unique": "UNI" in (row[2] or ""),
                    "primary": "PRI" in (row[2] or ""),
                    "type": row[3] or "",
                }

    except SQLAlchemyError:
        # Fallback for SQLite
        try:
            query = text(f"PRAGMA table_info({table})")
            with engine.begin() as conn:
                result = conn.execute(query)
                for row in result:
                    constraints[row[1]] = {
                        "nullable": not row[3],  # not null flag
                        "unique": False,  # Would need separate query
                        "primary": bool(row[5]),  # pk flag
                        "type": row[2] or "",
                    }
        except SQLAlchemyError:
            pass

    return constraints


def _get_column_types(engine: Engine, table: str) -> Dict[str, str]:
    """Get column data types."""
    column_types = {}

    try:
        # Try information_schema first
        query = text(
            """
            SELECT column_name, data_type, column_type
            FROM information_schema.columns 
            WHERE table_schema = DATABASE() 
            AND table_name = :table
        """
        )

        with engine.begin() as conn:
            result = conn.execute(query, {"table": table})
            for row in result:
                column_types[row[0]] = row[1] or row[2] or ""

    except SQLAlchemyError:
        # Fallback for SQLite
        try:
            query = text(f"PRAGMA table_info({table})")
            with engine.begin() as conn:
                result = conn.execute(query)
                for row in result:
                    column_types[row[1]] = row[2] or ""
        except SQLAlchemyError:
            pass

    return column_types


def _get_table_columns(engine: Engine, table: str) -> List[str]:
    """Get list of column names for a table."""
    columns = []

    try:
        query = text(f"SELECT * FROM {table} LIMIT 0")
        with engine.begin() as conn:
            result = conn.execute(query)
            columns = list(result.keys())
    except SQLAlchemyError:
        pass

    return columns


def _is_natural_key_candidate(column: str, constraints: Dict[str, bool]) -> bool:
    """Determine if a column could be a natural key."""
    column_lower = column.lower()

    # Check for unique constraint
    if not constraints.get("unique", False) and not constraints.get("primary", False):
        return False

    # Common natural key patterns
    natural_key_patterns = [
        "email",
        "username",
        "isrc",
        "isbn",
        "sku",
        "code",
        "spotify_id",
        "youtube_id",
        "tidal_id",
        "apple_id",
        "external_id",
        "reference",
        "slug",
    ]

    return any(pattern in column_lower for pattern in natural_key_patterns)


def _is_boolean_column(column: str, col_type: str) -> bool:
    """Determine if a column is or should be boolean."""
    column_lower = column.lower()
    type_lower = col_type.lower()

    # Explicit boolean types
    if "bool" in type_lower or "bit" in type_lower:
        return True

    # Boolean naming patterns
    boolean_patterns = [
        "is_",
        "has_",
        "can_",
        "should_",
        "will_",
        "was_",
        "were_",
        "_flag",
        "_enabled",
        "_active",
        "_verified",
        "_confirmed",
    ]

    # Integer columns that might be booleans (0/1)
    if "int" in type_lower and any(pattern in column_lower for pattern in boolean_patterns):
        return True

    return any(pattern in column_lower for pattern in boolean_patterns)


def _analyze_column_for_boolean(engine: Engine, table: str, column: str) -> Optional[str]:
    """Analyze if a column could be replaced with a boolean."""
    try:
        # Get distinct values
        query = text(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT 10")
        with engine.begin() as conn:
            result = conn.execute(query)
            values = [row[0] for row in result if row[0] is not None]

        # Check for binary patterns
        if len(values) == 2:
            values_str = [str(v).lower() for v in values]

            # Common binary patterns
            binary_patterns = [
                ("active", "inactive"),
                ("enabled", "disabled"),
                ("visible", "hidden"),
                ("public", "private"),
                ("yes", "no"),
                ("true", "false"),
                ("1", "0"),
                ("on", "off"),
            ]

            for pattern in binary_patterns:
                if set(values_str) == set(pattern):
                    return f"{pattern[0]}/{pattern[1]}"

        # Check for datetime patterns that could be booleans
        column_lower = column.lower()
        if "_at" in column_lower and any(
            prefix in column_lower for prefix in ["fetch", "process", "complet", "verif"]
        ):
            # Count nulls vs non-nulls
            null_query = text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
            total_query = text(f"SELECT COUNT(*) FROM {table}")

            with engine.begin() as conn:
                null_count = conn.execute(null_query).scalar()
                total_count = conn.execute(total_query).scalar()

            if null_count > 0 and null_count < total_count:
                return f"is_{column_lower.replace('_at', 'ed')}"

    except SQLAlchemyError:
        pass

    return None


def _detect_denormalization(engine: Engine, table: str) -> List[Dict[str, str]]:
    """Detect denormalization patterns that could be normalized."""
    patterns = []

    try:
        columns = _get_table_columns(engine, table)

        # Look for repeated prefixes (e.g., artist_name, artist_country, artist_genre)
        prefix_groups = {}
        for column in columns:
            if "_" in column:
                prefix = column.split("_")[0]
                if prefix not in prefix_groups:
                    prefix_groups[prefix] = []
                prefix_groups[prefix].append(column)

        # Suggest extraction for groups with multiple columns
        for prefix, group_columns in prefix_groups.items():
            if len(group_columns) >= 2 and prefix not in ["created", "updated", "is", "has"]:
                patterns.append(
                    {
                        "type": "repeated_attributes",
                        "prefix": prefix,
                        "columns": group_columns,
                        "suggested_table": f"{prefix}s",
                        "description": f"Extract {prefix} attributes into separate table",
                    }
                )

    except SQLAlchemyError:
        pass

    return patterns


def _create_normalization_recommendation(pattern: Dict[str, str]) -> SchemaRecommendation:
    """Create a normalization recommendation from a detected pattern."""
    if pattern["type"] == "repeated_attributes":
        prefix = pattern["prefix"]
        columns = pattern["columns"]
        suggested_table = pattern["suggested_table"]

        # Generate SQL example
        sql_example = f"""
-- Create normalized {suggested_table} table
CREATE TABLE {suggested_table} (
    id INTEGER PRIMARY KEY,
    {', '.join([col.replace(f'{prefix}_', '') + ' TEXT' for col in columns])}
);

-- Add foreign key to main table
ALTER TABLE {pattern.get('table', 'main_table')} 
ADD COLUMN {prefix}_id INTEGER REFERENCES {suggested_table}(id);
"""

        return SchemaRecommendation(
            type="normalization",
            priority="medium",
            description=f"Extract {prefix} attributes into separate {suggested_table} table",
            sql_example=sql_example.strip(),
            benefits=[
                "Reduces data redundancy",
                "Improves data consistency",
                "Easier to maintain reference data",
                "Better query performance for reference lookups",
            ],
            effort_level="medium",
        )

    return SchemaRecommendation(
        type="normalization",
        priority="low",
        description="Consider normalization improvements",
        benefits=["Better data organization"],
    )


def _suggest_dimensional_modeling(engine: Engine, table: str) -> List[SchemaRecommendation]:
    """Suggest dimensional modeling improvements for fact tables."""
    recommendations = []

    # Suggest star schema organization
    recommendations.append(
        SchemaRecommendation(
            type="dimensional_modeling",
            priority="medium",
            description=f"Consider organizing {table} as fact table in star schema",
            sql_example=f"""
-- Example star schema for {table}
-- Fact table (current table with metrics)
-- Dimension tables for each foreign key reference
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);
""",
            benefits=[
                "Optimized for analytical queries",
                "Better performance for aggregations",
                "Easier to understand business metrics",
                "Supports OLAP operations",
            ],
            effort_level="high",
        )
    )

    return recommendations


def _is_metric_column(column: str) -> bool:
    """Determine if a column represents a metric/measure."""
    column_lower = column.lower()

    metric_patterns = [
        "count",
        "total",
        "sum",
        "amount",
        "revenue",
        "cost",
        "price",
        "duration",
        "length",
        "size",
        "weight",
        "volume",
        "quantity",
        "rate",
        "percentage",
        "score",
        "rating",
        "plays",
        "views",
        "clicks",
        "impressions",
        "conversions",
    ]

    return any(pattern in column_lower for pattern in metric_patterns)


def _get_foreign_key_columns(engine: Engine, table: str) -> List[str]:
    """Get columns that are foreign keys."""
    fk_columns = []

    try:
        # Look for columns ending in _id (common foreign key pattern)
        columns = _get_table_columns(engine, table)
        fk_columns = [col for col in columns if col.lower().endswith("_id") and col.lower() != "id"]

    except SQLAlchemyError:
        pass

    return fk_columns


def _estimate_normalization_level(engine: Engine, table: str) -> int:
    """Estimate the normalization level of a table."""
    try:
        # Check for denormalization patterns
        denorm_patterns = _detect_denormalization(engine, table)

        if len(denorm_patterns) > 2:
            return 1  # Likely 1NF - has repeating groups
        elif len(denorm_patterns) > 0:
            return 2  # Likely 2NF - some partial dependencies
        else:
            return 3  # Likely 3NF or higher

    except SQLAlchemyError:
        return 3  # Assume normalized if can't analyze


def _get_ai_recommendations(
    database_url: str, 
    tables: List[str], 
    user_preferences: Optional[Dict[str, str]] = None
) -> List[SchemaRecommendation]:
    """
    Generate AI-powered schema recommendations using pattern analysis.
    
    This AI system analyzes database patterns and provides intelligent suggestions
    based on industry best practices and common optimization patterns.
    """
    ai_recommendations = []
    
    try:
        engine = create_engine(database_url)
        
        for table in tables:
            # AI Analysis 1: Index Recommendations
            index_recs = _ai_analyze_indexing_opportunities(engine, table)
            ai_recommendations.extend(index_recs)
            
            # AI Analysis 2: Industry-Specific Patterns
            industry_recs = _ai_analyze_industry_patterns(engine, table, user_preferences)
            ai_recommendations.extend(industry_recs)
            
    except SQLAlchemyError:
        # Fallback AI recommendations based on common patterns
        ai_recommendations = _get_fallback_ai_recommendations(tables)
    
    return ai_recommendations


def _ai_analyze_indexing_opportunities(engine: Engine, table: str) -> List[SchemaRecommendation]:
    """AI analysis for indexing opportunities."""
    recommendations = []
    
    try:
        # Get columns that would benefit from indexes
        columns = _get_table_columns(engine, table)
        
        # AI Rule: Foreign key columns should have indexes
        fk_columns = [col for col in columns if col.lower().endswith('_id') and col.lower() != 'id']
        for fk_col in fk_columns:
            recommendations.append(SchemaRecommendation(
                type="ai_indexing",
                priority="high",
                description=f"🚀 Add index on foreign key column '{fk_col}' for better JOIN performance",
                sql_example=f"CREATE INDEX idx_{table}_{fk_col} ON {table}({fk_col});",
                benefits=["Faster JOINs", "Improved query performance", "Better foreign key lookups"],
                effort_level="low"
            ))
        
        # AI Rule: Email columns should have indexes
        email_columns = [col for col in columns if 'email' in col.lower()]
        for email_col in email_columns:
            recommendations.append(SchemaRecommendation(
                type="ai_indexing",
                priority="medium",
                description=f"📧 Add index on email column '{email_col}' for user lookups",
                sql_example=f"CREATE INDEX idx_{table}_{email_col} ON {table}({email_col});",
                benefits=["Faster user authentication", "Improved search performance"],
                effort_level="low"
            ))
            
    except SQLAlchemyError:
        pass
    
    return recommendations


def _ai_analyze_industry_patterns(engine: Engine, table: str, user_preferences: Optional[Dict[str, str]] = None) -> List[SchemaRecommendation]:
    """AI analysis for industry-specific patterns and best practices."""
    recommendations = []
    
    try:
        columns = _get_table_columns(engine, table)
        column_names_lower = [col.lower() for col in columns]
        
        # AI Rule: Music industry patterns
        if any(col in column_names_lower for col in ['isrc', 'artist', 'song', 'track', 'album']):
            if 'isrc' in column_names_lower and 'spotify_id' not in column_names_lower:
                recommendations.append(SchemaRecommendation(
                    type="ai_industry",
                    priority="medium",
                    description="🎵 Music table detected - consider adding streaming platform IDs",
                    sql_example=f"ALTER TABLE {table} ADD COLUMN spotify_id VARCHAR(50);\nALTER TABLE {table} ADD COLUMN apple_music_id VARCHAR(50);",
                    benefits=["Better platform integration", "Enhanced data linking", "Industry standard compliance"],
                    effort_level="low"
                ))
            
            if 'play_count' in column_names_lower or 'streams' in column_names_lower:
                recommendations.append(SchemaRecommendation(
                    type="ai_industry",
                    priority="high",
                    description="📊 Metrics table detected - consider partitioning by date for performance",
                    sql_example=f"-- Consider partitioning large metrics tables\nCREATE TABLE {table}_2024 PARTITION OF {table} FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
                    benefits=["Better query performance", "Easier data archiving", "Improved maintenance"],
                    effort_level="high"
                ))
        
        # AI Rule: User management patterns
        if any(col in column_names_lower for col in ['user', 'email', 'password', 'login']):
            if 'email' in column_names_lower and 'email_verified' not in column_names_lower:
                recommendations.append(SchemaRecommendation(
                    type="ai_security",
                    priority="high",
                    description="🔐 User table should track email verification for security",
                    sql_example=f"ALTER TABLE {table} ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;\nALTER TABLE {table} ADD COLUMN email_verified_at TIMESTAMP;",
                    benefits=["Better security", "Email validation", "User onboarding tracking"],
                    effort_level="low"
                ))
                
    except SQLAlchemyError:
        pass
    
    return recommendations


def _get_fallback_ai_recommendations(tables: List[str]) -> List[SchemaRecommendation]:
    """Fallback AI recommendations when database analysis fails."""
    recommendations = []
    
    # General best practices
    recommendations.append(SchemaRecommendation(
        type="ai_general",
        priority="medium",
        description="🕒 Consider adding audit timestamps to all tables",
        sql_example="ALTER TABLE your_table ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;\nALTER TABLE your_table ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
        benefits=["Better audit trails", "Data lineage tracking", "Debugging capabilities"],
        effort_level="low"
    ))
    
    recommendations.append(SchemaRecommendation(
        type="ai_general",
        priority="high",
        description="🔑 Ensure all tables have proper primary keys",
        sql_example="-- If no primary key exists:\nALTER TABLE your_table ADD COLUMN id INTEGER PRIMARY KEY AUTO_INCREMENT FIRST;",
        benefits=["Better replication", "Improved performance", "Data integrity"],
        effort_level="medium"
    ))
    
    return recommendations


def _get_table_columns(engine: Engine, table: str) -> List[str]:
    """Get list of column names for a table."""
    columns = []
    
    try:
        query = text(f"SELECT * FROM {table} LIMIT 0")
        with engine.begin() as conn:
            result = conn.execute(query)
            columns = list(result.keys())
    except SQLAlchemyError:
        pass
    
    return columns