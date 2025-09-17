"""
TDD Tests for schema analyzer - advanced database design recommendations.

These tests define the expected behavior for:
- Natural key detection
- Boolean column analysis and recommendations
- Normalization suggestions (fact tables, snowflake schema)
- AI-powered schema improvement recommendations
"""

from unittest.mock import patch
from sqlalchemy import create_engine, text

from data_quality.schema_analyzer import (
    analyze_schema,
    suggest_improvements,
    SchemaAnalysis,
    SchemaRecommendation,
    _detect_natural_keys,
    _analyze_boolean_columns,
    _suggest_normalization,
    _detect_fact_table_candidates,
    _suggest_boolean_replacements,
)


class TestSchemaAnalysis:
    """Test SchemaAnalysis dataclass."""

    def test_schema_analysis_creation(self):
        """Test creating a SchemaAnalysis object."""
        analysis = SchemaAnalysis(
            table="users",
            natural_keys=["email", "username"],
            boolean_columns=["is_active", "is_verified"],
            suggested_booleans={"status": "active/inactive"},
            normalization_level=2,
            fact_table_candidate=False,
            dimension_tables=[],
            recommendations=[],
        )

        assert analysis.table == "users"
        assert analysis.natural_keys == ["email", "username"]
        assert analysis.boolean_columns == ["is_active", "is_verified"]
        assert analysis.suggested_booleans == {"status": "active/inactive"}
        assert analysis.normalization_level == 2
        assert analysis.fact_table_candidate is False


class TestSchemaRecommendation:
    """Test SchemaRecommendation dataclass."""

    def test_schema_recommendation_creation(self):
        """Test creating a SchemaRecommendation object."""
        rec = SchemaRecommendation(
            type="normalization",
            priority="high",
            description="Extract user roles into separate table",
            sql_example="CREATE TABLE user_roles...",
            benefits=["Reduces redundancy", "Improves consistency"],
            effort_level="medium",
        )

        assert rec.type == "normalization"
        assert rec.priority == "high"
        assert rec.description == "Extract user roles into separate table"
        assert rec.sql_example == "CREATE TABLE user_roles..."
        assert rec.benefits == ["Reduces redundancy", "Improves consistency"]
        assert rec.effort_level == "medium"


class TestNaturalKeyDetection:
    """Test natural key detection functionality."""

    def test_detect_natural_keys_email_username(self):
        """Test detection of email and username as natural keys."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    username TEXT UNIQUE NOT NULL,
                    name TEXT,
                    created_at DATETIME
                )
            """
                )
            )

        natural_keys = _detect_natural_keys(engine, "users")

        # Should detect email and username as natural keys
        assert "email" in natural_keys
        assert "username" in natural_keys
        # Should not include regular columns
        assert "name" not in natural_keys
        assert "created_at" not in natural_keys

    def test_detect_natural_keys_isrc(self):
        """Test detection of ISRC as natural key in music context."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE songs (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    isrc TEXT UNIQUE,
                    spotify_id TEXT UNIQUE,
                    duration_ms INTEGER
                )
            """
                )
            )

        natural_keys = _detect_natural_keys(engine, "songs")

        # Should detect ISRC and spotify_id as natural keys
        assert "isrc" in natural_keys
        assert "spotify_id" in natural_keys
        # Should not include regular columns
        assert "title" not in natural_keys
        assert "duration_ms" not in natural_keys

    def test_detect_natural_keys_no_candidates(self):
        """Test when no natural key candidates exist."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE simple_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                )
            """
                )
            )

        natural_keys = _detect_natural_keys(engine, "simple_table")

        # Should return empty list when no natural keys found
        assert natural_keys == []


class TestBooleanAnalysis:
    """Test boolean column analysis and suggestions."""

    def test_analyze_boolean_columns_existing(self):
        """Test analysis of existing boolean columns."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    is_active BOOLEAN DEFAULT TRUE,
                    is_verified BOOLEAN DEFAULT FALSE,
                    email_confirmed INTEGER DEFAULT 0,
                    name TEXT
                )
            """
                )
            )

        boolean_cols = _analyze_boolean_columns(engine, "users")

        # Should detect boolean columns
        assert "is_active" in boolean_cols
        assert "is_verified" in boolean_cols
        # Should detect integer columns that might be booleans
        assert "email_confirmed" in boolean_cols
        # Should not include regular columns
        assert "name" not in boolean_cols

    def test_suggest_boolean_replacements_status_patterns(self):
        """Test suggestions for columns that could be booleans."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    status TEXT,  -- Could be is_completed boolean
                    payment_status TEXT,  -- Could be is_paid boolean
                    created_at DATETIME,
                    fetched_at DATETIME  -- Could be is_fetched boolean
                )
            """
                )
            )

            # Insert sample data to analyze patterns
            conn.execute(
                text(
                    """
                INSERT INTO orders (status, payment_status, created_at, fetched_at) VALUES
                ('completed', 'paid', '2024-01-01', '2024-01-01'),
                ('pending', 'unpaid', '2024-01-02', NULL),
                ('completed', 'paid', '2024-01-03', '2024-01-03')
            """
                )
            )

        suggestions = _suggest_boolean_replacements(engine, "orders")

        # Should suggest boolean replacements
        assert "status" in suggestions
        assert "payment_status" in suggestions
        # Should detect datetime patterns that could be booleans
        assert "fetched_at" in suggestions
        # Should not suggest regular datetime columns
        assert "created_at" not in suggestions

    def test_suggest_boolean_replacements_binary_values(self):
        """Test detection of columns with binary-like values."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    availability TEXT,  -- active/inactive
                    visibility TEXT,    -- visible/hidden
                    category TEXT       -- multiple values, shouldn't suggest
                )
            """
                )
            )

            conn.execute(
                text(
                    """
                INSERT INTO products (availability, visibility, category) VALUES
                ('active', 'visible', 'electronics'),
                ('inactive', 'hidden', 'books'),
                ('active', 'visible', 'clothing'),
                ('inactive', 'visible', 'electronics')
            """
                )
            )

        suggestions = _suggest_boolean_replacements(engine, "products")

        # Should suggest columns with binary-like values
        assert "availability" in suggestions
        assert suggestions["availability"] == "active/inactive"
        assert "visibility" in suggestions
        assert suggestions["visibility"] == "visible/hidden"
        # Should not suggest columns with many values
        assert "category" not in suggestions


class TestNormalizationSuggestions:
    """Test database normalization recommendations."""

    def test_suggest_normalization_denormalized_data(self):
        """Test detection of denormalized data needing extraction."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create denormalized table with repeated artist info
            conn.execute(
                text(
                    """
                CREATE TABLE songs_denormalized (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    artist_name TEXT,
                    artist_country TEXT,
                    artist_genre TEXT,
                    album_name TEXT,
                    album_year INTEGER,
                    duration_ms INTEGER
                )
            """
                )
            )

            # Insert data with repeated artist/album info
            conn.execute(
                text(
                    """
                INSERT INTO songs_denormalized VALUES
                (1, 'Song 1', 'Artist A', 'USA', 'Rock', 'Album X', 2020, 180000),
                (2, 'Song 2', 'Artist A', 'USA', 'Rock', 'Album X', 2020, 200000),
                (3, 'Song 3', 'Artist B', 'UK', 'Pop', 'Album Y', 2021, 190000)
            """
                )
            )

        recommendations = _suggest_normalization(engine, "songs_denormalized")

        # Should suggest extracting artist table
        artist_recs = [r for r in recommendations if "artist" in r.description.lower()]
        assert len(artist_recs) > 0

        # Should suggest extracting album table
        album_recs = [r for r in recommendations if "album" in r.description.lower()]
        assert len(album_recs) > 0

        # Should include SQL examples
        for rec in recommendations:
            assert rec.sql_example is not None
            assert len(rec.sql_example) > 0

    def test_suggest_normalization_already_normalized(self):
        """Test with already well-normalized schema."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create normalized tables
            conn.execute(
                text(
                    """
                CREATE TABLE artists (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE,
                    country TEXT
                )
            """
                )
            )

            conn.execute(
                text(
                    """
                CREATE TABLE songs (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    artist_id INTEGER REFERENCES artists(id),
                    duration_ms INTEGER
                )
            """
                )
            )

        recommendations = _suggest_normalization(engine, "songs")

        # Should suggest fewer or no normalization changes
        assert len(recommendations) <= 1  # Maybe some minor suggestions


class TestFactTableDetection:
    """Test fact table and dimensional modeling suggestions."""

    def test_detect_fact_table_candidates_metrics_table(self):
        """Test detection of tables that could be fact tables."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create table with metrics (fact table candidate)
            conn.execute(
                text(
                    """
                CREATE TABLE song_plays (
                    id INTEGER PRIMARY KEY,
                    song_id INTEGER,
                    user_id INTEGER,
                    play_date DATE,
                    play_count INTEGER,
                    duration_played INTEGER,
                    revenue_cents INTEGER
                )
            """
                )
            )

        is_fact_candidate = _detect_fact_table_candidates(engine, "song_plays")

        # Should detect as fact table candidate due to metrics
        assert is_fact_candidate is True

    def test_detect_fact_table_candidates_dimension_table(self):
        """Test that dimension tables are not detected as fact tables."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create dimension table (not a fact table)
            conn.execute(
                text(
                    """
                CREATE TABLE artists (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    country TEXT,
                    genre TEXT,
                    formed_year INTEGER
                )
            """
                )
            )

        is_fact_candidate = _detect_fact_table_candidates(engine, "artists")

        # Should not detect as fact table candidate
        assert is_fact_candidate is False


class TestSchemaAnalysisIntegration:
    """Test full schema analysis integration."""

    def test_analyze_schema_comprehensive(self):
        """Test comprehensive schema analysis."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            # Create realistic music database schema
            conn.execute(
                text(
                    """
                CREATE TABLE songs (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    isrc TEXT UNIQUE,
                    artist_name TEXT,  -- Denormalized
                    artist_country TEXT,  -- Denormalized
                    is_explicit BOOLEAN,
                    status TEXT,  -- Could be boolean
                    play_count INTEGER,  -- Metric
                    revenue_cents INTEGER,  -- Metric
                    created_at DATETIME,
                    fetched_at DATETIME  -- Could be is_fetched boolean
                )
            """
                )
            )

            conn.execute(
                text(
                    """
                INSERT INTO songs VALUES
                (1, 'Song 1', 'USRC123', 'Artist A', 'USA', 1, 'active', 1000, 500, '2024-01-01', '2024-01-01'),
                (2, 'Song 2', 'USRC456', 'Artist A', 'USA', 0, 'inactive', 2000, 1000, '2024-01-02', NULL)
            """
                )
            )

        analysis = analyze_schema(str(engine.url), "songs")

        # Should detect natural keys
        assert "isrc" in analysis.natural_keys

        # Should detect boolean columns
        assert "is_explicit" in analysis.boolean_columns

        # Should suggest boolean replacements
        assert "status" in analysis.suggested_booleans
        assert "fetched_at" in analysis.suggested_booleans

        # Should detect as potential fact table (has metrics)
        assert analysis.fact_table_candidate is True

        # Should have normalization recommendations
        assert len(analysis.recommendations) > 0

    def test_analyze_schema_with_options(self):
        """Test schema analysis with different options enabled/disabled."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    email TEXT UNIQUE,
                    status TEXT,
                    is_active BOOLEAN
                )
            """
                )
            )

        # Test with all features enabled
        analysis_full = analyze_schema(
            str(engine.url),
            "test_table",
            include_normalization=True,
            include_boolean_suggestions=True,
            include_fact_analysis=True,
        )

        assert len(analysis_full.natural_keys) > 0
        assert len(analysis_full.boolean_columns) > 0
        assert len(analysis_full.suggested_booleans) > 0

        # Test with features disabled
        analysis_minimal = analyze_schema(
            str(engine.url),
            "test_table",
            include_normalization=False,
            include_boolean_suggestions=False,
            include_fact_analysis=False,
        )

        assert len(analysis_minimal.recommendations) == 0
        assert len(analysis_minimal.suggested_booleans) == 0


class TestSuggestImprovements:
    """Test AI-powered improvement suggestions."""

    def test_suggest_improvements_basic(self):
        """Test basic improvement suggestions."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    email TEXT,
                    status TEXT,
                    created_at DATETIME
                )
            """
                )
            )

        suggestions = suggest_improvements(str(engine.url), ["users"])

        # Should return list of recommendations
        assert isinstance(suggestions, list)
        assert len(suggestions) >= 0  # May have suggestions

        # Each suggestion should be properly formatted
        for suggestion in suggestions:
            assert hasattr(suggestion, "type")
            assert hasattr(suggestion, "priority")
            assert hasattr(suggestion, "description")

    @patch("data_quality.schema_analyzer._get_ai_recommendations")
    def test_suggest_improvements_with_ai(self, mock_ai):
        """Test improvement suggestions with AI recommendations."""
        # Mock AI response
        mock_ai.return_value = [
            SchemaRecommendation(
                type="ai_suggestion",
                priority="medium",
                description="Consider adding indexes on frequently queried columns",
                sql_example="CREATE INDEX idx_email ON users(email);",
                benefits=["Faster queries", "Better performance"],
                effort_level="low",
            )
        ]

        engine = create_engine("sqlite+pysqlite:///:memory:")

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    email TEXT,
                    name TEXT
                )
            """
                )
            )

        suggestions = suggest_improvements(str(engine.url), ["users"], use_ai=True)

        # Should include AI suggestions
        ai_suggestions = [s for s in suggestions if s.type == "ai_suggestion"]
        assert len(ai_suggestions) > 0

        # Should have called AI function
        mock_ai.assert_called_once()


class TestErrorHandling:
    """Test error handling in schema analysis."""

    def test_analyze_schema_invalid_database(self):
        """Test schema analysis with invalid database."""
        analysis = analyze_schema("invalid://database/url", "nonexistent_table")

        # Should return analysis with error information
        assert analysis.table == "nonexistent_table"
        assert len(analysis.recommendations) > 0

        # Should have error recommendation
        error_recs = [r for r in analysis.recommendations if "error" in r.description.lower()]
        assert len(error_recs) > 0

    def test_analyze_schema_nonexistent_table(self):
        """Test schema analysis with nonexistent table."""
        engine = create_engine("sqlite+pysqlite:///:memory:")

        analysis = analyze_schema(str(engine.url), "nonexistent_table")

        # Should handle gracefully
        assert analysis.table == "nonexistent_table"
        assert analysis.natural_keys == []
        assert analysis.boolean_columns == []
