#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Perday CatalogLAB™

"""
Smart Backfill Suggester

Scans your database for missing foreign key relationships and suggests
intelligent backfill strategies. Uses AI to generate specific SQL queries.

Usage:
    python smart_backfill_suggester.py --interactive
    python smart_backfill_suggester.py --table songs --save - suggestions
"""

import argparse
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

# Add project paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from data_quality.quality_scanner import _get_tables

# DEPRECATED: # DEPRECATED: from web.db_guard import
# Use SQLAlchemy directly get_engine
# Use SQLAlchemy create_engine directly


@dataclass
class BackfillOpportunity:
    """Represents a potential backfill opportunity."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    missing_count: int
    total_count: int
    confidence: str  # "high", "medium", "low"
    suggested_query: str
    explanation: str


class SmartBackfillSuggester:
    """Intelligent backfill suggestion engine."""

    def __init__(self, engine):
        self.engine = engine
        self.opportunities = []

    def scan_for_opportunities(
        self, table_filter: Optional[str] = None, verbose: bool = False
    ) -> List[BackfillOpportunity]:
        """Scan database for backfill opportunities."""
        print("🔍 Scanning for backfill opportunities...")

        tables = self._get_relevant_tables(table_filter)
        opportunities = []

        for table in tables:
            print(f"   📋 Analyzing {table}...")
            table_opportunities = self._analyze_table_relationships(table, verbose)
            opportunities.extend(table_opportunities)

        # Sort by impact (missing count desc)
        opportunities.sort(key=lambda x: x.missing_count, reverse=True)
        self.opportunities = opportunities
        return opportunities

    def _get_relevant_tables(self, table_filter: Optional[str] = None) -> List[str]:
        """Get tables to analyze."""
        all_tables = _get_tables(self.engine)

        if table_filter:
            return [t for t in all_tables if table_filter.lower() in t.lower()]

        # Focus on key music industry tables
        priority_tables = [
            "songs",
            "artists",
            "albums",
            "labels",
            "spotify_tracks",
            "youtube_videos",
            "tidal_tracks_raw",
        ]

        return [t for t in all_tables if t in priority_tables] or all_tables[:10]

    def _analyze_table_relationships(
        self, table: str, verbose: bool = False
    ) -> List[BackfillOpportunity]:
        """Analyze a table for potential backfill opportunities."""
        opportunities = []

        try:
            # Get table structure
            columns = self._get_table_columns(table)
            if verbose:
                print(f"      Columns: {', '.join(columns)}")

            # Look for foreign key patterns
            fk_columns = [col for col in columns if self._looks_like_foreign_key(col)]
            if verbose and fk_columns:
                print(f"      FK - like columns: {', '.join(fk_columns)}")

            for column in fk_columns:
                opportunity = self._check_foreign_key_opportunity(
                    table, column, verbose
                )
                if opportunity:
                    opportunities.append(opportunity)

            # Look for missing relationships based on naming patterns
            relationship_opportunities = self._find_missing_relationships(
                table, columns, verbose
            )
            opportunities.extend(relationship_opportunities)

            # Skip NULL checking - user wants orphaned foreign keys, not NULL values

        except Exception as e:
            print(f"   ⚠️  Error analyzing {table}: {e}")

        return opportunities

    def _get_table_columns(self, table: str) -> List[str]:
        """Get all columns for a table."""
        try:
            from sqlalchemy import text

            query = text(f"SELECT * FROM {table} LIMIT 0")
            with self.engine.connect() as conn:
                result = conn.execute(query)
                return list(result.keys())
        except:
            return []

    def _find_null_opportunities(
        self, table: str, columns: List[str], verbose: bool = False
    ) -> List[BackfillOpportunity]:
        """Find opportunities to backfill NULL values in important columns."""
        opportunities = []

        # Important columns that shouldn't be NULL
        important_columns = [
            col
            for col in columns
            if col.lower()
            in ["isrc", "spotify_id", "youtube_id", "artist_id", "album_id", "label_id"]
        ]

        if not important_columns:
            return []

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                for column in important_columns:
                    # Count NULL values
                    null_query = text(
                        f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
                    )
                    null_count = conn.execute(null_query).scalar() or 0

                    total_query = text(f"SELECT COUNT(*) FROM {table}")
                    total_count = conn.execute(total_query).scalar() or 0

                    if null_count > 0 and total_count > 0:
                        percent = (null_count / total_count) * 100

                        if verbose:
                            print(
                                f"        {column}: {null_count}/{total_count} NULL ({percent:.1f}%)"
                            )

                        # Only suggest backfill if it's a significant issue
                        if percent > 5:  # More than 5% NULL
                            opportunity = self._create_null_backfill_opportunity(
                                table, column, null_count, total_count
                            )
                            if opportunity:
                                opportunities.append(opportunity)

        except Exception as e:
            if verbose:
                print(f"        Error checking NULLs: {e}")

        return opportunities

    def _create_null_backfill_opportunity(
        self, table: str, column: str, null_count: int, total_count: int
    ) -> Optional[BackfillOpportunity]:
        """Create backfill opportunity for NULL values."""

        percent = (null_count / total_count) * 100
        confidence = "medium" if percent < 30 else "low"

        if column.lower() == "isrc":
            suggested_query = f"""
-- Generate ISRCs for songs missing them
-- Note: Real ISRCs should come from official sources
UPDATE {table}
SET {column} = CONCAT('TEMP', LPAD(id, 10, '0'))
WHERE {column} IS NULL
AND title IS NOT NULL;

-- Better: Import real ISRCs from external source
-- UPDATE {table} SET isrc = external_data.isrc
-- FROM external_isrc_data external_data
-- WHERE {table}.title = external_data.title
-- AND {table}.artist_name = external_data.artist_name;
"""
            explanation = "Missing ISRCs can be backfilled from external music databases or generated temporarily"

        elif "spotify_id" in column.lower():
            suggested_query = f"""
-- Find Spotify IDs by matching song / artist names
-- This requires Spotify API integration
UPDATE {table}
SET {column} = spotify_lookup.spotify_id
FROM (
    -- This would be populated by Spotify API calls
    SELECT song_title, artist_name, spotify_id
    FROM spotify_api_results
) spotify_lookup
WHERE {table}.title = spotify_lookup.song_title
AND {table}.{column} IS NULL;
"""
            explanation = "Missing Spotify IDs can be found using Spotify Web API by matching song / artist names"

        elif "album_id" in column.lower():
            # Check if we can match by album name or other fields
            suggested_query = self._generate_smart_album_backfill(table)
            explanation = "Missing album_id values can potentially be matched using song title patterns or external music databases"

        else:
            suggested_query = f"""
-- Backfill {column} based on available data
-- Strategy depends on your specific data sources
UPDATE {table}
SET {column} = 'NEEDS_RESEARCH'
WHERE {column} IS NULL;
"""
            explanation = f"Missing {column} values need manual research or external data source integration"

        return BackfillOpportunity(
            source_table=table,
            source_column=f"{column} (NULL values)",
            target_table="external_data_source",
            target_column=column,
            missing_count=null_count,
            total_count=total_count,
            confidence=confidence,
            suggested_query=suggested_query.strip(),
            explanation=explanation,
        )

    def _generate_smart_album_backfill(self, table: str) -> str:
        """Generate smart album backfill suggestions based on actual data."""
        try:
            from sqlalchemy import text

            # Check what data we have to work with
            with self.engine.connect() as conn:
                # See if there are patterns we can use
                sample_query = text(
                    f"""
                    SELECT song_title, album_id
                    FROM {table}
                    WHERE album_id IS NULL
                    LIMIT 5
                """
                )
                samples = conn.execute(sample_query).fetchall()

                if samples:
                    sample_titles = [row[0] for row in samples if row[0]]

                    return f"""
-- Smart album_id backfill strategies:

-- Option 1: Match by song title patterns (if albums table exists)
UPDATE {table}
SET album_id = (
    SELECT a.id
    FROM albums a
    WHERE {table}.song_title LIKE CONCAT('%', a.title, '%')
    LIMIT 1
)
WHERE album_id IS NULL;

-- Option 2: Use external music database APIs
-- Example songs needing album_id: {', '.join(sample_titles[:3]) if sample_titles else 'N / A'}
-- Consider using MusicBrainz, Spotify, or Last.fm APIs

-- Option 3: Create "Unknown Album" entries
INSERT INTO albums (title, artist_id)
VALUES ('Unknown Album', NULL);

UPDATE {table}
SET album_id = (SELECT id FROM albums WHERE title = 'Unknown Album')
WHERE album_id IS NULL;
"""

        except Exception:
            return """
-- Basic album_id backfill
UPDATE songs
SET album_id = NULL  -- Keep as NULL until proper data is available
WHERE album_id IS NULL;
"""

    def _looks_like_foreign_key(self, column: str) -> bool:
        """Check if column name suggests it's a foreign key."""
        column_lower = column.lower()
        return (
            column_lower.endswith("_id")
            or column_lower.endswith("id")
            and len(column_lower) > 2
            or column_lower
            in ["artist_id", "album_id", "label_id", "spotify_id", "youtube_id"]
        )

    def _check_foreign_key_opportunity(
        self, table: str, column: str, verbose: bool = False
    ) -> Optional[BackfillOpportunity]:
        """Check if a foreign key column has missing relationships - the REAL backfill opportunity finder."""
        try:
            from sqlalchemy import text

            # Guess the target table name
            target_table = self._guess_target_table(column)
            if not target_table:
                return None

            # Check if target table exists
            if not self._table_exists(target_table):
                if verbose:
                    print(f"        {column} -> {target_table} (table doesn't exist)")
                return None

            # THIS IS THE KEY PART: Find orphaned foreign keys
            with self.engine.connect() as conn:
                # Count total non - null values in source
                total_query = text(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"
                )
                total_count = conn.execute(total_query).scalar() or 0

                if total_count == 0:
                    return None

                # Count ORPHANED relationships - foreign keys pointing to non - existent records
                target_pk = self._guess_primary_key(target_table)
                orphan_query = text(
                    f"""
                    SELECT COUNT(*)
                    FROM {table} s
                    LEFT JOIN {target_table} t ON s.{column} = t.{target_pk}
                    WHERE s.{column} IS NOT NULL AND t.{target_pk} IS NULL
                """
                )
                orphan_count = conn.execute(orphan_query).scalar() or 0

                if orphan_count > 0:
                    if verbose:
                        print(
                            f"        {column} -> {target_table}: 🔴 {orphan_count}/{total_count} ORPHANED!"
                        )

                    # Get sample orphaned values to help with backfill
                    sample_query = text(
                        f"""
                        SELECT DISTINCT s.{column}
                        FROM {table} s
                        LEFT JOIN {target_table} t ON s.{column} = t.{target_pk}
                        WHERE s.{column} IS NOT NULL AND t.{target_pk} IS NULL
                        LIMIT 5
                    """
                    )
                    sample_orphans = [
                        row[0] for row in conn.execute(sample_query).fetchall()
                    ]

                    return self._create_orphan_backfill_opportunity(
                        table,
                        column,
                        target_table,
                        target_pk,
                        orphan_count,
                        total_count,
                        sample_orphans,
                    )
                elif verbose:
                    print(
                        f"        {column} -> {target_table}: ✅ all relationships valid ({total_count} records)"
                    )

        except Exception as e:
            if verbose:
                print(f"        Error checking {column}: {e}")

        return None

    def _find_missing_relationships(
        self, table: str, columns: List[str], verbose: bool = False
    ) -> List[BackfillOpportunity]:
        """Find potential relationships that don't exist yet."""
        opportunities = []

        # Look for patterns like: songs table missing artist_id but has artist_name
        if table == "songs":
            if "artist_name" in [c.lower() for c in columns] and "artist_id" not in [
                c.lower() for c in columns
            ]:
                # Suggest creating artist_id relationship
                opportunity = self._suggest_name_to_id_relationship(
                    table, "artist_name", "artists", "id"
                )
                if opportunity:
                    opportunities.append(opportunity)

        return opportunities

    def _suggest_name_to_id_relationship(
        self, source_table: str, name_column: str, target_table: str, target_id: str
    ) -> Optional[BackfillOpportunity]:
        """Suggest creating ID relationships from name matches."""
        try:
            from sqlalchemy import text

            if not self._table_exists(target_table):
                return None

            with self.engine.connect() as conn:
                # Count potential matches
                match_query = text(
                    f"""
                    SELECT COUNT(DISTINCT s.{name_column})
                    FROM {source_table} s
                    INNER JOIN {target_table} t ON LOWER(s.{name_column}) = LOWER(t.name)
                    WHERE s.{name_column} IS NOT NULL
                """
                )

                try:
                    match_count = conn.execute(match_query).scalar() or 0

                    if match_count > 0:
                        total_query = text(
                            f"SELECT COUNT(*) FROM {source_table} WHERE {name_column} IS NOT NULL"
                        )
                        total_count = conn.execute(total_query).scalar() or 0

                        suggested_query = f"""
-- Add artist_id column and populate from name matches
ALTER TABLE {source_table} ADD COLUMN artist_id INTEGER;

UPDATE {source_table}
SET artist_id = (
    SELECT t.{target_id}
    FROM {target_table} t
    WHERE LOWER(t.name) = LOWER({source_table}.{name_column})
    LIMIT 1
)
WHERE {name_column} IS NOT NULL;
"""

                        return BackfillOpportunity(
                            source_table=source_table,
                            source_column=f"{name_column} -> new artist_id",
                            target_table=target_table,
                            target_column=target_id,
                            missing_count=match_count,
                            total_count=total_count,
                            confidence="medium",
                            suggested_query=suggested_query.strip(),
                            explanation=f"Can create {match_count} artist relationships by matching names",
                        )
                except:
                    pass

        except Exception:
            pass

        return None

    def _create_orphan_backfill_opportunity(
        self,
        source_table: str,
        source_column: str,
        target_table: str,
        target_column: str,
        orphan_count: int,
        total_count: int,
        sample_orphans: List,
    ) -> BackfillOpportunity:
        """Create a backfill opportunity for ORPHANED foreign keys - the real deal."""

        confidence = (
            "high"
            if orphan_count < total_count * 0.1
            else "medium"
            if orphan_count < total_count * 0.5
            else "low"
        )

        # Generate smart SQL suggestion for ORPHANED records
        suggested_query = self._generate_orphan_backfill_query(
            source_table, source_column, target_table, target_column, sample_orphans
        )

        explanation = self._generate_orphan_explanation(
            source_table,
            source_column,
            target_table,
            orphan_count,
            total_count,
            sample_orphans,
        )

        return BackfillOpportunity(
            source_table=source_table,
            source_column=source_column,
            target_table=target_table,
            target_column=target_column,
            missing_count=orphan_count,
            total_count=total_count,
            confidence=confidence,
            suggested_query=suggested_query,
            explanation=explanation,
        )

    def _generate_orphan_backfill_query(
        self,
        source_table: str,
        source_column: str,
        target_table: str,
        target_column: str,
        sample_orphans: List,
    ) -> str:
        """Generate intelligent backfill SQL for ORPHANED foreign keys."""

        orphan_list = (
            ", ".join([f"'{o}'" for o in sample_orphans[:5]])
            if sample_orphans
            else "N / A"
        )

        if "spotify" in source_column.lower():
            return f"""
-- ORPHANED SPOTIFY IDs FOUND: {orphan_list}

-- Option 1: Remove orphaned Spotify references (DESTRUCTIVE)
DELETE FROM {source_table}
WHERE {source_column} NOT IN (SELECT {target_column} FROM {target_table});

-- Option 2: Create missing Spotify records (RECOMMENDED)
INSERT INTO {target_table} ({target_column}, name, created_at)
SELECT DISTINCT s.{source_column},
       CONCAT('Missing Spotify Track: ', s.{source_column}),
       NOW()
FROM {source_table} s
LEFT JOIN {target_table} t ON s.{source_column} = t.{target_column}
WHERE s.{source_column} IS NOT NULL AND t.{target_column} IS NULL;

-- Option 3: Fetch real data from Spotify API
-- Use Spotify Web API to get track details for: {orphan_list}
"""

        elif "artist" in source_column.lower():
            return f"""
-- ORPHANED ARTIST IDs FOUND: {orphan_list}

-- Option 1: Create placeholder artist records
INSERT INTO {target_table} ({target_column}, name, created_at)
SELECT DISTINCT s.{source_column},
       CONCAT('Missing Artist ID: ', s.{source_column}),
       NOW()
FROM {source_table} s
LEFT JOIN {target_table} t ON s.{source_column} = t.{target_column}
WHERE s.{source_column} IS NOT NULL AND t.{target_column} IS NULL;

-- Option 2: Research and add real artist data
-- Missing artist IDs: {orphan_list}
-- Look these up in MusicBrainz, Discogs, or other music databases
"""

        elif "album" in source_column.lower():
            return f"""
-- ORPHANED ALBUM IDs FOUND: {orphan_list}

-- Option 1: Create placeholder album records
INSERT INTO {target_table} ({target_column}, title, created_at)
SELECT DISTINCT s.{source_column},
       CONCAT('Missing Album ID: ', s.{source_column}),
       NOW()
FROM {source_table} s
LEFT JOIN {target_table} t ON s.{source_column} = t.{target_column}
WHERE s.{source_column} IS NOT NULL AND t.{target_column} IS NULL;

-- Option 2: Match by song titles to find real albums
-- Check if songs with these album_ids have recognizable patterns
"""

        else:
            return f"""
-- ORPHANED {source_column.upper()} FOUND: {orphan_list}

-- Option 1: Create placeholder records
INSERT INTO {target_table} ({target_column}, created_at)
SELECT DISTINCT s.{source_column}, NOW()
FROM {source_table} s
LEFT JOIN {target_table} t ON s.{source_column} = t.{target_column}
WHERE s.{source_column} IS NOT NULL AND t.{target_column} IS NULL;

-- Option 2: Remove orphaned references (DESTRUCTIVE)
UPDATE {source_table}
SET {source_column} = NULL
WHERE {source_column} NOT IN (SELECT {target_column} FROM {target_table});
"""

    def _generate_orphan_explanation(
        self,
        source_table: str,
        source_column: str,
        target_table: str,
        orphan_count: int,
        total_count: int,
        sample_orphans: List,
    ) -> str:
        """Generate human - readable explanation for ORPHANED foreign keys."""
        percent = (orphan_count / total_count) * 100 if total_count > 0 else 0

        sample_text = (
            f"Examples: {', '.join(map(str, sample_orphans[:3]))}"
            if sample_orphans
            else ""
        )

        return f"""
🚨 ORPHANED FOREIGN KEYS DETECTED!
{orphan_count} records in {source_table}.{source_column} point to non - existent {target_table} records ({percent:.1f}%).
{sample_text}

This is a real data integrity issue caused by:
- Records deleted from {target_table} without updating references
- Data imported with invalid foreign key values
- ETL process creating references before target records exist
- Database constraints not properly enforced

This WILL cause application errors and should be fixed!
"""

    def _guess_target_table(self, column: str) -> Optional[str]:
        """Guess target table name from foreign key column."""
        column_lower = column.lower()

        mappings = {
            "artist_id": "artists",
            "album_id": "albums",
            "label_id": "labels",
            "spotify_id": "spotify_tracks",
            "youtube_id": "youtube_videos",
            "tidal_id": "tidal_tracks_raw",
        }

        if column_lower in mappings:
            return mappings[column_lower]

        # Generic pattern: remove _id and pluralize
        if column_lower.endswith("_id"):
            base = column_lower[:-3]
            return f"{base}s"

        return None

    def _guess_primary_key(self, table: str) -> str:
        """Guess primary key column name based on actual table structure."""
        # Check actual table structure
        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                result = conn.execute(text(f"DESCRIBE {table}"))
                for row in result:
                    column_name, data_type, null, key, default, extra = row
                    if key == "PRI":  # Primary key
                        return column_name
        except:
            pass

        # Fallback patterns
        if table == "albums":
            return "album_id"
        elif table == "artists":
            return "artist_id"
        elif table == "labels":
            return "label_id"
        elif table == "songs":
            return "song_id"
        else:
            return "id"  # Generic fallback

    def _table_exists(self, table: str) -> bool:
        """Check if table exists."""
        try:
            from sqlalchemy import text

            query = text(f"SELECT 1 FROM {table} LIMIT 1")
            with self.engine.connect() as conn:
                conn.execute(query)
                return True
        except:
            return False

    def print_interactive_report(self):
        """Print clean, actionable report to terminal."""
        if not self.opportunities:
            print("✅ No backfill opportunities found - your foreign keys look good!")
            return

        print("\n🎯" + "=" * 78 + "🎯")
        print("                    SMART BACKFILL SUGGESTIONS")
        print("🎯" + "=" * 78 + "🎯\n")

        print(f"Found {len(self.opportunities)} backfill opportunities:\n")

        for i, opp in enumerate(self.opportunities, 1):
            confidence_icon = (
                "🟢"
                if opp.confidence == "high"
                else "🟡"
                if opp.confidence == "medium"
                else "🔴"
            )

            print(
                f"{confidence_icon} {i}. {opp.source_table}.{opp.source_column} → {opp.target_table}"
            )
            print(
                f"   📊 {opp.missing_count:,} missing relationships ({opp.confidence} confidence)"
            )
            print(f"   💡 {opp.explanation.split('.')[0]}")
            print()

        # Interactive mode
        while True:
            try:
                choice = input(
                    "Enter number to see SQL suggestion (or 'q' to quit): "
                ).strip()

                if choice.lower() == "q":
                    break

                if choice.isdigit() and 1 <= int(choice) <= len(self.opportunities):
                    opp = self.opportunities[int(choice) - 1]
                    self._show_detailed_suggestion(opp)
                else:
                    print("Invalid choice. Try again.")

            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break

    def _show_detailed_suggestion(self, opp: BackfillOpportunity):
        """Show detailed suggestion for a specific opportunity."""
        print("\n" + "=" * 80)
        print(f"BACKFILL SUGGESTION: {opp.source_table}.{opp.source_column}")
        print("=" * 80)
        print(f"Target: {opp.target_table}.{opp.target_column}")
        print(f"Missing: {opp.missing_count:,} records")
        print(f"Confidence: {opp.confidence}")
        print()
        print("EXPLANATION:")
        print(opp.explanation)
        print()
        print("SUGGESTED SQL:")
        print("-" * 40)
        print(opp.suggested_query)
        print("-" * 40)
        print()

        save = input("Save this SQL to file? (y / N): ").strip().lower()
        if save == "y":
            filename = f"backfill_{opp.source_table}_{opp.source_column}.sql"
            with open(filename, "w") as f:
                f.write(
                    f"-- Backfill suggestion for {opp.source_table}.{opp.source_column}\n"
                )
                f.write(f"-- Generated: {__import__('datetime').datetime.now()}\n\n")
                f.write(opp.suggested_query)
            print(f"💾 Saved to {filename}")

        print()


def get_real_database_connection():
    """Get connection to actual production database."""
    try:
        # Load environment variables from .env file
        try:
            from dotenv import load_dotenv

            # Look for .env in project root (two levels up from this script)
            env_path = os.path.join(os.path.dirname(__file__), "../../.env")
            if os.path.exists(env_path):
                load_dotenv(env_path)
            else:
                # Try current directory
                if os.path.exists(".env"):
                    load_dotenv(".env")
        except ImportError:
            pass

        # Try to connect to PUBLIC schema (icatalog_public)
        engine = get_engine(schema="PUBLIC")

        # Test connection
        with engine.connect() as conn:
            from sqlalchemy import text

            result = conn.execute(text("SELECT 1 as test"))
            if result.scalar() == 1:
                return engine

        return None
    except Exception as e:
        print(f"⚠️  Could not connect to production database: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Smart Backfill Suggester")
    parser.add_argument("--interactive", action="store_true", help="Interactive mode")
    parser.add_argument("--table", help="Focus on specific table")
    parser.add_argument(
        "--save - suggestions",
        action="store_true",
        help="Save all suggestions to files",
    )
    parser.add_argument("--verbose", action="store_true", help="Show detailed analysis")

    args = parser.parse_args()

    # Get database connection
    engine = get_real_database_connection()
    if not engine:
        print("❌ Could not connect to database")
        print("💡 Make sure .env file has DB_PASS and other credentials")
        sys.exit(1)

    print("✅ Connected to production database")

    # Create suggester and scan
    suggester = SmartBackfillSuggester(engine)
    opportunities = suggester.scan_for_opportunities(args.table, args.verbose)

    if args.interactive:
        suggester.print_interactive_report()
    else:
        # Just print summary
        if opportunities:
            print(f"\n🎯 Found {len(opportunities)} backfill opportunities")
            for opp in opportunities[:5]:  # Show top 5
                print(
                    f"   • {opp.source_table}.{opp.source_column}: {opp.missing_count:,} missing"
                )

            if len(opportunities) > 5:
                print(f"   ... and {len(opportunities) - 5} more")

            print("\n💡 Run with --interactive to see detailed suggestions")
        else:
            print("✅ No backfill opportunities found")

    if args.save_suggestions and opportunities:
        for opp in opportunities:
            filename = f"backfill_{opp.source_table}_{opp.source_column}.sql"
            with open(filename, "w") as f:
                f.write(
                    f"-- Backfill suggestion for {opp.source_table}.{opp.source_column}\n"
                )
                f.write(f"-- Generated: {__import__('datetime').datetime.now()}\n\n")
                f.write(opp.suggested_query)
        print(f"💾 Saved {len(opportunities)} SQL files")


if __name__ == "__main__":
    main()
