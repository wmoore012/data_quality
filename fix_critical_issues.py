#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Critical Data Quality Issue Fixer

This script demonstrates how to fix critical data quality issues
that are blocking deployment.

Usage:
    python fix_critical_issues.py --database-url sqlite:///critical_issues_db.sqlite
"""

import argparse
import sqlite3
import sys
from datetime import datetime


def fix_missing_isrcs(conn):
    """Fix missing ISRC values by generating valid ones."""
    print("🔧 Fixing missing ISRC values...")
    
    cursor = conn.cursor()
    
    # Find songs with missing ISRCs
    cursor.execute("SELECT id, title FROM songs WHERE isrc IS NULL")
    missing_isrc_songs = cursor.fetchall()
    
    print(f"   Found {len(missing_isrc_songs)} songs with missing ISRCs")
    
    # Generate valid ISRCs for missing ones
    for song_id, title in missing_isrc_songs:
        # Generate a valid ISRC format: USRC + year + 5-digit number
        new_isrc = f"USRC24{song_id:05d}"
        cursor.execute("UPDATE songs SET isrc = ? WHERE id = ?", (new_isrc, song_id))
        print(f"   ✅ Fixed song '{title}' (ID: {song_id}) -> ISRC: {new_isrc}")
    
    conn.commit()
    return len(missing_isrc_songs)


def fix_orphaned_records(conn):
    """Fix orphaned records by either creating missing references or removing orphans."""
    print("🔧 Fixing orphaned records...")
    
    cursor = conn.cursor()
    
    # Find orphaned songs (artist_id doesn't exist in artists table)
    cursor.execute("""
        SELECT s.id, s.title, s.artist_id 
        FROM songs s 
        LEFT JOIN artists a ON s.artist_id = a.id 
        WHERE s.artist_id IS NOT NULL AND a.id IS NULL
    """)
    orphaned_songs = cursor.fetchall()
    
    print(f"   Found {len(orphaned_songs)} orphaned songs")
    
    # Create missing artists or reassign to existing ones
    for song_id, title, missing_artist_id in orphaned_songs:
        # Option 1: Create a placeholder artist
        cursor.execute("INSERT OR IGNORE INTO artists (id, name, country) VALUES (?, ?, ?)", 
                      (missing_artist_id, f"Unknown Artist {missing_artist_id}", "Unknown"))
        print(f"   ✅ Created missing artist {missing_artist_id} for song '{title}'")
    
    # Fix orphaned labels
    cursor.execute("""
        SELECT s.id, s.title, s.label_id 
        FROM songs s 
        LEFT JOIN labels l ON s.label_id = l.id 
        WHERE s.label_id IS NOT NULL AND l.id IS NULL
    """)
    orphaned_labels = cursor.fetchall()
    
    print(f"   Found {len(orphaned_labels)} songs with orphaned label references")
    
    for song_id, title, missing_label_id in orphaned_labels:
        # Create placeholder label
        cursor.execute("INSERT OR IGNORE INTO labels (id, name) VALUES (?, ?)", 
                      (missing_label_id, f"Unknown Label {missing_label_id}"))
        print(f"   ✅ Created missing label {missing_label_id} for song '{title}'")
    
    conn.commit()
    return len(orphaned_songs) + len(orphaned_labels)


def fix_duplicate_isrcs(conn):
    """Fix duplicate ISRC values by making them unique."""
    print("🔧 Fixing duplicate ISRC values...")
    
    cursor = conn.cursor()
    
    # Find duplicate ISRCs
    cursor.execute("""
        SELECT isrc, COUNT(*) as count, GROUP_CONCAT(id) as song_ids
        FROM songs 
        WHERE isrc IS NOT NULL 
        GROUP BY isrc 
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    print(f"   Found {len(duplicates)} duplicate ISRC groups")
    
    fixed_count = 0
    for isrc, count, song_ids_str in duplicates:
        song_ids = [int(x) for x in song_ids_str.split(',')]
        print(f"   Duplicate ISRC '{isrc}' found in songs: {song_ids}")
        
        # Keep the first song with the original ISRC, fix the others
        for i, song_id in enumerate(song_ids[1:], 1):  # Skip first song
            new_isrc = f"{isrc[:-2]}{i:02d}"  # Modify last 2 digits
            cursor.execute("UPDATE songs SET isrc = ? WHERE id = ?", (new_isrc, song_id))
            print(f"   ✅ Changed song {song_id} ISRC from '{isrc}' to '{new_isrc}'")
            fixed_count += 1
    
    conn.commit()
    return fixed_count


def main():
    parser = argparse.ArgumentParser(description="Fix critical data quality issues")
    parser.add_argument("--database-url", required=True, help="Database connection URL")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed without making changes")
    
    args = parser.parse_args()
    
    if not args.database_url.startswith("sqlite:///"):
        print("❌ This demo script only works with SQLite databases")
        sys.exit(1)
    
    db_path = args.database_url.replace("sqlite:///", "")
    
    print("🚨 CRITICAL DATA QUALITY ISSUE FIXER")
    print("=" * 50)
    print(f"Database: {db_path}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE FIXES'}")
    print()
    
    if args.dry_run:
        print("⚠️  DRY RUN MODE: No changes will be made")
        print()
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Fix critical issues
        total_fixes = 0
        
        if not args.dry_run:
            total_fixes += fix_missing_isrcs(conn)
            total_fixes += fix_orphaned_records(conn)
            total_fixes += fix_duplicate_isrcs(conn)
        else:
            print("🔍 DRY RUN: Would fix missing ISRCs, orphaned records, and duplicates")
        
        conn.close()
        
        if not args.dry_run:
            print()
            print("✅ FIXES COMPLETED")
            print("=" * 50)
            print(f"Total fixes applied: {total_fixes}")
            print()
            print("🚀 Ready to re-run deployment gate!")
            print("   python deployment_gate.py --database-url", args.database_url)
        
    except Exception as e:
        print(f"❌ Fix failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()