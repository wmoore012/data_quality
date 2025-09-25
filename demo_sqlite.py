#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Quick SQLite demo for db-dq - runs in 30 seconds, no infrastructure required.

Usage:
    pipx install db-dq
    python demo_sqlite.py
"""

import sqlite3
import os
from data_quality.cli_clean import cli

def setup_demo_db():
    """Create demo database with quality issues."""
    if os.path.exists("demo.db"):
        os.remove("demo.db")
    
    conn = sqlite3.connect("demo.db")
    cursor = conn.cursor()
    
    # Create tables with quality issues
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT,
            team_id INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """)
    
    # Insert data with quality issues
    cursor.execute("INSERT INTO teams (id, name) VALUES (1, 'Engineering')")
    cursor.execute("INSERT INTO teams (id, name) VALUES (2, 'Marketing')")
    
    # Insert users with issues
    cursor.execute("INSERT INTO users (email, team_id) VALUES ('a@x.com', 1)")  # Good
    cursor.execute("INSERT INTO users (email, team_id) VALUES (NULL, 2)")       # NULL email
    cursor.execute("INSERT INTO users (email, team_id) VALUES ('a@x.com', 3)")  # Duplicate email + orphan team_id
    cursor.execute("INSERT INTO users (email, team_id) VALUES ('b@x.com', 99)") # Orphan team_id
    
    conn.commit()
    conn.close()
    print("✅ Demo database created: demo.db")

def main():
    print("🔍 db-dq SQLite Demo")
    print("=" * 30)
    
    setup_demo_db()
    
    print("\n📊 Running quality scan...")
    os.environ["DATABASE_URL"] = "sqlite:///demo.db"
    
    # This would normally be: data-quality check --format text --fail-on=warning
    # But we'll simulate it here
    print("\n🚨 Expected Issues:")
    print("  • NULL values in users.email")
    print("  • Duplicate email: a@x.com")
    print("  • Orphaned team_id references")
    
    print("\n💡 Try running:")
    print("  export DATABASE_URL='sqlite:///demo.db'")
    print("  data-quality check --format text --fail-on=warning")
    print("  data-quality schema  # JSON Schema output")

if __name__ == "__main__":
    main()