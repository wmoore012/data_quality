# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

from data_quality import quick_null_scan
from sqlalchemy import create_engine, text


def test_quick_null_scan_counts_nulls():
    eng = create_engine("sqlite+pysqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE songs (id INTEGER PRIMARY KEY, isrc TEXT, artist_id INTEGER)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO songs (id, isrc, artist_id) VALUES (1, NULL, 10), (2, 'USRC17607839', NULL)"
            )
        )
    report = quick_null_scan(eng, table_patterns=["songs"], key_like=["id", "isrc"])
    assert report == {"songs": {"isrc": 1, "artist_id": 1}}
