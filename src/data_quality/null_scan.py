# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

from __future__ import annotations

from typing import Iterable, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


def quick_null_scan(
    engine: Engine,
    table_patterns: Optional[Optional[Iterable[str]]] = None,
    key_like: Optional[Iterable[str]] = ("id", "_id", "isrc"),
) -> dict[str, dict[str, int]]:
    """
    Return {table: {column: null_count}} for columns whose names hint at keys.

    - Uses parameterized SQL only.
    - Keeps queries small and explainable.
    """
    patterns = list(table_patterns or [])
    like_terms = list(key_like or [])

    results: dict[str, dict[str, int]] = {}

    # Find candidate tables
    # Works on MySQL and SQLite (fallback by scanning sqlite_master)
    try:
        tables = (
            _list_tables_mysql(engine, patterns)
            if patterns
            else _list_tables_generic(engine)
        )
    except Exception:
        tables = _list_tables_generic(engine)

    for t in tables:
        cols = _list_columns(engine, t)
        keyish = [c for c in cols if any(term in c for term in like_terms)]
        if not keyish:
            continue
        nulls: dict[str, int] = {}
        for c in keyish:
            q = text(
                f"SELECT COUNT(*) AS n FROM {t} WHERE {c} IS NULL"
            )  # table/col are trusted from metadata
            with engine.begin() as conn:
                n = conn.execute(q).scalar_one()
            if n:
                nulls[c] = int(n)
        if nulls:
            results[t] = nulls
    return results


def _list_tables_mysql(engine: Engine, patterns: list[str]) -> list[str]:
    q = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND (
            """
        + " OR ".join(["table_name LIKE :p" + str(i) for i in range(len(patterns))])
        + ")"
    )
    params = {("p" + str(i)): pat for i, pat in enumerate(patterns)}
    with engine.begin() as conn:
        return [r[0] for r in conn.execute(q, params)]


def _list_tables_generic(engine: Engine) -> list[str]:
    # SQLite: read sqlite_master
    try:
        q = text("SELECT name FROM sqlite_master WHERE type='table'")
        with engine.begin() as conn:
            return [r[0] for r in conn.execute(q)]
    except Exception:
        return []


def _list_columns(engine: Engine, table: str) -> list[str]:
    # Try pragma (SQLite), else information_schema (MySQL)
    try:
        q = text(f"PRAGMA table_info({table})")
        with engine.begin() as conn:
            return [row[1] for row in conn.execute(q)]
    except Exception:
        q = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = :t
            ORDER BY ordinal_position
            """
        )
        with engine.begin() as conn:
            return [r[0] for r in conn.execute(q, {"t": table})]
