# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Safe database scanners using SQLAlchemy reflection and Inspector API.

This module provides SQL injection-safe database quality checks using
SQLAlchemy's Inspector and reflection capabilities instead of raw SQL.
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy import create_engine, MetaData, Table, select, func, and_
from sqlalchemy.engine import Engine
from sqlalchemy import inspect


@dataclass(frozen=True)
class ForeignKeyIssue:
    table: str
    fk_name: str | None
    constrained: Tuple[str, ...]
    referred_table: str
    missing_count: int
    sample_sql: str


@contextmanager
def engine_ctx(url: str) -> None:
    eng = create_engine(url, pool_pre_ping=True)
    try:
        yield eng
    finally:
        eng.dispose()


def find_orphans(db_url: str, schema: str | None = None, limit: int = 1000) -> List[ForeignKeyIssue]:
    """
    Find orphaned records using SQLAlchemy reflection (SQL injection safe).
    
    Args:
        db_url: Database connection URL
        schema: Optional schema name
        limit: Limit for sample queries
        
    Returns:
        List of foreign key issues with orphaned records
    """
    with engine_ctx(db_url) as engine:
        insp = inspect(engine)
        md = MetaData()
        out: List[ForeignKeyIssue] = []

        for child_name in insp.get_table_names(schema=schema):
            for fk in insp.get_foreign_keys(child_name, schema=schema):
                parent_name = fk["referred_table"]
                child = Table(child_name, md, autoload_with=engine, schema=schema)
                parent = Table(parent_name, md, autoload_with=engine, schema=schema)

                cons_cols = fk["constrained_columns"]
                ref_cols = fk["referred_columns"] or tuple(insp.get_pk_constraint(parent_name, schema=schema)["constrained_columns"])

                join_cond = and_(*[child.c[c] == parent.c[r] for c, r in zip(cons_cols, ref_cols)])
                missing_parent = and_(*[parent.c[r].is_(None) for r in ref_cols])

                stmt = (
                    select(func.count())
                    .select_from(child.outerjoin(parent, join_cond))
                    .where(missing_parent)
                )

                count = engine.execute(stmt).scalar_one()
                if count:
                    out.append(
                        ForeignKeyIssue(
                            table=child_name,
                            fk_name=fk.get("name"),
                            constrained=tuple(cons_cols),
                            referred_table=parent_name,
                            missing_count=int(count),
                            sample_sql=str(stmt.limit(limit))
                        )
                    )
        return out


def find_nulls_safe(db_url: str, schema: str | None = None) -> List[Dict]:
    """
    Find null values using SQLAlchemy reflection (SQL injection safe).
    
    Args:
        db_url: Database connection URL
        schema: Optional schema name
        
    Returns:
        List of null count issues
    """
    with engine_ctx(db_url) as engine:
        insp = inspect(engine)
        md = MetaData()
        issues = []

        for table_name in insp.get_table_names(schema=schema):
            table = Table(table_name, md, autoload_with=engine, schema=schema)
            
            for column in table.columns:
                # Skip if column allows nulls by design
                if column.nullable:
                    continue
                    
                # Count nulls using safe SQLAlchemy query
                null_stmt = select(func.count()).where(column.is_(None))
                total_stmt = select(func.count()).select_from(table)
                
                with engine.begin() as conn:
                    null_count = conn.execute(null_stmt).scalar_one()
                    total_count = conn.execute(total_stmt).scalar_one()
                    
                    if null_count > 0:
                        issues.append({
                            'table': table_name,
                            'column': column.name,
                            'null_count': null_count,
                            'total_count': total_count,
                            'percent': (null_count / total_count * 100) if total_count > 0 else 0
                        })
        
        return issues


def find_duplicates_safe(db_url: str, schema: str | None = None) -> List[Dict]:
    """
    Find duplicate records using SQLAlchemy reflection (SQL injection safe).
    
    Args:
        db_url: Database connection URL
        schema: Optional schema name
        
    Returns:
        List of duplicate issues
    """
    with engine_ctx(db_url) as engine:
        insp = inspect(engine)
        md = MetaData()
        issues = []

        for table_name in insp.get_table_names(schema=schema):
            table = Table(table_name, md, autoload_with=engine, schema=schema)
            
            # Look for unique constraints to check for violations
            unique_constraints = insp.get_unique_constraints(table_name, schema=schema)
            
            for constraint in unique_constraints:
                cols = [table.c[col_name] for col_name in constraint['column_names']]
                
                # Count duplicates using safe SQLAlchemy query
                stmt = (
                    select(func.count())
                    .select_from(
                        select(*cols, func.count().label('cnt'))
                        .group_by(*cols)
                        .having(func.count() > 1)
                        .subquery()
                    )
                )
                
                with engine.begin() as conn:
                    duplicate_groups = conn.execute(stmt).scalar_one()
                    
                    if duplicate_groups > 0:
                        issues.append({
                            'table': table_name,
                            'columns': constraint['column_names'],
                            'constraint_name': constraint.get('name'),
                            'duplicate_groups': duplicate_groups
                        })
        
        return issues