"""Schema discovery MCP tools.

Provides tools for exploring database structure: tables, columns, types,
sample values, foreign keys, and indexes.
"""

import logging
from typing import Any

from psycopg import sql

from ..database import execute_query

logger = logging.getLogger(__name__)


def describe_database() -> list[dict[str, Any]]:
    """
    List all tables in the database with metadata.

    Returns:
        List of table info dicts with:
        - table_name: Name of the table
        - column_count: Number of columns
        - row_count: Approximate row count
        - description: Table comment/description if available
        - size_bytes: Table size in bytes
    """
    sql = """
        SELECT
            t.table_name,
            (SELECT COUNT(*)
             FROM information_schema.columns c
             WHERE c.table_name = t.table_name
               AND c.table_schema = 'public') as column_count,
            COALESCE(s.n_live_tup, 0) as row_count,
            obj_description(
                (t.table_schema || '.' || t.table_name)::regclass, 'pg_class') as description,
            pg_total_relation_size((t.table_schema || '.' || t.table_name)::regclass) as size_bytes
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s
            ON t.table_name = s.relname AND t.table_schema = s.schemaname
        WHERE t.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    """
    return execute_query(sql)


def describe_table(table_name: str) -> dict[str, Any]:
    """
    Get detailed information about a specific table.

    Args:
        table_name: Name of the table to describe

    Returns:
        Dict with:
        - table_name: Name of the table
        - description: Table comment if available
        - row_count: Approximate row count
        - columns: List of column dicts (name, type, nullable, default, description)
        - sample_values: Dict mapping column name to list of sample values
        - foreign_keys: List of FK dicts (column, references_table, references_column)
        - indexes: List of index dicts (name, columns, is_unique, type)
        - primary_key: List of primary key column names
    """
    # Validate table exists
    check_sql = """
        SELECT COUNT(*) as cnt
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %(table_name)s
    """
    result = execute_query(check_sql, {"table_name": table_name})
    if not result or result[0]["cnt"] == 0:
        raise ValueError(f"Table '{table_name}' not found in public schema")

    # Build safe table reference for regclass casts
    table_ref = sql.SQL("{}.{}").format(
        sql.Identifier("public"), sql.Identifier(table_name)
    )

    # Get table description and row count
    meta_query = sql.SQL("""
        SELECT
            obj_description({table_ref}::regclass, 'pg_class') as description,
            COALESCE(n_live_tup, 0) as row_count
        FROM pg_stat_user_tables
        WHERE relname = %(table_name)s AND schemaname = 'public'
    """).format(table_ref=table_ref)
    meta = execute_query(meta_query, {"table_name": table_name})
    table_desc = meta[0]["description"] if meta else None
    row_count = meta[0]["row_count"] if meta else 0

    # Get columns
    columns_query = sql.SQL("""
        SELECT
            c.column_name as name,
            c.data_type as type,
            c.is_nullable = 'YES' as nullable,
            c.column_default as default_value,
            col_description({table_ref}::regclass, c.ordinal_position) as description
        FROM information_schema.columns c
        WHERE c.table_schema = 'public' AND c.table_name = %(table_name)s
        ORDER BY c.ordinal_position
    """).format(table_ref=table_ref)
    columns = execute_query(columns_query, {"table_name": table_name})

    # Get sample values (first 3 distinct non-null values per column)
    sample_values = {}
    for col in columns[:20]:  # Limit to first 20 columns to avoid huge queries
        col_name = col["name"]
        try:
            # Use sql.Identifier for safe column/table name quoting
            sample_query = sql.SQL("""
                SELECT DISTINCT {col}::text as val
                FROM {table}
                WHERE {col} IS NOT NULL
                LIMIT 3
            """).format(
                col=sql.Identifier(col_name),
                table=sql.Identifier(table_name),
            )
            samples = execute_query(sample_query)
            sample_values[col_name] = [s["val"] for s in samples]
        except Exception:
            sample_values[col_name] = []

    # Get foreign keys
    fk_sql = """
        SELECT
            kcu.column_name,
            ccu.table_name as references_table,
            ccu.column_name as references_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = %(table_name)s
    """
    foreign_keys = execute_query(fk_sql, {"table_name": table_name})

    # Get indexes
    idx_sql = """
        SELECT
            i.relname as name,
            array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
            ix.indisunique as is_unique,
            am.amname as type
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = %(table_name)s
          AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        GROUP BY i.relname, ix.indisunique, am.amname
        ORDER BY i.relname
    """
    indexes = execute_query(idx_sql, {"table_name": table_name})

    # Get primary key columns
    pk_sql = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = %(table_name)s
        ORDER BY kcu.ordinal_position
    """
    pk_result = execute_query(pk_sql, {"table_name": table_name})
    primary_key = [r["column_name"] for r in pk_result]

    return {
        "table_name": table_name,
        "description": table_desc,
        "row_count": row_count,
        "columns": columns,
        "sample_values": sample_values,
        "foreign_keys": foreign_keys,
        "indexes": indexes,
        "primary_key": primary_key,
    }
