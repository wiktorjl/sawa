#!/usr/bin/env python3
"""Recreate v_company_with_indices view with nasdaq5000."""
import os
import psycopg

VIEW_SQL = """
CREATE OR REPLACE VIEW v_company_with_indices AS
SELECT
    c.ticker,
    c.name,
    c.market_cap,
    c.sic_description as sector,
    c.primary_exchange as exchange,
    c.active,
    COALESCE(
        (SELECT array_agg(i.code ORDER BY i.name)
         FROM index_constituents ic
         JOIN indices i ON ic.index_id = i.id
         WHERE ic.ticker = c.ticker),
        ARRAY[]::varchar[]
    ) as indices,
    (EXISTS (
        SELECT 1 FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        WHERE ic.ticker = c.ticker AND i.code = 'sp500'
    )) as in_sp500,
    (EXISTS (
        SELECT 1 FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        WHERE ic.ticker = c.ticker AND i.code = 'nasdaq5000'
    )) as in_nasdaq5000
FROM companies c
"""

db_url = os.environ["DATABASE_URL"]
conn = psycopg.connect(db_url, autocommit=True)
conn.execute("DROP VIEW IF EXISTS v_company_with_indices")
conn.execute(VIEW_SQL)
print("View v_company_with_indices recreated with nasdaq5000")
conn.close()
