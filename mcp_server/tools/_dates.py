"""Shared reference-date lookups for price-based MCP tools.

Tools used to derive reference trading dates inside each query by
aggregating over the stock_prices_live view (``MAX(date) FILTER ...``
CTEs) and joining the view on those values. Join keys produced at run
time can never be pushed down into the view's UNION ALL arms, so every
such reference materialized the full multi-million-row view and the
market-wide tools exceeded the 30s statement timeout.

These helpers compute the same dates with index-backed lookups in a few
milliseconds. Tools pass the results as bind parameters, which the
planner pushes into each arm of stock_prices_live as index conditions.
"""

from datetime import date
from typing import Any

from ..database import execute_query

# Matches the view's notion of "latest": the newest EOD date, or today's
# America/New_York market date once in-session intraday bars exist.
_PRICE_DATE_REFS_QUERY = """
    WITH market_clock AS (
        SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date AS today
    ),
    latest_ref AS (
        SELECT GREATEST(
            (SELECT MAX(date) FROM stock_prices),
            (SELECT mc.today FROM market_clock mc
             WHERE EXISTS (
                SELECT 1 FROM stock_prices_intraday spi
                WHERE (spi.timestamp AT TIME ZONE 'America/New_York')::date = mc.today
                  AND (spi.timestamp AT TIME ZONE 'America/New_York')::time >= TIME '09:30:00'
                  AND (spi.timestamp AT TIME ZONE 'America/New_York')::time < TIME '16:00:00'
             ))
        ) AS latest
    )
    SELECT
        lr.latest,
        (SELECT MAX(date) FROM stock_prices WHERE date < lr.latest) AS prev_day,
        (SELECT MAX(date) FROM stock_prices
         WHERE date <= mc.today - INTERVAL '7 days') AS week_ago,
        (SELECT MAX(date) FROM stock_prices
         WHERE date <= mc.today - INTERVAL '30 days') AS month_ago,
        (SELECT MIN(date) FROM stock_prices
         WHERE date >= DATE_TRUNC('year', mc.today)) AS ytd_start
    FROM latest_ref lr, market_clock mc
"""

_EOD_DATE_REFS_QUERY = """
    SELECT
        (SELECT MAX(date) FROM stock_prices) AS latest,
        (SELECT MAX(date) FROM stock_prices
         WHERE date < (SELECT MAX(date) FROM stock_prices)) AS prev_day
"""


def get_price_date_refs() -> dict[str, date | None]:
    """Reference dates aligned with stock_prices_live (includes today intraday).

    Returns:
        Dict with keys latest, prev_day, week_ago, month_ago, ytd_start.
        Values are None when no data covers the period.
    """
    rows: list[dict[str, Any]] = execute_query(_PRICE_DATE_REFS_QUERY)
    if not rows:
        return {
            "latest": None,
            "prev_day": None,
            "week_ago": None,
            "month_ago": None,
            "ytd_start": None,
        }
    return rows[0]


def get_eod_date_refs() -> dict[str, date | None]:
    """Latest and previous completed-session (EOD) dates from stock_prices.

    Returns:
        Dict with keys latest and prev_day; values are None on empty data.
    """
    rows: list[dict[str, Any]] = execute_query(_EOD_DATE_REFS_QUERY)
    if not rows:
        return {"latest": None, "prev_day": None}
    return rows[0]
