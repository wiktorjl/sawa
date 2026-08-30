"""Bounded, database-backed market performance scanner."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

from ..database import execute_query

_SUPPORTED_INDICES = {"sp500", "nasdaq_listed", "us_active", "both"}
_MAX_TAIL_SIZE = 50

_PERFORMANCE_CTE = """
WITH universe AS (
    SELECT DISTINCT
        c.ticker,
        c.name,
        c.market_cap,
        get_gics_sector(c.ticker, c.sic_code, c.sic_description) AS sector
    FROM companies c
    JOIN index_constituents ic ON ic.ticker = c.ticker
    JOIN indices i ON i.id = ic.index_id
    WHERE c.active = TRUE
      AND i.code = ANY(%(index_codes)s)
),
performance AS (
    SELECT
        u.ticker,
        u.name,
        COALESCE(u.sector, 'Unknown') AS sector,
        ROUND((COALESCE(u.market_cap, 0) / 1000000000.0)::numeric, 2)
            AS market_cap_billions,
        p_start.close AS start_price,
        p_end.close AS end_price,
        ROUND(
            (((p_end.close - p_start.close) / p_start.close) * 100)::numeric,
            2
        ) AS change_percent,
        COALESCE(u.market_cap, 0) >= (%(large_cap_threshold)s * 1000000000.0)
            AS is_large_cap
    FROM universe u
    JOIN LATERAL (
        SELECT sp.date, sp.close
        FROM stock_prices sp
        WHERE sp.ticker = u.ticker
          AND sp.date >= %(start_date)s
          AND sp.date <= %(end_date)s
          AND sp.close > 0
        ORDER BY sp.date ASC
        LIMIT 1
    ) p_start ON TRUE
    JOIN LATERAL (
        SELECT sp.date, sp.close
        FROM stock_prices sp
        WHERE sp.ticker = u.ticker
          AND sp.date >= %(start_date)s
          AND sp.date <= %(end_date)s
          AND sp.close > 0
        ORDER BY sp.date DESC
        LIMIT 1
    ) p_end ON p_end.date > p_start.date
)
"""

_SUMMARY_QUERY = (
    _PERFORMANCE_CTE
    + """
SELECT
    (SELECT COUNT(*) FROM universe) AS total_symbols,
    COUNT(*) AS successful
FROM performance
"""
)

_TAIL_QUERY = (
    _PERFORMANCE_CTE
    + """,
ranked AS (
    SELECT
        performance.*,
        ROW_NUMBER() OVER (
            PARTITION BY is_large_cap
            ORDER BY change_percent DESC, ticker ASC
        ) AS gain_rank,
        ROW_NUMBER() OVER (
            PARTITION BY is_large_cap
            ORDER BY change_percent ASC, ticker ASC
        ) AS loss_rank
    FROM performance
)
SELECT *
FROM ranked
WHERE gain_rank <= %(top_n)s OR loss_rank <= %(bottom_n)s
ORDER BY is_large_cap DESC, change_percent DESC, ticker ASC
"""
)

_SECTOR_QUERY = (
    _PERFORMANCE_CTE
    + """
SELECT
    is_large_cap,
    sector,
    COUNT(*) AS count,
    ROUND(AVG(change_percent)::numeric, 2) AS average_change_percent
FROM performance
GROUP BY is_large_cap, sector
ORDER BY is_large_cap DESC, sector ASC
"""
)


def _scan_ytd_performance_local(
    *,
    index: str,
    start_date: str | None,
    large_cap_threshold: float,
    top_n: int,
    bottom_n: int,
) -> dict[str, Any]:
    """Compute a full-universe scan in bounded SQL result sets."""
    normalized_index = index.lower()
    if normalized_index not in _SUPPORTED_INDICES:
        raise ValueError(f"Unsupported scanner index: {index}")

    end = date.today()
    start = date.fromisoformat(start_date) if start_date else date(end.year, 1, 1)
    if start > end:
        raise ValueError("start_date cannot be after today")
    if large_cap_threshold < 0:
        raise ValueError("large_cap_threshold cannot be negative")

    top_n = max(1, min(int(top_n), _MAX_TAIL_SIZE))
    bottom_n = max(1, min(int(bottom_n), _MAX_TAIL_SIZE))
    index_codes = (
        ["sp500", "nasdaq_listed"]
        if normalized_index == "both"
        else [normalized_index]
    )
    params: dict[str, Any] = {
        "index_codes": index_codes,
        "start_date": start,
        "end_date": end,
        "large_cap_threshold": large_cap_threshold,
        "top_n": top_n,
        "bottom_n": bottom_n,
    }

    summary_rows = execute_query(_SUMMARY_QUERY, params)
    summary = summary_rows[0] if summary_rows else {}
    total_symbols = int(summary.get("total_symbols") or 0)
    successful = int(summary.get("successful") or 0)
    if total_symbols == 0:
        return {
            "error": f"No stored constituents found for index '{normalized_index}'",
            "index": normalized_index,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        }
    if successful == 0:
        return {
            "error": "No constituents had two stored prices in the requested window",
            "index": normalized_index,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "total_symbols": total_symbols,
        }

    tail_rows = execute_query(_TAIL_QUERY, params)
    sector_rows = execute_query(_SECTOR_QUERY, params)

    def stock_row(row: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in row.items()
            if key not in {"gain_rank", "loss_rank"}
        }

    def cap_group(is_large_cap: bool) -> dict[str, Any]:
        matching = [
            row for row in tail_rows if bool(row.get("is_large_cap")) is is_large_cap
        ]
        gainers = sorted(
            (
                row
                for row in matching
                if int(row.get("gain_rank") or top_n + 1) <= top_n
            ),
            key=lambda row: int(row["gain_rank"]),
        )
        losers = sorted(
            (
                row
                for row in matching
                if int(row.get("loss_rank") or bottom_n + 1) <= bottom_n
            ),
            key=lambda row: int(row["loss_rank"]),
        )
        sector_summary = {
            str(row["sector"]): {
                "count": int(row["count"]),
                "average_change_percent": row["average_change_percent"],
            }
            for row in sector_rows
            if bool(row.get("is_large_cap")) is is_large_cap
        }
        # Keep the historical ``by_sector: sector -> stock[]`` shape for
        # clients, but only publish the already-bounded tail rows.  The
        # aggregate ``sector_summary`` carries full-universe counts/averages.
        by_sector: dict[str, list[dict[str, Any]]] = {
            sector: [] for sector in sector_summary
        }
        seen_by_sector: dict[str, set[str]] = {
            sector: set() for sector in sector_summary
        }
        for row in (*gainers, *losers):
            sector = str(row["sector"])
            ticker = str(row["ticker"])
            by_sector.setdefault(sector, [])
            seen_by_sector.setdefault(sector, set())
            if ticker not in seen_by_sector[sector]:
                by_sector[sector].append(stock_row(row))
                seen_by_sector[sector].add(ticker)
        return {
            "count": sum(item["count"] for item in sector_summary.values()),
            "top_gainers": [stock_row(row) for row in gainers],
            "top_losers": [stock_row(row) for row in losers],
            "by_sector": by_sector,
            "by_sector_truncated": True,
            "sector_summary": sector_summary,
        }

    return {
        "index": normalized_index,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "total_symbols": total_symbols,
        "successful": successful,
        "failed": max(total_symbols - successful, 0),
        "large_cap": cap_group(True),
        "small_mid_cap": cap_group(False),
        "errors": [],
        "source": "database",
        "data_schema_version": "scan_ytd_performance.v2",
    }


async def scan_ytd_performance_async(
    *,
    start_date: str | None = None,
    large_cap_threshold: float = 100.0,
    top_n: int = 20,
    bottom_n: int = 20,
    index: str = "sp500",
) -> dict[str, Any]:
    """Run the bounded local scan off the MCP event loop."""
    return await asyncio.to_thread(
        _scan_ytd_performance_local,
        index=index,
        start_date=start_date,
        large_cap_threshold=large_cap_threshold,
        top_n=top_n,
        bottom_n=bottom_n,
    )
