"""Stock performance scanner MCP tool."""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def scan_ytd_performance_async(
    start_date: str | None = None,
    large_cap_threshold: float = 100.0,
    top_n: int = 20,
    bottom_n: int = 20,
    index: str = "sp500",
) -> dict[str, Any]:
    """
    Scan market indices for YTD performance (async wrapper for sawa).

    Args:
        start_date: Start date (defaults to Jan 1 current year)
        large_cap_threshold: Market cap threshold in billions
        top_n: Number of top gainers
        bottom_n: Number of bottom losers
        index: Index to scan ("sp500", "nasdaq100", or "both")

    Returns:
        Formatted results with sector grouping
    """
    from sawa import scan_ytd_performance as sawa_scan_ytd

    try:
        results = await sawa_scan_ytd(
            index=index,
            start_date=start_date,
            large_cap_threshold=large_cap_threshold,
            top_n=top_n,
            bottom_n=bottom_n,
            concurrency=10,
        )

        # Add MCP-specific formatting or visualization here if needed
        return results

    except Exception as e:
        logger.error(f"Scanner error: {e}")
        return {"error": str(e)}


def scan_ytd_performance(
    start_date: str | None = None,
    large_cap_threshold: float = 100.0,
    top_n: int = 20,
    bottom_n: int = 20,
    index: str = "sp500",
) -> dict[str, Any]:
    """Sync wrapper for scan_ytd_performance_async."""
    return asyncio.run(
        scan_ytd_performance_async(
            start_date=start_date,
            large_cap_threshold=large_cap_threshold,
            top_n=top_n,
            bottom_n=bottom_n,
            index=index,
        )
    )
