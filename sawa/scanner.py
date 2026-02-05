"""
Market scanner functions for batch analysis.

This module provides functions for scanning market indices (S&P 500, NASDAQ-100)
and analyzing performance with sector grouping.

Usage:
    from sawa.scanner import scan_ytd_performance

    results = await scan_ytd_performance(
        index="sp500",
        start_date="2026-01-01",
        large_cap_threshold=100,
    )
"""

import asyncio
import logging
from datetime import date, datetime
from typing import Any

from sawa.api.async_client import AsyncPolygonClient
from sawa.utils.config import get_env
from sawa.utils.sic_mapping import map_sic_to_gics
from sawa.utils.symbols import fetch_index_symbols


async def scan_ytd_performance(
    index: str = "sp500",
    start_date: str | None = None,
    large_cap_threshold: float = 100.0,
    top_n: int = 20,
    bottom_n: int = 20,
    concurrency: int = 10,
    api_key: str | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Scan index for YTD performance with sector grouping.

    Args:
        index: Index to scan ("sp500", "nasdaq100", or "both")
        start_date: Start date in YYYY-MM-DD format (defaults to Jan 1 current year)
        large_cap_threshold: Market cap threshold in billions for large cap classification
        top_n: Number of top gainers to return per table
        bottom_n: Number of bottom losers to return per table
        concurrency: Max concurrent API requests
        api_key: Polygon API key (defaults to env var)
        logger: Logger instance

    Returns:
        Dict with:
            - index: str
            - start_date: str
            - end_date: str
            - total_symbols: int
            - large_cap: dict with sector analysis
            - small_mid_cap: dict with sector analysis
            - top_gainers: list of dicts
            - top_losers: list of dicts
            - errors: list of error messages
    """
    api_key = api_key or get_env("POLYGON_API_KEY")
    logger = logger or logging.getLogger(__name__)

    # Default to Jan 1 of current year
    if not start_date:
        start_date = f"{datetime.now().year}-01-01"

    start = datetime.fromisoformat(start_date).date()
    end = date.today()

    # Fetch symbols
    logger.info(f"Fetching {index} symbols...")
    if index.lower() == "both":
        sp500 = fetch_index_symbols("sp500", logger)
        nasdaq100 = fetch_index_symbols("nasdaq100", logger)
        symbols = list(set(sp500 + nasdaq100))  # Remove duplicates
    else:
        symbols = fetch_index_symbols(index, logger)

    logger.info(f"Fetching data for {len(symbols)} symbols...")

    # Fetch price data
    client = AsyncPolygonClient(api_key, logger)
    price_data = await client.get_aggregates_batch(
        tickers=symbols,
        start_date=start,
        end_date=end,
        concurrency=concurrency,
    )

    # Fetch company details for sector/market cap
    logger.info("Fetching company details...")
    details_tasks = []
    for ticker in symbols:
        details_tasks.append(client.get_ticker_details(ticker))

    details_results = await asyncio.gather(*details_tasks, return_exceptions=True)

    # Build ticker -> details map
    ticker_details: dict[str, dict[str, Any]] = {}
    for ticker, result in zip(symbols, details_results):
        if isinstance(result, Exception) or result is None:
            continue
        ticker_details[ticker] = result

    # Process results
    results: list[dict[str, Any]] = []
    errors: list[str] = []

    for ticker, prices in price_data.items():
        if not prices or len(prices) < 2:
            errors.append(f"{ticker}: Insufficient data")
            continue

        details = ticker_details.get(ticker)
        if not details:
            errors.append(f"{ticker}: No company details")
            continue

        # Calculate performance
        prices.sort(key=lambda x: x["t"])
        start_price = prices[0]["c"]
        end_price = prices[-1]["c"]
        change_pct = ((end_price - start_price) / start_price) * 100

        # Get market cap and sector
        market_cap = details.get("market_cap", 0) / 1e9  # Convert to billions
        sic_code = details.get("sic_code")
        sector = map_sic_to_gics(sic_code) if sic_code else "Unknown"

        results.append(
            {
                "ticker": ticker,
                "name": details.get("name", ""),
                "sector": sector,
                "market_cap_billions": round(market_cap, 2),
                "start_price": start_price,
                "end_price": end_price,
                "change_percent": round(change_pct, 2),
                "is_large_cap": market_cap >= large_cap_threshold,
            }
        )

    # Sort by performance
    results.sort(key=lambda x: x["change_percent"], reverse=True)

    # Split by market cap
    large_cap = [r for r in results if r["is_large_cap"]]
    small_mid_cap = [r for r in results if not r["is_large_cap"]]

    # Group by sector
    def group_by_sector(data: list[dict]) -> dict[str, list[dict]]:
        sectors: dict[str, list[dict]] = {}
        for item in data:
            sector = item["sector"]
            if sector not in sectors:
                sectors[sector] = []
            sectors[sector].append(item)
        return sectors

    return {
        "index": index,
        "start_date": start_date,
        "end_date": end.isoformat(),
        "total_symbols": len(symbols),
        "successful": len(results),
        "failed": len(errors),
        "large_cap": {
            "count": len(large_cap),
            "top_gainers": large_cap[:top_n],
            "top_losers": large_cap[-bottom_n:][::-1],
            "by_sector": group_by_sector(large_cap),
        },
        "small_mid_cap": {
            "count": len(small_mid_cap),
            "top_gainers": small_mid_cap[:top_n],
            "top_losers": small_mid_cap[-bottom_n:][::-1],
            "by_sector": group_by_sector(small_mid_cap),
        },
        "errors": errors[:10],  # Limit error list
    }
