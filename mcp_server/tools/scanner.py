"""Stock performance scanner MCP tool."""

import asyncio
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

import requests
from bs4 import BeautifulSoup

from ..utils.sic_mapping import map_sic_to_gics

logger = logging.getLogger(__name__)


def fetch_sp500_symbols() -> list[str]:
    """Fetch S&P 500 symbols from Wikipedia."""
    from sawa.coldstart import fetch_sp500_symbols as fetch_symbols
    from sawa.utils.logging import setup_logging

    logger_temp = setup_logging(verbose=False)
    return fetch_symbols(logger_temp)


def fetch_nasdaq100_symbols() -> list[str]:
    """Fetch NASDAQ-100 symbols from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    logger.info(f"Fetching NASDAQ-100 symbols from {url}")

    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; StockScanner/1.0; +https://github.com)"}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Find the components table by looking for headers containing "ticker" and "company"
        tables = soup.find_all("table", {"class": "wikitable"})
        components_table = None

        for table in tables:
            headers = [h.get_text(strip=True).lower() for h in table.find_all("th")]
            if "ticker" in headers and "company" in headers:
                components_table = table
                break

        if not components_table:
            raise ValueError("Could not find NASDAQ-100 components table")

        symbols = []
        for row in components_table.find_all("tr")[1:]:  # Skip header row
            cells = row.find_all("td")
            if len(cells) >= 1:
                # Ticker is in the first column (index 0)
                ticker = cells[0].get_text(strip=True)
                if ticker:
                    # Clean up ticker - keep only alphanumeric characters
                    ticker = "".join(c for c in ticker if c.isalnum())
                    # Only add valid tickers (all uppercase letters, 1-5 chars)
                    if ticker and ticker.isalpha() and 1 <= len(ticker) <= 5:
                        symbols.append(ticker.upper())

        logger.info(f"Fetched {len(symbols)} NASDAQ-100 symbols")
        return sorted(set(symbols))  # Remove duplicates and sort

    except Exception as e:
        logger.error(f"Failed to fetch NASDAQ-100 symbols: {e}")
        raise


async def fetch_ticker_data_async(
    ticker: str, start_date: str, end_date: str, api_key: str, semaphore: asyncio.Semaphore
) -> dict[str, Any] | None:
    """
    Fetch price data and company details for a single ticker (async).

    Args:
        ticker: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        api_key: Polygon API key
        semaphore: Semaphore for rate limiting

    Returns:
        Dict with ticker data or None if failed
    """
    import httpx

    async with semaphore:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Fetch prices
                price_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
                price_params = {
                    "adjusted": "true",
                    "sort": "asc",
                    "limit": 50000,
                    "apiKey": api_key,
                }

                price_resp = await client.get(price_url, params=price_params)
                price_resp.raise_for_status()
                price_data = price_resp.json()

                results = price_data.get("results", [])
                if not results or len(results) < 2:
                    logger.warning(f"{ticker}: Insufficient price data")
                    return None

                # Calculate YTD return
                first_close = results[0]["c"]
                last_close = results[-1]["c"]
                ytd_return = ((last_close - first_close) / first_close) * 100

                # Fetch ticker details for market cap and sector
                detail_url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
                detail_params = {"apiKey": api_key}

                detail_resp = await client.get(detail_url, params=detail_params)
                detail_resp.raise_for_status()
                detail_data = detail_resp.json()

                details = detail_data.get("results", {})
                market_cap = details.get("market_cap")
                sic_code = details.get("sic_code")
                sic_description = details.get("sic_description")

                # Map to GICS sector
                sector_info = map_sic_to_gics(sic_code, sic_description)

                return {
                    "ticker": ticker,
                    "name": details.get("name", ticker),
                    "ytd_return": ytd_return,
                    "market_cap": market_cap or 0,
                    "first_close": first_close,
                    "last_close": last_close,
                    "days": len(results),
                    "sic_code": sic_code,
                    "sic_description": sic_description,
                    "gics_sector": sector_info.gics_sector,
                    "subsector": sector_info.subsector,
                }

        except Exception as e:
            logger.warning(f"{ticker}: Failed to fetch data - {e}")
            return None


async def fetch_all_tickers_async(
    tickers: list[str], start_date: str, end_date: str, api_key: str, max_concurrent: int = 10
) -> list[dict[str, Any]]:
    """
    Fetch data for all tickers with rate limiting.

    Args:
        tickers: List of ticker symbols
        start_date: Start date
        end_date: End date
        api_key: Polygon API key
        max_concurrent: Max concurrent requests

    Returns:
        List of ticker data dicts
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = [
        fetch_ticker_data_async(ticker, start_date, end_date, api_key, semaphore)
        for ticker in tickers
    ]

    logger.info(f"Fetching data for {len(tickers)} tickers...")
    results = await asyncio.gather(*tasks)

    # Filter out None results
    valid_results = [r for r in results if r is not None]
    logger.info(f"Successfully fetched {len(valid_results)}/{len(tickers)} tickers")

    return valid_results


def format_table_a(data: list[dict[str, Any]]) -> str:
    """
    Format Table A - Subsector Performance YTD.

    Args:
        data: List of ticker data dicts

    Returns:
        Formatted table string
    """
    # Group by sector and subsector
    sector_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for item in data:
        key = (item["gics_sector"], item["subsector"])
        sector_groups[key].append(item)

    # Calculate stats for each subsector
    subsector_stats = []
    for (sector, subsector), stocks in sector_groups.items():
        returns = [s["ytd_return"] for s in stocks]
        avg_return = sum(returns) / len(returns)

        best = max(stocks, key=lambda x: x["ytd_return"])
        worst = min(stocks, key=lambda x: x["ytd_return"])

        subsector_stats.append(
            {
                "sector": sector,
                "subsector": subsector,
                "count": len(stocks),
                "avg_return": avg_return,
                "best_ticker": best["ticker"],
                "best_return": best["ytd_return"],
                "worst_ticker": worst["ticker"],
                "worst_return": worst["ytd_return"],
            }
        )

    # Sort by sector, then avg return descending
    subsector_stats.sort(key=lambda x: (x["sector"], -x["avg_return"]))

    # Format table
    lines = []
    lines.append("=" * 120)
    lines.append("TABLE A: SUBSECTOR PERFORMANCE YTD")
    lines.append("=" * 120)
    lines.append(
        f"{'Sector':<22} | {'Subsector':<35} | {'Count':>5} | {'Avg YTD':>9} | {'Best':>15} | {'Worst':>15}"
    )
    lines.append("-" * 120)

    for stat in subsector_stats:
        lines.append(
            f"{stat['sector']:<22} | {stat['subsector']:<35} | {stat['count']:>5} | "
            f"{stat['avg_return']:>+8.2f}% | {stat['best_ticker']} {stat['best_return']:>+6.1f}% | "
            f"{stat['worst_ticker']} {stat['worst_return']:>+6.1f}%"
        )

    lines.append("=" * 120)

    return "\n".join(lines)


def format_table_b(
    data: list[dict[str, Any]], large_cap_threshold: float = 100e9, top_n: int = 10
) -> str:
    """
    Format Table B - Top Large Cap Winners and Losers.

    Args:
        data: List of ticker data dicts
        large_cap_threshold: Market cap threshold in dollars (default: $100B)
        top_n: Number of winners/losers to show

    Returns:
        Formatted table string
    """
    # Filter for large caps
    large_caps = [d for d in data if d["market_cap"] >= large_cap_threshold]

    if not large_caps:
        return f"No stocks found with market cap >= ${large_cap_threshold / 1e9:.0f}B"

    # Sort by YTD return
    winners = sorted(large_caps, key=lambda x: x["ytd_return"], reverse=True)[:top_n]
    losers = sorted(large_caps, key=lambda x: x["ytd_return"])[:top_n]

    def format_market_cap(mc: float) -> str:
        if mc >= 1e12:
            return f"${mc / 1e12:.2f}T"
        elif mc >= 1e9:
            return f"${mc / 1e9:.0f}B"
        else:
            return f"${mc / 1e6:.0f}M"

    # Build table
    lines = []
    lines.append("=" * 120)
    lines.append(
        f"TABLE B: TOP {top_n} LARGE CAP (${large_cap_threshold / 1e9:.0f}B+) WINNERS & LOSERS"
    )
    lines.append("=" * 120)

    # Header
    lines.append(f"{'WINNERS':<58} | {'LOSERS':<58}")
    lines.append(
        f"{'#':>2}  {'Ticker':<6} {'YTD':>8} {'Market Cap':>12} {'Name':<23} | "
        f"{'#':>2}  {'Ticker':<6} {'YTD':>8} {'Market Cap':>12} {'Name':<23}"
    )
    lines.append("-" * 120)

    # Rows
    for i in range(max(len(winners), len(losers))):
        winner_str = ""
        if i < len(winners):
            w = winners[i]
            winner_str = (
                f"{i + 1:>2}  {w['ticker']:<6} {w['ytd_return']:>+7.2f}% "
                f"{format_market_cap(w['market_cap']):>12} {w['name'][:23]:<23}"
            )
        else:
            winner_str = " " * 58

        loser_str = ""
        if i < len(losers):
            l = losers[i]
            loser_str = (
                f"{i + 1:>2}  {l['ticker']:<6} {l['ytd_return']:>+7.2f}% "
                f"{format_market_cap(l['market_cap']):>12} {l['name'][:23]:<23}"
            )
        else:
            loser_str = " " * 58

        lines.append(f"{winner_str} | {loser_str}")

    lines.append("=" * 120)
    lines.append(f"Total large cap stocks analyzed: {len(large_caps)}")

    return "\n".join(lines)


def scan_ytd_performance(
    start_date: str | None = None,
    large_cap_threshold: float = 100.0,
    top_n: int = 10,
    index: str = "sp500",
) -> dict[str, Any]:
    """
    Scan market indices for YTD performance.

    Args:
        start_date: Start date (default: Jan 1 of current year)
        large_cap_threshold: Market cap threshold in billions (default: 100)
        top_n: Number of winners/losers to show (default: 10)
        index: Index to scan - "sp500", "nasdaq100", or "both" (default: "sp500")

    Returns:
        Dict with tables and summary data
    """
    from sawa.utils.config import get_env

    # Validate index parameter
    valid_indices = ["sp500", "nasdaq100", "both"]
    if index not in valid_indices:
        raise ValueError(f"Invalid index '{index}'. Must be one of: {valid_indices}")

    # Get API key
    api_key = get_env("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY not configured")

    # Default to YTD
    if start_date is None:
        start_date = f"{datetime.now().year}-01-01"

    end_date = datetime.now().strftime("%Y-%m-%d")

    # Fetch symbols based on index selection
    start_time = time.time()
    symbols = []
    index_name = ""

    if index == "sp500":
        symbols = fetch_sp500_symbols()
        index_name = "S&P 500"
        logger.info(f"Fetched {len(symbols)} S&P 500 symbols")
    elif index == "nasdaq100":
        symbols = fetch_nasdaq100_symbols()
        index_name = "NASDAQ-100"
        logger.info(f"Fetched {len(symbols)} NASDAQ-100 symbols")
    elif index == "both":
        sp500_symbols = fetch_sp500_symbols()
        nasdaq100_symbols = fetch_nasdaq100_symbols()
        symbols = sorted(set(sp500_symbols + nasdaq100_symbols))
        index_name = "S&P 500 + NASDAQ-100"
        logger.info(
            f"Fetched {len(sp500_symbols)} S&P 500 + {len(nasdaq100_symbols)} NASDAQ-100 = "
            f"{len(symbols)} unique symbols"
        )

    logger.info(f"Scanning {index_name} performance from {start_date} to {end_date}")

    # Fetch data for all tickers
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        ticker_data = loop.run_until_complete(
            fetch_all_tickers_async(symbols, start_date, end_date, api_key, max_concurrent=20)
        )
    finally:
        loop.close()

    elapsed = time.time() - start_time

    # Generate tables
    threshold_dollars = large_cap_threshold * 1e9
    table_a = format_table_a(ticker_data)
    table_b = format_table_b(ticker_data, threshold_dollars, top_n)

    # Summary stats
    total_stocks = len(ticker_data)
    avg_return = sum(d["ytd_return"] for d in ticker_data) / total_stocks if total_stocks else 0
    winners = len([d for d in ticker_data if d["ytd_return"] > 0])
    losers = len([d for d in ticker_data if d["ytd_return"] < 0])

    winners_pct = (winners / total_stocks * 100) if total_stocks else 0
    losers_pct = (losers / total_stocks * 100) if total_stocks else 0

    summary = f"""
SCAN SUMMARY
============
Index: {index_name}
Period: {start_date} to {end_date}
Stocks analyzed: {total_stocks} / {len(symbols)}
Average YTD return: {avg_return:+.2f}%
Winners: {winners} ({winners_pct:.1f}%)
Losers: {losers} ({losers_pct:.1f}%)
Execution time: {elapsed:.1f}s
"""

    return {
        "summary": summary,
        "table_a": table_a,
        "table_b": table_b,
        "data": ticker_data,
        "stats": {
            "total_stocks": total_stocks,
            "avg_return": avg_return,
            "winners": winners,
            "losers": losers,
            "elapsed_seconds": elapsed,
        },
    }
