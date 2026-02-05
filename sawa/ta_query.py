"""
Technical indicators query functions.

Provides functions to query technical indicators data for CLI and other interfaces.
Uses the repository layer for data access.
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Any

from sawa.repositories import get_factory

logger = logging.getLogger(__name__)


def get_latest_indicators(ticker: str) -> dict[str, Any] | None:
    """Get the most recent technical indicators for a ticker.

    Args:
        ticker: Stock symbol

    Returns:
        Dict with indicator values, or None if not found
    """
    factory = get_factory()
    repo = factory.get_technical_indicators_repository()

    async def _fetch():
        return await repo.get_latest_indicators(ticker)

    result = asyncio.get_event_loop().run_until_complete(_fetch())

    if result is None:
        return None

    return _indicators_to_dict(result)


def get_indicators_history(
    ticker: str,
    start_date: date,
    end_date: date | None = None,
) -> list[dict[str, Any]]:
    """Get technical indicators history for a ticker.

    Args:
        ticker: Stock symbol
        start_date: Start date
        end_date: End date (defaults to today)

    Returns:
        List of indicator dicts
    """
    factory = get_factory()
    repo = factory.get_technical_indicators_repository()

    if end_date is None:
        end_date = date.today()

    async def _fetch():
        return await repo.get_indicators(ticker, start_date, end_date)

    results = asyncio.get_event_loop().run_until_complete(_fetch())
    return [_indicators_to_dict(r) for r in results]


def screen_indicators(
    filters: dict[str, tuple[float | None, float | None]],
    target_date: date | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Screen stocks by technical indicator values.

    Args:
        filters: Dict mapping indicator name to (min, max) tuple.
        target_date: Date to screen (defaults to most recent)
        limit: Maximum results

    Returns:
        List of matching indicator dicts
    """
    factory = get_factory()
    repo = factory.get_technical_indicators_repository()

    async def _fetch():
        return await repo.screen_by_indicators(filters, target_date, limit)

    results = asyncio.get_event_loop().run_until_complete(_fetch())
    return [_indicators_to_dict(r) for r in results]


def _indicators_to_dict(ind) -> dict[str, Any]:
    """Convert TechnicalIndicators to dict."""
    return {
        "ticker": ind.ticker,
        "date": ind.date.isoformat() if ind.date else None,
        # Trend
        "sma_5": float(ind.sma_5) if ind.sma_5 else None,
        "sma_10": float(ind.sma_10) if ind.sma_10 else None,
        "sma_20": float(ind.sma_20) if ind.sma_20 else None,
        "sma_50": float(ind.sma_50) if ind.sma_50 else None,
        "ema_12": float(ind.ema_12) if ind.ema_12 else None,
        "ema_26": float(ind.ema_26) if ind.ema_26 else None,
        "ema_50": float(ind.ema_50) if ind.ema_50 else None,
        "vwap": float(ind.vwap) if ind.vwap else None,
        # Momentum
        "rsi_14": float(ind.rsi_14) if ind.rsi_14 else None,
        "rsi_21": float(ind.rsi_21) if ind.rsi_21 else None,
        "macd_line": float(ind.macd_line) if ind.macd_line else None,
        "macd_signal": float(ind.macd_signal) if ind.macd_signal else None,
        "macd_histogram": float(ind.macd_histogram) if ind.macd_histogram else None,
        # Volatility
        "bb_upper": float(ind.bb_upper) if ind.bb_upper else None,
        "bb_middle": float(ind.bb_middle) if ind.bb_middle else None,
        "bb_lower": float(ind.bb_lower) if ind.bb_lower else None,
        "atr_14": float(ind.atr_14) if ind.atr_14 else None,
        # Volume
        "obv": ind.obv,
        "volume_sma_20": ind.volume_sma_20,
        "volume_ratio": float(ind.volume_ratio) if ind.volume_ratio else None,
    }


def format_indicators_table(indicators: dict[str, Any]) -> str:
    """Format indicators as a readable table.

    Args:
        indicators: Dict of indicator values

    Returns:
        Formatted string
    """
    lines = [
        f"Technical Indicators for {indicators['ticker']}",
        f"Date: {indicators['date']}",
        "",
        "TREND",
        f"  SMA-5:   {_fmt(indicators['sma_5'])}",
        f"  SMA-10:  {_fmt(indicators['sma_10'])}",
        f"  SMA-20:  {_fmt(indicators['sma_20'])}",
        f"  SMA-50:  {_fmt(indicators['sma_50'])}",
        f"  EMA-12:  {_fmt(indicators['ema_12'])}",
        f"  EMA-26:  {_fmt(indicators['ema_26'])}",
        f"  EMA-50:  {_fmt(indicators['ema_50'])}",
        f"  VWAP:    {_fmt(indicators['vwap'])}",
        "",
        "MOMENTUM",
        f"  RSI-14:       {_fmt(indicators['rsi_14'], 2)}",
        f"  RSI-21:       {_fmt(indicators['rsi_21'], 2)}",
        f"  MACD Line:    {_fmt(indicators['macd_line'])}",
        f"  MACD Signal:  {_fmt(indicators['macd_signal'])}",
        f"  MACD Hist:    {_fmt(indicators['macd_histogram'])}",
        "",
        "VOLATILITY",
        f"  BB Upper:  {_fmt(indicators['bb_upper'])}",
        f"  BB Middle: {_fmt(indicators['bb_middle'])}",
        f"  BB Lower:  {_fmt(indicators['bb_lower'])}",
        f"  ATR-14:    {_fmt(indicators['atr_14'])}",
        "",
        "VOLUME",
        f"  OBV:          {indicators['obv'] or 'N/A':>15}",
        f"  Volume SMA:   {indicators['volume_sma_20'] or 'N/A':>15}",
        f"  Volume Ratio: {_fmt(indicators['volume_ratio'], 2)}",
    ]
    return "\n".join(lines)


def format_screen_results(results: list[dict[str, Any]], filters: dict) -> str:
    """Format screening results as a table.

    Args:
        results: List of indicator dicts
        filters: Filters that were applied

    Returns:
        Formatted string
    """
    if not results:
        return "No stocks match the criteria."

    # Header
    lines = [
        f"Screening Results ({len(results)} matches)",
        f"Filters: {_format_filters(filters)}",
        "",
        f"{'Ticker':<8} {'Date':<12} {'RSI-14':>8} {'MACD':>10} {'Vol Ratio':>10}",
        "-" * 50,
    ]

    # Data rows
    for r in results:
        lines.append(
            f"{r['ticker']:<8} {r['date']:<12} "
            f"{_fmt(r['rsi_14'], 1):>8} "
            f"{_fmt(r['macd_histogram'], 2):>10} "
            f"{_fmt(r['volume_ratio'], 2):>10}"
        )

    return "\n".join(lines)


def _fmt(value: float | None, decimals: int = 2) -> str:
    """Format a numeric value."""
    if value is None:
        return "N/A".rjust(10)
    return f"{value:>{10}.{decimals}f}"


def _format_filters(filters: dict) -> str:
    """Format filters for display."""
    parts = []
    for name, (min_val, max_val) in filters.items():
        if min_val is not None and max_val is not None:
            parts.append(f"{min_val} <= {name} <= {max_val}")
        elif min_val is not None:
            parts.append(f"{name} >= {min_val}")
        elif max_val is not None:
            parts.append(f"{name} <= {max_val}")
    return ", ".join(parts) if parts else "none"
