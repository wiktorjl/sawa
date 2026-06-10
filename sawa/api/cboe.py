"""
CBOE delayed-quotes API client.

Fetches same-day VIX/VIX3M settlement values. FRED (the primary
market-internals source) publishes VIXCLS/VXVCLS with a one-business-day
lag, so the evening daily run never sees today's row from FRED. CBOE's
delayed-quote feed carries the settled close (VIX settles 4:15 PM ET)
within minutes of settlement, letting the daily write today's VIX/VIX3M.

No API key required.
"""

import logging
from typing import Any

import httpx

BASE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/quotes"

# CBOE quote symbols -> market_internals columns
SYMBOLS = {
    "_VIX": "vix",
    "_VIX3M": "vix3m",
}


class CboeClient:
    """Client for CBOE's delayed-quotes CDN endpoints."""

    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or logging.getLogger(__name__)
        self.client = httpx.Client(timeout=30.0)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "CboeClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get_quote(self, symbol: str) -> dict[str, Any] | None:
        """
        Fetch the delayed quote for an index symbol (e.g. '_VIX').

        Returns:
            {"date": "YYYY-MM-DD", "close": float} for the most recent
            session, or None if the quote is unavailable or unusable.
        """
        url = f"{BASE_URL}/{symbol}.json"
        self.logger.debug(f"CBOE: fetching {url}")

        response = self.client.get(url)
        response.raise_for_status()
        data = response.json().get("data") or {}

        close = data.get("close")
        last_trade = data.get("last_trade_time") or ""
        # last_trade_time is ET, e.g. "2026-06-10T16:15:01"; its date part is
        # the trading date the close belongs to.
        trade_date = last_trade[:10]
        if not close or len(trade_date) != 10:
            self.logger.warning(
                f"  CBOE {symbol}: unusable quote "
                f"(close={close!r}, last_trade_time={last_trade!r})"
            )
            return None

        return {"date": trade_date, "close": float(close)}

    def get_market_internals(self) -> list[dict[str, Any]]:
        """
        Fetch latest VIX/VIX3M settlement values, merged by date.

        Returns:
            List of dicts with keys: date, vix, vix3m (no hy_spread —
            CBOE does not carry credit spreads). Usually a single row;
            empty if both quotes failed.
        """
        by_date: dict[str, dict[str, Any]] = {}
        for symbol, field in SYMBOLS.items():
            try:
                quote = self.get_quote(symbol)
            except Exception as e:
                self.logger.warning(f"  CBOE {symbol} failed: {e}")
                continue
            if quote is None:
                continue
            row = by_date.setdefault(quote["date"], {"date": quote["date"]})
            row[field] = quote["close"]
            self.logger.info(
                f"  CBOE {symbol}: {quote['close']} ({quote['date']})"
            )

        return [by_date[dt] for dt in sorted(by_date)]
