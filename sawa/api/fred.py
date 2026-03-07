"""
FRED (Federal Reserve Economic Data) API client.

Fetches market internals data:
- VIXCLS: CBOE Volatility Index (VIX) daily close
- VXVCLS: CBOE S&P 500 3-Month Volatility Index (VIX3M)
- BAMLH0A0HYM2: ICE BofA US High Yield Index OAS (credit spread)
"""

import logging
from typing import Any

import httpx

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# FRED series IDs for market internals
SERIES = {
    "vix_close": "VIXCLS",
    "vix3m": "VXVCLS",
    "hy_spread": "BAMLH0A0HYM2",
}


class FredClient:
    """Client for the FRED API."""

    def __init__(self, api_key: str, logger: logging.Logger | None = None):
        self.api_key = api_key
        self.logger = logger or logging.getLogger(__name__)
        self.client = httpx.Client(timeout=30.0)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "FredClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get_series(
        self,
        series_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch observations for a FRED series.

        Args:
            series_id: FRED series ID (e.g., 'VIXCLS')
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD

        Returns:
            List of {"date": "YYYY-MM-DD", "value": "123.45"} dicts.
            Entries with value "." (missing) are filtered out.
        """
        params: dict[str, str] = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        if start_date:
            params["observation_start"] = start_date
        if end_date:
            params["observation_end"] = end_date

        self.logger.debug(f"FRED: fetching {series_id} ({start_date} to {end_date})")

        response = self.client.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        observations = data.get("observations", [])
        # Filter out missing values (FRED uses "." for no data)
        return [
            obs for obs in observations
            if obs.get("value") not in (".", "", None)
        ]

    def get_market_internals(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch all market internals series and merge by date.

        Returns:
            List of dicts with keys: date, vix_close, vix3m, hy_spread.
            Missing values for a given date are None.
        """
        # Fetch all series
        series_data: dict[str, dict[str, str]] = {}
        for field, series_id in SERIES.items():
            try:
                observations = self.get_series(series_id, start_date, end_date)
                series_data[field] = {
                    obs["date"]: obs["value"] for obs in observations
                }
                self.logger.info(
                    f"  FRED {series_id}: {len(observations)} observations"
                )
            except Exception as e:
                self.logger.warning(f"  FRED {series_id} failed: {e}")
                series_data[field] = {}

        # Use VIX dates as anchor (trading days only).
        # HY spread sometimes reports on weekends/holidays (month-ends),
        # which would create orphan rows with no VIX data.
        vix_dates = set(series_data.get("vix_close", {}).keys())
        vix3m_dates = set(series_data.get("vix3m", {}).keys())
        trading_dates = vix_dates | vix3m_dates

        # For HY spread dates that fall on non-trading days,
        # carry the value forward to the next trading day.
        hy_data = series_data.get("hy_spread", {})
        if hy_data:
            hy_non_trading = set(hy_data.keys()) - trading_dates
            if hy_non_trading:
                all_trading = sorted(trading_dates)
                for hy_dt in sorted(hy_non_trading):
                    # Find next trading day
                    for td in all_trading:
                        if td > hy_dt:
                            # Only carry forward if that day has no value
                            if td not in hy_data:
                                hy_data[td] = hy_data[hy_dt]
                            break
                    del hy_data[hy_dt]

        rows: list[dict[str, Any]] = []
        for dt in sorted(trading_dates):
            row: dict[str, Any] = {"date": dt}
            for field in SERIES:
                val = series_data[field].get(dt)
                row[field] = val  # str or None, DB will cast
            rows.append(row)

        self.logger.info(f"  Market internals: {len(rows)} dates total")
        return rows
