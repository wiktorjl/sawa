"""
Unified Polygon/Massive REST API client.

Handles all REST API calls for:
- Financial ratios
- Trading days
- Fundamentals (balance sheets, cash flow, income statements)
- Economy data (treasury yields, inflation, labor market)
- Company overviews
"""

import logging
import time
from typing import Any
from urllib.parse import urljoin

import requests

# Polygon rebranded to Massive, but API structure is similar
BASE_URL = "https://api.polygon.io"

# Endpoint configurations
ENDPOINTS = {
    # Financial data
    "ratios": "/stocks/financials/v1/ratios",
    "balance-sheets": "/stocks/financials/v1/balance-sheets",
    "cash-flow": "/stocks/financials/v1/cash-flow-statements",
    "income-statements": "/stocks/financials/v1/income-statements",
    # Market data
    "aggregates": "/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}",
    "ticker-details": "/v3/reference/tickers/{ticker}",
    # Economy
    "treasury-yields": "/fed/v1/treasury-yields",
    "inflation": "/fed/v1/inflation",
    "inflation-expectations": "/fed/v1/inflation-expectations",
    "labor-market": "/fed/v1/labor-market",
    # Other
    "short-interest": "/stocks/v1/short-interest",
    "short-volume": "/stocks/v1/short-volume",
    # News
    "news": "/v2/reference/news",
}


class PolygonClient:
    """Unified client for Polygon/Massive REST API."""

    def __init__(self, api_key: str, logger: logging.Logger | None = None):
        self.api_key = api_key
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        path_params: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> dict[str, Any]:
        """
        Make GET request to API.

        Args:
            endpoint: Endpoint key from ENDPOINTS or full path
            params: Query parameters
            path_params: URL path parameters (e.g., {ticker})
            timeout: Request timeout

        Returns:
            JSON response data
        """
        path = ENDPOINTS.get(endpoint, endpoint)
        if path_params:
            path = path.format(**path_params)

        url = urljoin(BASE_URL, path)
        params = params or {}
        params["apiKey"] = self.api_key

        self.logger.debug(f"GET {url}")
        response = self.session.get(url, params=params, timeout=timeout)
        response.raise_for_status()

        data = response.json()
        if data.get("status") not in ("OK", "DELAYED"):
            error = data.get("error", data.get("message", "Unknown error"))
            raise ValueError(f"API error: {error}")

        return data

    def get_paginated(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        timeout: int = 30,
    ) -> list[dict[str, Any]]:
        """
        Fetch all pages from paginated endpoint.

        Args:
            endpoint: Endpoint key or path
            params: Query parameters
            timeout: Request timeout

        Returns:
            List of all results across pages
        """
        path = ENDPOINTS.get(endpoint, endpoint)
        url = urljoin(BASE_URL, path)
        params = params or {}
        params["apiKey"] = self.api_key

        all_results: list[dict[str, Any]] = []
        page = 0

        while url:
            page += 1
            self.logger.debug(f"Fetching page {page}")

            if page > 1:
                response = self.session.get(url, timeout=timeout)
            else:
                response = self.session.get(url, params=params, timeout=timeout)

            response.raise_for_status()
            data = response.json()

            if data.get("status") not in ("OK", "DELAYED"):
                error = data.get("error", data.get("message", "Unknown"))
                raise ValueError(f"API error: {error}")

            all_results.extend(data.get("results", []))
            url = data.get("next_url")

            if url and not url.startswith("http"):
                url = urljoin(BASE_URL, url)

        self.logger.debug(f"Total results: {len(all_results)}")
        return all_results

    def get_single(
        self,
        endpoint: str,
        path_params: dict[str, str] | None = None,
        timeout: int = 30,
        max_retries: int = 3,
    ) -> dict[str, Any] | None:
        """
        Fetch single resource with retry logic.

        Args:
            endpoint: Endpoint key or path
            path_params: URL path parameters
            timeout: Request timeout
            max_retries: Retry attempts for rate limits

        Returns:
            Result data or None if not found
        """
        path = ENDPOINTS.get(endpoint, endpoint)
        if path_params:
            path = path.format(**path_params)

        url = urljoin(BASE_URL, path)

        for attempt in range(max_retries):
            try:
                response = self.session.get(
                    url,
                    params={"apiKey": self.api_key},
                    timeout=timeout,
                )

                if response.status_code == 404:
                    return None

                if response.status_code == 429:
                    wait = (attempt + 1) * 2
                    self.logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                data = response.json()

                if data.get("status") != "OK":
                    return None

                return data.get("results")

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Request failed: {e}. Retrying...")
                    time.sleep(1)
                else:
                    raise

        return None

    # Convenience methods for specific data types

    def get_trading_days(self, start_date: str, end_date: str, ticker: str = "AAPL") -> list[str]:
        """Get trading days in date range using ticker as proxy."""
        data = self.get(
            "aggregates",
            path_params={"ticker": ticker, "start": start_date, "end": end_date},
            params={"adjusted": "true"},
        )
        results = data.get("results", [])
        from sawa.utils.dates import DATE_FORMAT, timestamp_to_date

        return [timestamp_to_date(r["t"]).strftime(DATE_FORMAT) for r in results if r.get("t")]

    def get_ratios(self, ticker: str, limit: int = 100) -> list[dict[str, Any]]:
        """Get financial ratios for ticker."""
        data = self.get("ratios", params={"ticker": ticker, "limit": limit})
        return data.get("results", [])

    def get_fundamentals(
        self,
        endpoint: str,
        ticker: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: str | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        """Get fundamentals data (balance sheets, income, cash flow)."""
        params: dict[str, Any] = {"limit": limit}
        if ticker:
            params["tickers"] = ticker
        if start_date:
            params["period_end.gte"] = start_date
        if end_date:
            params["period_end.lte"] = end_date
        if timeframe:
            params["timeframe"] = timeframe
        return self.get_paginated(endpoint, params)

    def get_economy_data(
        self,
        endpoint: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get economy data (treasury, inflation, labor)."""
        params: dict[str, Any] = {"limit": 50000, "sort": "date.asc"}
        if start_date:
            params["date.gte"] = start_date
        if end_date:
            params["date.lte"] = end_date
        return self.get_paginated(endpoint, params)

    def get_ticker_details(self, ticker: str) -> dict[str, Any] | None:
        """Get company overview/details."""
        return self.get_single("ticker-details", path_params={"ticker": ticker})

    def get_news(
        self,
        ticker: str | None = None,
        published_utc_gte: str | None = None,
        published_utc_lte: str | None = None,
        limit: int = 100,
        order: str = "desc",
        sort: str = "published_utc",
    ) -> list[dict[str, Any]]:
        """
        Get news articles with sentiment analysis.

        Args:
            ticker: Filter by ticker symbol (e.g., 'AAPL')
            published_utc_gte: Return articles published after this date (RFC3339)
            published_utc_lte: Return articles published before this date (RFC3339)
            limit: Max results per page (max 1000)
            order: Sort order ('asc' or 'desc')
            sort: Sort field ('published_utc')

        Returns:
            List of news articles with sentiment insights
        """
        params: dict[str, Any] = {
            "limit": limit,
            "order": order,
            "sort": sort,
        }
        if ticker:
            params["ticker"] = ticker
        if published_utc_gte:
            params["published_utc.gte"] = published_utc_gte
        if published_utc_lte:
            params["published_utc.lte"] = published_utc_lte

        return self.get_paginated("news", params)
