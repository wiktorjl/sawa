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
from typing import Any, cast

import httpx

from sawa.domain.exceptions import ProviderError
from sawa.utils.constants import DEFAULT_HTTP_TIMEOUT
from sawa.utils.security import redact_sensitive_text, validate_https_origin_url
from sawa.utils.symbols import validate_ticker

# Polygon rebranded to Massive, but API structure is similar
BASE_URL = "https://api.polygon.io"
MAX_PAGINATION_PAGES = 1_000
MAX_PAGINATION_RESULTS = 1_000_000

# Endpoint configurations
ENDPOINTS = {
    # Financial data
    "ratios": "/stocks/financials/v1/ratios",
    "balance-sheets": "/stocks/financials/v1/balance-sheets",
    "cash-flow": "/stocks/financials/v1/cash-flow-statements",
    "income-statements": "/stocks/financials/v1/income-statements",
    # Market data
    "aggregates": "/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}",
    "aggregates-intraday": "/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start}/{end}",
    "ticker-details": "/v3/reference/tickers/{ticker}",
    # Economy
    "treasury-yields": "/fed/v1/treasury-yields",
    "inflation": "/fed/v1/inflation",
    "inflation-expectations": "/fed/v1/inflation-expectations",
    "labor-market": "/fed/v1/labor-market",
    # Corporate actions
    "splits": "/v3/reference/splits",
    "dividends": "/v3/reference/dividends",
    "ticker-events": "/vX/reference/tickers/{ticker}/events",
    # Other
    "short-interest": "/stocks/v1/short-interest",
    "short-volume": "/stocks/v1/short-volume",
    # News
    "news": "/v2/reference/news",
}


def _validated_polygon_url(path_or_url: str) -> str:
    """Resolve a Polygon path and reject credential-crossing origins."""
    try:
        return validate_https_origin_url(BASE_URL, path_or_url)
    except ValueError as e:
        raise ProviderError(
            "Refusing Polygon URL outside the configured HTTPS API origin",
            provider="polygon",
        ) from e


class PolygonClient:
    """Unified client for Polygon/Massive REST API."""

    def __init__(self, api_key: str, logger: logging.Logger | None = None):
        self.api_key = api_key
        self.logger = logger or logging.getLogger(__name__)
        self.client = httpx.Client(
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=float(DEFAULT_HTTP_TIMEOUT),
            follow_redirects=False,
        )

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        self.client.close()

    def __enter__(self) -> "PolygonClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        path_params: dict[str, str] | None = None,
        timeout: int = DEFAULT_HTTP_TIMEOUT,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """
        Make GET request to API.

        Retries transient failures (timeouts/connection drops, HTTP 429
        rate limits, and 5xx server errors) with backoff, matching
        get_single's retry style.

        Args:
            endpoint: Endpoint key from ENDPOINTS or full path
            params: Query parameters
            path_params: URL path parameters (e.g., {ticker})
            timeout: Request timeout
            max_retries: Retry attempts for transient failures

        Returns:
            JSON response data
        """
        path = ENDPOINTS.get(endpoint, endpoint)
        if path_params:
            path = path.format(**path_params)

        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")

        url = _validated_polygon_url(path)
        request_params = dict(params or {})
        request_params["apiKey"] = self.api_key

        self.logger.debug(f"GET {url}")

        for attempt in range(max_retries):
            try:
                response = self.client.get(url, params=request_params, timeout=timeout)

                if response.status_code == 429:
                    if attempt >= max_retries - 1:
                        raise ProviderError(
                            f"Rate limited after {max_retries} attempts",
                            provider="polygon",
                        )
                    wait = (attempt + 1) * 2
                    self.logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    raise ProviderError(
                        "Invalid response object",
                        provider="polygon",
                    )
                if data.get("status") not in ("OK", "DELAYED"):
                    error = data.get("error", data.get("message", "Unknown error"))
                    raise ProviderError(f"API error: {error}", provider="polygon")

                return data

            except httpx.RequestError as e:
                if attempt < max_retries - 1:
                    self.logger.warning(
                        "Polygon request failed (%s); retrying",
                        type(e).__name__,
                    )
                    time.sleep(1)
                else:
                    raise
            except httpx.HTTPStatusError as e:
                # Retry on transient 5xx; surface other 4xx/5xx as ProviderError.
                if e.response.status_code >= 500 and attempt < max_retries - 1:
                    self.logger.warning(
                        f"Polygon HTTP {e.response.status_code}; retrying"
                    )
                    time.sleep(1)
                    continue
                raise ProviderError(
                    f"HTTP {e.response.status_code}: {e}",
                    provider="polygon",
                    original_error=e,
                ) from e

        # All attempts exhausted on 429s without returning.
        raise ProviderError(
            f"Rate limited after {max_retries} attempts", provider="polygon"
        )

    def get_paginated(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        timeout: int = DEFAULT_HTTP_TIMEOUT,
        max_retries: int = 3,
    ) -> list[dict[str, Any]]:
        """
        Fetch all pages from paginated endpoint.

        Retries each page independently on transient httpx.RequestError
        (timeouts, connection drops) and on HTTP 429 rate-limit responses,
        matching get_single's behavior.

        Args:
            endpoint: Endpoint key or path
            params: Query parameters
            timeout: Request timeout per page
            max_retries: Retry attempts per page for transient failures

        Returns:
            List of all results across pages
        """
        path = ENDPOINTS.get(endpoint, endpoint)
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")

        url = _validated_polygon_url(path)
        request_params = dict(params or {})
        request_params["apiKey"] = self.api_key

        all_results: list[dict[str, Any]] = []
        page = 0
        seen_urls = {url}

        while url:
            page += 1
            self.logger.debug(f"Fetching page {page}")

            response: httpx.Response | None = None
            for attempt in range(max_retries):
                try:
                    if page > 1:
                        # httpx REPLACES an existing query string when params=
                        # is supplied, which stripped the cursor Polygon puts
                        # in next_url and made every page re-request page 1
                        # until the repeated-URL guard fired. Merge the key
                        # into the validated URL so pagination advances.
                        request_url = (
                            httpx.URL(url)
                            .copy_remove_param("apiKey")
                            .copy_merge_params({"apiKey": self.api_key})
                        )
                        response = self.client.get(
                            request_url,
                            timeout=timeout,
                        )
                    else:
                        response = self.client.get(
                            url,
                            params=request_params,
                            timeout=timeout,
                        )

                    if response.status_code == 429:
                        if attempt >= max_retries - 1:
                            raise ProviderError(
                                f"Rate limited on page {page} after "
                                f"{max_retries} attempts",
                                provider="polygon",
                            )
                        wait = (attempt + 1) * 2
                        self.logger.warning(
                            f"Rate limited on page {page}. Waiting {wait}s..."
                        )
                        time.sleep(wait)
                        continue

                    response.raise_for_status()
                    break
                except httpx.RequestError as e:
                    if attempt < max_retries - 1:
                        wait = attempt + 1
                        self.logger.warning(
                            f"Polygon request failed on page {page} "
                            f"({type(e).__name__}); retrying in {wait}s..."
                        )
                        time.sleep(wait)
                    else:
                        raise
                except httpx.HTTPStatusError as e:
                    if e.response.status_code >= 500 and attempt < max_retries - 1:
                        wait = attempt + 1
                        self.logger.warning(
                            f"Polygon HTTP {e.response.status_code} on page "
                            f"{page}; retrying in {wait}s..."
                        )
                        time.sleep(wait)
                        continue
                    raise ProviderError(
                        f"HTTP {e.response.status_code} on page {page}",
                        provider="polygon",
                        original_error=e,
                    ) from e
            else:
                # All attempts exhausted on 429s without ever breaking out.
                raise ProviderError(
                    f"Rate limited on page {page} after {max_retries} attempts",
                    provider="polygon",
                )

            assert response is not None  # for type checker; loop must have set it
            try:
                data = response.json()
            except (TypeError, ValueError) as e:
                raise ProviderError(
                    f"Invalid JSON response on page {page}",
                    provider="polygon",
                    original_error=e,
                ) from e

            if not isinstance(data, dict):
                raise ProviderError(
                    f"Invalid response object on page {page}",
                    provider="polygon",
                )

            if data.get("status") not in ("OK", "DELAYED"):
                error = data.get("error", data.get("message", "Unknown"))
                raise ProviderError(f"API error: {error}", provider="polygon")

            results = data.get("results", [])
            if not isinstance(results, list):
                raise ProviderError(
                    f"Invalid results array on page {page}",
                    provider="polygon",
                )
            if len(all_results) + len(results) > MAX_PAGINATION_RESULTS:
                raise ProviderError(
                    "Pagination exceeded maximum of "
                    f"{MAX_PAGINATION_RESULTS} results",
                    provider="polygon",
                )
            all_results.extend(results)

            next_url = data.get("next_url")
            if next_url is None or next_url == "":
                url = ""
            elif not isinstance(next_url, str):
                raise ProviderError(
                    f"Invalid pagination URL on page {page}",
                    provider="polygon",
                )
            else:
                # Validate before the shared bearer-auth client can issue the
                # next request. Relative Polygon paths remain supported.
                validated_next_url = _validated_polygon_url(next_url)
                if validated_next_url in seen_urls:
                    raise ProviderError(
                        f"Repeated pagination URL after page {page}",
                        provider="polygon",
                    )
                if page >= MAX_PAGINATION_PAGES:
                    raise ProviderError(
                        "Pagination exceeded maximum of "
                        f"{MAX_PAGINATION_PAGES} pages",
                        provider="polygon",
                    )
                seen_urls.add(validated_next_url)
                url = validated_next_url

        self.logger.debug(f"Total results: {len(all_results)}")
        return all_results

    def get_single(
        self,
        endpoint: str,
        path_params: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: int = DEFAULT_HTTP_TIMEOUT,
        max_retries: int = 3,
    ) -> dict[str, Any] | None:
        """
        Fetch single resource with retry logic.

        Args:
            endpoint: Endpoint key or path
            path_params: URL path parameters
            params: Query parameters
            timeout: Request timeout
            max_retries: Retry attempts for rate limits

        Returns:
            Result data or None if not found
        """
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")

        path = ENDPOINTS.get(endpoint, endpoint)
        if path_params:
            path = path.format(**path_params)

        url = _validated_polygon_url(path)

        query_params: dict[str, Any] = {"apiKey": self.api_key}
        if params:
            query_params.update(params)

        for attempt in range(max_retries):
            try:
                response = self.client.get(
                    url,
                    params=query_params,
                    timeout=timeout,
                )
            except httpx.RequestError as e:
                if attempt < max_retries - 1:
                    self.logger.warning(
                        f"Polygon request failed ({type(e).__name__}); retrying"
                    )
                    time.sleep(1)
                    continue
                raise ProviderError(
                    f"Request failed after {max_retries} attempts",
                    provider="polygon",
                    original_error=e,
                ) from e

            if response.status_code == 404:
                return None

            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 2
                    self.logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                raise ProviderError(
                    f"Rate limited after {max_retries} attempts",
                    provider="polygon",
                )

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                if status_code >= 500 and attempt < max_retries - 1:
                    self.logger.warning(f"Polygon HTTP {status_code}; retrying")
                    time.sleep(1)
                    continue
                raise ProviderError(
                    f"HTTP {status_code} request failed",
                    provider="polygon",
                    original_error=e,
                ) from e

            try:
                data = response.json()
            except (TypeError, ValueError) as e:
                raise ProviderError(
                    "Invalid JSON response",
                    provider="polygon",
                    original_error=e,
                ) from e

            if not isinstance(data, dict):
                raise ProviderError("Invalid response object", provider="polygon")

            if data.get("status") != "OK":
                error = data.get("error", data.get("message", "Unknown error"))
                raise ProviderError(
                    f"API error: {redact_sensitive_text(error)}",
                    provider="polygon",
                )

            results = data.get("results")
            if not isinstance(results, dict):
                raise ProviderError(
                    "Invalid response: expected object results",
                    provider="polygon",
                )
            return cast(dict[str, Any], results)

        raise AssertionError("get_single retry loop exited unexpectedly")

    # Convenience methods for specific data types

    def get_trading_days(self, start_date: str, end_date: str, ticker: str = "AAPL") -> list[str]:
        """Get trading days in date range using ticker as proxy."""
        ticker = validate_ticker(ticker)
        # Pass an explicit high limit + sort like the other aggregate callers
        # (daily.py / add_symbol.py use limit=50000). Polygon's /v2/aggs
        # otherwise defaults to limit=5000 ascending, which would silently
        # truncate ranges longer than ~5000 trading days (~19.8 years) and,
        # because results are ascending, drop the MOST RECENT trading days.
        # The aggregates endpoint is paginated, so follow next_url too.
        path = ENDPOINTS["aggregates"].format(ticker=ticker, start=start_date, end=end_date)
        results = self.get_paginated(
            path,
            params={"adjusted": "true", "limit": 50000, "sort": "asc"},
        )
        from sawa.utils.dates import DATE_FORMAT, timestamp_to_date

        return [timestamp_to_date(r["t"]).strftime(DATE_FORMAT) for r in results if r.get("t")]

    def get_ratios(self, ticker: str, limit: int = 100) -> list[dict[str, Any]]:
        """Get financial ratios for ticker.

        Routes through get_paginated (follows next_url) like the other
        financial-data getters, so a ticker with more than `limit` ratio rows
        is not silently truncated to the first page.
        """
        ticker = validate_ticker(ticker)
        return self.get_paginated("ratios", params={"ticker": ticker, "limit": limit})

    def get_fundamentals(
        self,
        endpoint: str,
        ticker: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: str | None = None,
        filing_date_gte: str | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        """Get fundamentals data (balance sheets, income, cash flow).

        start_date/end_date filter on period_end (when the fiscal period
        closed). filing_date_gte filters on filing_date (when the report
        became available), which the incremental quarterly pull uses to
        catch late filings and restatements of older periods.
        """
        params: dict[str, Any] = {"limit": limit}
        if ticker:
            ticker = validate_ticker(ticker)
            params["tickers"] = ticker
        if start_date:
            params["period_end.gte"] = start_date
        if end_date:
            params["period_end.lte"] = end_date
        if filing_date_gte:
            params["filing_date.gte"] = filing_date_gte
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
        ticker = validate_ticker(ticker)
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
            ticker = validate_ticker(ticker)
            params["ticker"] = ticker
        if published_utc_gte:
            params["published_utc.gte"] = published_utc_gte
        if published_utc_lte:
            params["published_utc.lte"] = published_utc_lte

        return self.get_paginated("news", params)

    def get_splits(
        self,
        ticker: str | None = None,
        execution_date_gte: str | None = None,
        execution_date_lte: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Get stock split history.

        Args:
            ticker: Filter by ticker symbol (e.g., 'AAPL')
            execution_date_gte: Return splits on or after this date (YYYY-MM-DD)
            execution_date_lte: Return splits on or before this date (YYYY-MM-DD)
            limit: Max results per page

        Returns:
            List of stock splits with ticker, execution_date, split_from, split_to
        """
        params: dict[str, Any] = {"limit": limit}
        if ticker:
            ticker = validate_ticker(ticker)
            params["ticker"] = ticker
        if execution_date_gte:
            params["execution_date.gte"] = execution_date_gte
        if execution_date_lte:
            params["execution_date.lte"] = execution_date_lte

        return self.get_paginated("splits", params)

    def get_dividends(
        self,
        ticker: str | None = None,
        ex_dividend_date_gte: str | None = None,
        ex_dividend_date_lte: str | None = None,
        dividend_type: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Get dividend history.

        Args:
            ticker: Filter by ticker symbol (e.g., 'AAPL')
            ex_dividend_date_gte: Return dividends on or after this date (YYYY-MM-DD)
            ex_dividend_date_lte: Return dividends on or before this date (YYYY-MM-DD)
            dividend_type: Filter by type (CD=cash, SC=special cash, etc.)
            limit: Max results per page

        Returns:
            List of dividends with ticker, ex_dividend_date, cash_amount, etc.
        """
        params: dict[str, Any] = {"limit": limit}
        if ticker:
            ticker = validate_ticker(ticker)
            params["ticker"] = ticker
        if ex_dividend_date_gte:
            params["ex_dividend_date.gte"] = ex_dividend_date_gte
        if ex_dividend_date_lte:
            params["ex_dividend_date.lte"] = ex_dividend_date_lte
        if dividend_type:
            params["dividend_type"] = dividend_type

        return self.get_paginated("dividends", params)

    def get_ticker_events(
        self,
        ticker: str,
        event_types: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """
        Get ticker events including earnings dates.

        Args:
            ticker: Ticker symbol (e.g., 'AAPL')
            event_types: Filter by event types (e.g., ['earnings'])

        Returns:
            Dict with events including earnings calendar
        """
        ticker = validate_ticker(ticker)
        params: dict[str, Any] = {}
        if event_types:
            params["types"] = ",".join(event_types)

        return self.get_single("ticker-events", path_params={"ticker": ticker}, params=params)
