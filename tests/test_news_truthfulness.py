"""News provider and persistence failures must remain visible to workflows."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from unittest import mock

import pytest

from sawa import coldstart, daily, weekly
from sawa.database import news


@pytest.fixture(autouse=True)
def _verified_coldstart_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep provider-focused coldstart tests past the schema boundary."""
    monkeypatch.setattr(coldstart, "validate_schema_files", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(coldstart, "verify_tables", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_views", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_materialized_views", lambda _conn: [])


def _connection() -> mock.MagicMock:
    conn = mock.MagicMock(name="news_connection")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    cursor = mock.MagicMock(name="news_cursor")
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    # Model an empty table rather than handing back a MagicMock where the
    # database returns a date; get_last_date is documented to return date|None
    # and callers do date arithmetic on the result.
    cursor.fetchone.return_value = (None,)
    cursor.fetchall.return_value = []
    conn.cursor.return_value = cursor
    return conn


def _all_failed_news() -> news.NewsLoadResult:
    return news.NewsLoadResult(
        0,
        requested=1,
        succeeded=0,
        empty=0,
        fetched_articles=0,
        persisted_articles=0,
        rejected_articles=0,
        failures=(
            news.NewsRequestFailure(
                ticker="AAPL",
                error_type="RuntimeError",
                message="provider unavailable",
            ),
        ),
    )


def _all_empty_news() -> news.NewsLoadResult:
    return news.NewsLoadResult(
        0,
        requested=1,
        succeeded=1,
        empty=1,
        fetched_articles=0,
        persisted_articles=0,
        rejected_articles=0,
    )


def _all_writes_failed_news() -> news.NewsLoadResult:
    return news.NewsLoadResult(
        0,
        requested=1,
        succeeded=1,
        empty=0,
        fetched_articles=2,
        persisted_articles=0,
        rejected_articles=2,
    )


def test_all_symbol_requests_failing_return_typed_outage() -> None:
    secret = "news-provider-secret"
    client = mock.MagicMock()
    client.get_news.side_effect = RuntimeError(
        f"https://provider.invalid/news?apiKey={secret}"
    )
    conn = _connection()

    result = news.fetch_news_for_symbols(
        conn,
        client,
        ["AAPL", "MSFT"],
        log=logging.getLogger(__name__),
    )

    assert int(result) == 0
    assert result.all_requests_failed is True
    assert result.failed == 2
    assert secret not in str(result.failure_details)
    assert "<redacted>" in str(result.failure_details)
    assert conn.rollback.call_count == 2


def test_partial_outage_with_only_empty_successes_has_no_fresh_result() -> None:
    result = news.NewsLoadResult(
        0,
        requested=2,
        succeeded=1,
        empty=1,
        fetched_articles=0,
        persisted_articles=0,
        rejected_articles=0,
        failures=(
            news.NewsRequestFailure("MSFT", "RuntimeError", "unavailable"),
        ),
    )

    assert result.all_successful_empty is False
    assert result.no_articles_fetched is True


def test_all_article_write_failures_are_counted_not_clean_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = mock.MagicMock()
    client.get_news.return_value = [{"id": "one"}, {"id": "two"}]
    conn = _connection()
    monkeypatch.setattr(
        news,
        "load_news_article",
        mock.MagicMock(side_effect=RuntimeError("database write failed")),
    )

    result = news.fetch_and_load_news(
        conn,
        client,
        ticker="AAPL",
        log=logging.getLogger(__name__),
    )

    assert int(result) == 0
    assert result.succeeded == 1
    assert result.fetched_articles == 2
    assert result.persisted_articles == 0
    assert result.rejected_articles == 2
    assert result.persistence_failed is True
    assert result.total_persistence_failure is True


@pytest.mark.parametrize(
    "news_result",
    [_all_failed_news(), _all_empty_news(), _all_writes_failed_news()],
)
def test_news_only_daily_fails_on_untrustworthy_news_outcome(
    news_result: news.NewsLoadResult,
) -> None:
    watermark = date(2026, 8, 28)
    with (
        mock.patch.object(daily.psycopg, "connect", return_value=_connection()),
        mock.patch.object(daily, "PolygonClient"),
        mock.patch.object(daily, "SyncRateLimiter"),
        mock.patch.object(daily, "get_last_date", return_value=None) as get_last,
        mock.patch.object(daily, "_last_date_coverage", return_value=(1, 1)),
        mock.patch.object(daily, "get_market_date", return_value=watermark),
        mock.patch.object(daily, "is_after_market_close", return_value=True),
        mock.patch.object(daily, "get_symbols_from_db", return_value=[]) as get_symbols,
        mock.patch.object(
            daily,
            "_symbol_price_watermarks",
            return_value={"AAPL": watermark},
        ),
        mock.patch.object(
            daily, "fetch_and_load_news", return_value=news_result
        ) as fetch_news,
        mock.patch.object(daily, "get_notifier", return_value=mock.MagicMock()),
    ):
        stats = daily.run_daily(
            api_key="offline-key",
            database_url="offline-db",
            skip_prices=True,
            skip_ta=True,
            skip_market_internals=True,
            news_only=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert "required news-only update failed" in stats["fatal_reasons"]
    get_last.assert_not_called()
    get_symbols.assert_not_called()
    fetch_news.assert_called_once()


def test_news_only_succeeds_without_price_state_or_company_universe() -> None:
    result = news.NewsLoadResult(
        1,
        requested=1,
        succeeded=1,
        empty=0,
        fetched_articles=1,
        persisted_articles=1,
        rejected_articles=0,
    )
    with (
        mock.patch.object(daily.psycopg, "connect", return_value=_connection()),
        mock.patch.object(daily, "PolygonClient"),
        mock.patch.object(daily, "SyncRateLimiter"),
        mock.patch.object(daily, "get_last_date", return_value=None) as get_last,
        mock.patch.object(daily, "get_symbols_from_db", return_value=[]) as get_symbols,
        mock.patch.object(
            daily, "fetch_and_load_news", return_value=result
        ) as fetch_news,
        mock.patch.object(daily, "get_notifier", return_value=mock.MagicMock()),
    ):
        stats = daily.run_daily(
            api_key="offline-key",
            database_url="offline-db",
            skip_prices=True,
            skip_ta=True,
            skip_market_internals=True,
            news_only=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    assert stats["news"] == 1
    get_last.assert_not_called()
    get_symbols.assert_not_called()
    fetch_news.assert_called_once()


def test_daily_surfaces_partial_news_provider_failure_as_degraded() -> None:
    result = news.NewsLoadResult(
        1,
        requested=2,
        succeeded=1,
        empty=0,
        fetched_articles=1,
        persisted_articles=1,
        rejected_articles=0,
        failures=(news.NewsRequestFailure("MSFT", "RuntimeError", "unavailable"),),
    )
    with (
        mock.patch.object(daily.psycopg, "connect", return_value=_connection()),
        mock.patch.object(daily, "PolygonClient"),
        mock.patch.object(daily, "SyncRateLimiter"),
        mock.patch.object(daily, "get_last_date") as get_last,
        mock.patch.object(daily, "get_symbols_from_db") as get_symbols,
        mock.patch.object(daily, "fetch_and_load_news", return_value=result),
        mock.patch.object(daily, "get_notifier", return_value=mock.MagicMock()),
    ):
        stats = daily.run_daily(
            api_key="offline-key",
            database_url="offline-db",
            skip_prices=True,
            skip_ta=True,
            skip_market_internals=True,
            news_only=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    assert stats["degraded"] is True
    assert stats["news_degraded"] == "news provider failed for 1/2 request(s)"
    assert stats["degraded_reasons"] == [stats["news_degraded"]]
    get_last.assert_not_called()
    get_symbols.assert_not_called()


@pytest.mark.parametrize(
    "news_result",
    [_all_failed_news(), _all_empty_news(), _all_writes_failed_news()],
)
def test_weekly_fails_on_untrustworthy_news_outcome(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    news_result: news.NewsLoadResult,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with (
        mock.patch.object(weekly.psycopg, "connect", return_value=_connection()),
        mock.patch.object(weekly, "PolygonClient"),
        mock.patch.object(weekly, "SyncRateLimiter"),
        mock.patch.object(weekly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(weekly, "load_news", return_value=news_result),
        mock.patch.object(weekly, "get_notifier", return_value=mock.MagicMock()),
        mock.patch.object(weekly, "alert_missing_api_key"),
        mock.patch(
            "sawa.mcp_query_insights.analyze_query_log",
            return_value={"summary": {}},
        ),
    ):
        stats = weekly.run_weekly(
            api_key="offline-key",
            database_url="offline-db",
            output_dir=tmp_path,
            skip_overviews=True,
            skip_economy=True,
            skip_corporate_actions=True,
            skip_character=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert "news" in stats["step_errors"]
    assert stats["news_requests"]["requested"] == 1


@pytest.mark.parametrize(
    "news_result",
    [_all_failed_news(), _all_empty_news(), _all_writes_failed_news()],
)
def test_coldstart_fails_on_untrustworthy_news_outcome(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    news_result: news.NewsLoadResult,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("AAPL\n", encoding="utf-8")
    client = mock.MagicMock()
    client.get_trading_days.return_value = []

    with (
        mock.patch.object(coldstart, "PolygonClient", return_value=client),
        mock.patch.object(coldstart, "PolygonS3Client"),
        mock.patch.object(coldstart, "SyncRateLimiter"),
        mock.patch("psycopg.connect", return_value=_connection()),
        mock.patch.object(
            coldstart, "get_sql_files", return_value=[tmp_path / "00_setup.sql"]
        ),
        mock.patch.object(
            coldstart,
            "execute_sql_files_atomically",
            return_value=(0, []),
        ),
        mock.patch.object(coldstart, "load_news", return_value=news_result),
        mock.patch.object(coldstart, "populate_index_constituents", return_value={}),
        mock.patch.object(coldstart, "alert_missing_api_key", create=True),
    ):
        stats = coldstart.run_coldstart(
            api_key="offline-key",
            s3_access_key="offline-access",
            s3_secret_key="offline-secret",
            database_url="offline-db",
            schema_dir=tmp_path / "schema",
            output_dir=tmp_path / "output",
            symbols_file=symbols_file,
            skip_prices=True,
            skip_fundamentals=True,
            skip_overviews=True,
            skip_economy=True,
            skip_ratios=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert "provider step failed (news)" in stats["fatal_reasons"]
    assert stats["news_requests"]["requested"] == 1
