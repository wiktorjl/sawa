"""Offline persistence-contract regressions for provider CSV loaders."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from unittest import mock

import pytest

from sawa import coldstart, quarterly, weekly
from sawa.database import load as database_load
from sawa.provider_downloads import DownloadCount


@pytest.fixture(autouse=True)
def _verified_coldstart_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep loader-focused coldstart tests past the schema boundary."""
    monkeypatch.setattr(coldstart, "validate_schema_files", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(coldstart, "verify_tables", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_views", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_materialized_views", lambda _conn: [])


def _schema_connection(*columns: str) -> mock.MagicMock:
    conn = mock.MagicMock(name="offline_schema_connection")
    cursor = mock.MagicMock(name="offline_schema_cursor")
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    cursor.fetchall.return_value = [(column,) for column in columns]
    conn.cursor.return_value = cursor
    return conn


def test_company_loader_surfaces_all_row_database_failure(tmp_path: Path) -> None:
    csv_path = tmp_path / "overviews.csv"
    csv_path.write_text("ticker,name\nAAPL,Apple\n", encoding="utf-8")

    with mock.patch.object(database_load, "_insert_rows", return_value=0) as insert:
        with pytest.raises(RuntimeError, match="Persisted only 0/1 companies"):
            database_load.load_companies(object(), csv_path)

    assert insert.call_args.kwargs["strict"] is True


def test_ratio_loader_surfaces_all_row_database_failure(tmp_path: Path) -> None:
    csv_path = tmp_path / "ratios.csv"
    csv_path.write_text("ticker,date\nAAPL,2026-08-01\n", encoding="utf-8")

    with mock.patch.object(database_load, "_insert_rows", return_value=0) as insert:
        with pytest.raises(RuntimeError, match="Persisted only 0/1 financial_ratios"):
            database_load.load_ratios(object(), csv_path)

    assert insert.call_args.kwargs["strict"] is True


def test_fundamental_loader_surfaces_all_row_database_failure(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "balance_sheets.csv"
    csv_path.write_text(
        "tickers,period_end\nAAPL,2026-06-30\n",
        encoding="utf-8",
    )
    conn = _schema_connection("ticker", "period_end")

    with mock.patch.object(database_load, "_insert_rows", return_value=0) as insert:
        with pytest.raises(RuntimeError, match="Persisted only 0/1 balance_sheets"):
            database_load.load_fundamentals(
                conn,
                tmp_path,
                only_tables={"balance_sheets"},
            )

    assert insert.call_args.kwargs["strict"] is True


def test_economy_loader_surfaces_all_row_database_failure(tmp_path: Path) -> None:
    csv_path = tmp_path / "treasury_yields.csv"
    csv_path.write_text("date,yield_10_year\n2026-08-01,4.25\n", encoding="utf-8")
    conn = _schema_connection("date", "yield_10_year")

    with mock.patch.object(database_load, "_insert_rows", return_value=0) as insert:
        with pytest.raises(RuntimeError, match="Persisted only 0/1 treasury_yields"):
            database_load.load_economy(
                conn,
                tmp_path,
                only_tables={"treasury_yields"},
            )

    assert insert.call_args.kwargs["strict"] is True


def test_price_loader_surfaces_per_file_all_row_database_failure(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "AAPL.csv"
    csv_path.write_text(
        "symbol,date,open,high,low,close,volume\n"
        "AAPL,2026-08-01,1,1,1,1,1\n",
        encoding="utf-8",
    )

    with mock.patch.object(database_load, "_insert_rows", return_value=0) as insert:
        with pytest.raises(RuntimeError, match="Persisted only 0/1 stock_prices"):
            database_load.load_prices(object(), tmp_path)

    assert insert.call_args.kwargs["strict"] is True


def test_empty_price_artifact_is_not_accepted(tmp_path: Path) -> None:
    (tmp_path / "AAPL.csv").write_text(
        "symbol,date,open,high,low,close,volume\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="persisted no rows"):
        database_load.load_prices(object(), tmp_path)


def test_missing_requested_economy_artifact_is_structured_and_rejected(
    tmp_path: Path,
) -> None:
    results = database_load.load_economy(
        object(),
        tmp_path,
        only_tables={"treasury_yields"},
    )
    result = results["treasury_yields"]

    assert isinstance(result, database_load.PersistenceResult)
    assert result.artifact_found is False
    assert result.fully_persisted is False
    with pytest.raises(RuntimeError, match="artifact was not found"):
        database_load.require_complete_persistence(result, expected_rows=1)


def test_successful_loader_returns_structured_persistence_counts(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "overviews.csv"
    csv_path.write_text("ticker,name\nAAPL,Apple\n", encoding="utf-8")

    with mock.patch.object(database_load, "_insert_rows", return_value=1):
        result = database_load.load_companies(
            object(), csv_path, logging.getLogger(__name__)
        )

    assert int(result) == 1
    assert result.summary() == {
        "table": "companies",
        "artifact_found": True,
        "source_rows": 1,
        "eligible_rows": 1,
        "inserted_rows": 1,
        "skipped_rows": 0,
        "failed_rows": 0,
        "fully_persisted": True,
    }


def test_strict_ratio_loader_rejects_filtered_rows_before_writing(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "ratios.csv"
    csv_path.write_text("ticker,date\nOLD,2026-08-01\n", encoding="utf-8")

    with mock.patch.object(database_load, "_insert_rows") as insert:
        with pytest.raises(ValueError, match="eligibility filtering"):
            database_load.load_ratios(
                object(),
                csv_path,
                valid_tickers={"AAPL"},
            )

    insert.assert_not_called()


def test_quarterly_rejects_truncated_fresh_ratio_artifact(tmp_path: Path) -> None:
    downloaded = DownloadCount(
        1,
        requested=1,
        succeeded=1,
        failed=0,
        artifact_written=True,
    )
    truncated = database_load.PersistenceResult(
        0,
        table="financial_ratios",
        artifact_found=True,
        source_rows=0,
        eligible_rows=0,
    )

    with (
        mock.patch.object(quarterly.psycopg, "connect", return_value=mock.MagicMock()),
        mock.patch.object(quarterly, "PolygonClient"),
        mock.patch.object(quarterly, "SyncRateLimiter"),
        mock.patch.object(quarterly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(quarterly, "get_last_date", return_value=date(2026, 1, 1)),
        mock.patch.object(quarterly, "download_ratios", return_value=downloaded),
        mock.patch.object(quarterly, "load_ratios", return_value=truncated),
    ):
        stats = quarterly.run_quarterly(
            api_key="offline-key",
            database_url="offline-db",
            output_dir=tmp_path,
            skip_fundamentals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert "ratios" in stats["step_errors"]


def test_weekly_rejects_truncated_fresh_overview_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    downloaded = DownloadCount(
        1,
        requested=1,
        succeeded=1,
        failed=0,
        artifact_written=True,
    )
    truncated = database_load.PersistenceResult(
        0,
        table="companies",
        artifact_found=True,
        source_rows=0,
        eligible_rows=0,
    )

    with (
        mock.patch.object(weekly.psycopg, "connect", return_value=mock.MagicMock()),
        mock.patch.object(weekly, "PolygonClient"),
        mock.patch.object(weekly, "SyncRateLimiter"),
        mock.patch.object(weekly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(weekly, "get_last_date", return_value=date(2026, 1, 1)),
        mock.patch.object(weekly, "download_overviews", return_value=downloaded),
        mock.patch.object(weekly, "load_companies", return_value=truncated),
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
            skip_economy=True,
            skip_news=True,
            skip_corporate_actions=True,
            skip_character=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert "overviews" in stats["step_errors"]


def test_weekly_redacts_step_and_query_insight_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    step_secret = "weekly-step-secret"
    query_secret = "weekly-query-secret"
    notifier = mock.MagicMock()
    logger = logging.getLogger("test_weekly_failure_redaction")
    caplog.set_level(logging.WARNING, logger=logger.name)

    with (
        mock.patch.object(weekly.psycopg, "connect", return_value=mock.MagicMock()),
        mock.patch.object(weekly, "PolygonClient"),
        mock.patch.object(weekly, "SyncRateLimiter"),
        mock.patch.object(weekly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(weekly, "get_last_date", return_value=date(2026, 1, 1)),
        mock.patch.object(
            weekly,
            "download_overviews",
            side_effect=RuntimeError(
                f"https://provider.invalid/data?api_key={step_secret}"
            ),
        ),
        mock.patch.object(weekly, "get_notifier", return_value=notifier),
        mock.patch.object(weekly, "alert_missing_api_key"),
        mock.patch(
            "sawa.mcp_query_insights.analyze_query_log",
            side_effect=RuntimeError(f"token={query_secret}"),
        ),
    ):
        stats = weekly.run_weekly(
            api_key="offline-key",
            database_url="offline-db",
            output_dir=tmp_path,
            skip_economy=True,
            skip_news=True,
            skip_corporate_actions=True,
            skip_character=True,
            logger=logger,
        )

    notification_bodies = " ".join(
        call.kwargs["body"] for call in notifier.send.call_args_list
    )
    visible_output = f"{stats} {caplog.text} {notification_bodies}"
    assert step_secret not in visible_output
    assert query_secret not in visible_output
    assert "<redacted>" in visible_output


def test_coldstart_offline_mode_requires_company_and_price_caches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    conn = mock.MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None

    with (
        mock.patch("psycopg.connect", return_value=conn),
        mock.patch.object(
            coldstart, "get_sql_files", return_value=[tmp_path / "00_setup.sql"]
        ),
        mock.patch.object(
            coldstart,
            "execute_sql_files_atomically",
            return_value=([], []),
        ),
        mock.patch.object(coldstart, "get_tickers_from_csv_files", return_value=set()),
        mock.patch.object(
            coldstart,
            "get_existing_tickers_from_db",
            return_value=set(),
        ),
    ):
        stats = coldstart.run_coldstart(
            api_key=None,
            s3_access_key=None,
            s3_secret_key=None,
            database_url="offline-db",
            schema_dir=tmp_path / "schema",
            output_dir=tmp_path / "output",
            skip_downloads=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert set(stats["fatal_reasons"]) == {
        "provider step failed (overviews)",
        "provider step failed (prices)",
    }
