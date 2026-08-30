"""Mocked pipeline tests for FRED degradation statistics."""

import importlib
import logging
import sys
from datetime import date
from pathlib import Path
from unittest import mock

import psycopg
import pytest

from sawa import coldstart, daily, weekly
from sawa.api.cboe import CboeMarketInternalsResult, CboeQuoteFailure
from sawa.api.fred import FredMarketInternalsResult, FredSeriesFailure


@pytest.fixture(autouse=True)
def _verified_coldstart_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep FRED-focused coldstart tests past the schema boundary."""
    monkeypatch.setattr(coldstart, "validate_schema_files", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        coldstart,
        "execute_sql_files_atomically",
        lambda *_args, **_kwargs: ([], []),
    )
    monkeypatch.setattr(coldstart, "verify_tables", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_views", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_materialized_views", lambda _conn: [])


def _connection() -> mock.MagicMock:
    conn = mock.MagicMock(name="mock_connection")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    return conn


def _failure(field: str, series_id: str) -> FredSeriesFailure:
    return FredSeriesFailure(
        field=field,
        series_id=series_id,
        error_type="ReadTimeout",
        message="timed out",
    )


def _partial_result() -> FredMarketInternalsResult:
    return FredMarketInternalsResult(
        rows=[
            {
                "date": "2026-08-28",
                "vix": "18.25",
                "vix3m": None,
                "hy_spread": "3.10",
            }
        ],
        failures=(_failure("vix3m", "VXVCLS"),),
    )


def _all_failed_result() -> FredMarketInternalsResult:
    return FredMarketInternalsResult(
        rows=[],
        failures=(
            _failure("vix", "VIXCLS"),
            _failure("vix3m", "VXVCLS"),
            _failure("hy_spread", "BAMLH0A0HYM2"),
        ),
    )


def _clean_result() -> FredMarketInternalsResult:
    return FredMarketInternalsResult(
        rows=[
            {
                "date": "2026-08-28",
                "vix": "18.25",
                "vix3m": "20.00",
                "hy_spread": "3.10",
            }
        ]
    )


def _cboe_failure(field: str, symbol: str) -> CboeQuoteFailure:
    return CboeQuoteFailure(
        symbol=symbol,
        field=field,
        error_type="ReadTimeout",
        message="timed out",
    )


def test_daily_partial_fred_result_loads_rows_and_is_nonfatal(
    monkeypatch,
) -> None:
    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    result = _partial_result()
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = result
    cboe = mock.MagicMock()
    cboe.__enter__.return_value.get_market_internals.return_value = []
    notifier = mock.MagicMock()

    with mock.patch.object(daily.psycopg, "connect", return_value=_connection()), mock.patch.object(
        daily, "PolygonClient"
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 8, 27)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(1, 1)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 8, 28)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["AAPL"]
    ), mock.patch.object(
        daily, "FredClient", return_value=fred
    ), mock.patch.object(
        daily, "CboeClient", return_value=cboe
    ), mock.patch.object(
        daily, "get_notifier", return_value=notifier
    ), mock.patch(
        "sawa.database.load.load_market_internals", return_value=1
    ) as load_rows:
        stats = daily.run_daily(
            api_key="offline-key",
            database_url="mock-db",
            skip_prices=True,
            skip_news=True,
            skip_ta=True,
            logger=logging.getLogger(__name__),
        )

    load_rows.assert_called_once_with(mock.ANY, result.rows, mock.ANY)
    assert stats["market_internals"] == 1
    assert stats["market_internals_failures"] == result.failure_details
    assert stats["degraded"] is True
    assert stats["success"] is True
    assert stats["fatal_reasons"] == []
    assert "market internals partial FRED series failure" in stats["degraded_reasons"]
    notifier.send.assert_called_once()


def test_daily_all_fred_failures_stay_nonfatal_and_load_cboe_supplement(
    monkeypatch,
) -> None:
    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    result = _all_failed_result()
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = result
    cboe = mock.MagicMock()
    cboe.__enter__.return_value.get_market_internals.return_value = [
        {"date": "2026-08-28", "vix": "17.50", "vix3m": "19.25"}
    ]
    notifier = mock.MagicMock()

    with (
        mock.patch.object(daily.psycopg, "connect", return_value=_connection()),
        mock.patch.object(daily, "PolygonClient"),
        mock.patch.object(daily, "SyncRateLimiter"),
        mock.patch.object(daily, "get_last_date", return_value=date(2026, 8, 27)),
        mock.patch.object(daily, "_last_date_coverage", return_value=(1, 1)),
        mock.patch.object(daily, "get_market_date", return_value=date(2026, 8, 28)),
        mock.patch.object(daily, "is_after_market_close", return_value=True),
        mock.patch.object(daily, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(daily, "FredClient", return_value=fred),
        mock.patch.object(daily, "CboeClient", return_value=cboe),
        mock.patch.object(daily, "get_notifier", return_value=notifier),
        mock.patch("sawa.database.load.load_market_internals", return_value=1) as load_rows,
    ):
        stats = daily.run_daily(
            api_key="offline-key",
            database_url="mock-db",
            skip_prices=True,
            skip_news=True,
            skip_ta=True,
            logger=logging.getLogger(__name__),
        )

    load_rows.assert_called_once_with(
        mock.ANY,
        [
            {
                "date": "2026-08-28",
                "vix": "17.50",
                "vix3m": "19.25",
                "hy_spread": None,
            }
        ],
        mock.ANY,
    )
    assert stats["market_internals"] == 1
    assert stats["market_internals_error"] == "all FRED series failed"
    assert stats["degraded"] is True
    assert stats["success"] is True
    assert stats["fatal_reasons"] == []
    assert "market internals failed (all FRED series)" in stats["degraded_reasons"]
    notifier.send.assert_called_once()


def _run_daily_market_case(
    monkeypatch,
    fred_result: FredMarketInternalsResult,
    cboe_result: CboeMarketInternalsResult,
    *,
    load_error: Exception | None = None,
) -> tuple[dict[str, object], mock.MagicMock]:
    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = fred_result
    cboe = mock.MagicMock()
    cboe.__enter__.return_value.get_market_internals.return_value = cboe_result
    notifier = mock.MagicMock()
    load_kwargs: dict[str, object]
    if load_error is None:
        load_kwargs = {"return_value": len(fred_result.rows)}
    else:
        load_kwargs = {"side_effect": load_error}

    with (
        mock.patch.object(daily.psycopg, "connect", return_value=_connection()),
        mock.patch.object(daily, "PolygonClient"),
        mock.patch.object(daily, "SyncRateLimiter"),
        mock.patch.object(daily, "get_last_date", return_value=date(2026, 8, 27)),
        mock.patch.object(daily, "_last_date_coverage", return_value=(1, 1)),
        mock.patch.object(daily, "get_market_date", return_value=date(2026, 8, 28)),
        mock.patch.object(daily, "is_after_market_close", return_value=True),
        mock.patch.object(daily, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(daily, "FredClient", return_value=fred),
        mock.patch.object(daily, "CboeClient", return_value=cboe),
        mock.patch.object(daily, "get_notifier", return_value=notifier),
        mock.patch("sawa.database.load.load_market_internals", **load_kwargs) as load_rows,
    ):
        stats = daily.run_daily(
            api_key="offline-key",
            database_url="mock-db",
            skip_prices=True,
            skip_news=True,
            skip_ta=True,
            logger=logging.getLogger(__name__),
        )

    return stats, load_rows


def test_daily_clean_fred_and_partial_cboe_is_truthfully_degraded(
    monkeypatch,
) -> None:
    failure = _cboe_failure("vix", "_VIX")
    cboe_result = CboeMarketInternalsResult(
        [{"date": "2026-08-28", "vix3m": 20.10}],
        failures=(failure,),
    )

    stats, _ = _run_daily_market_case(monkeypatch, _clean_result(), cboe_result)

    assert stats["cboe_market_internals_failures"] == [failure.to_dict()]
    assert stats["cboe_market_internals_degraded"] is True
    assert "market_internals_failures" not in stats
    assert stats["degraded_reasons"] == [
        "CBOE market internals partial quote failure"
    ]
    assert stats["success"] is True


def test_daily_clean_fred_and_total_cboe_outage_is_truthfully_degraded(
    monkeypatch,
) -> None:
    failures = (
        _cboe_failure("vix", "_VIX"),
        _cboe_failure("vix3m", "_VIX3M"),
    )
    cboe_result = CboeMarketInternalsResult(failures=failures)

    stats, _ = _run_daily_market_case(monkeypatch, _clean_result(), cboe_result)

    assert stats["cboe_market_internals_error"] == "all CBOE quotes failed"
    assert stats["degraded_reasons"] == ["CBOE market internals supplement failed"]
    assert stats["success"] is True


def test_daily_market_internal_write_failure_cannot_report_clean_success(
    monkeypatch,
) -> None:
    cboe_result = CboeMarketInternalsResult(
        [{"date": "2026-08-28", "vix": 18.25, "vix3m": 20.0}]
    )

    stats, load_rows = _run_daily_market_case(
        monkeypatch,
        _clean_result(),
        cboe_result,
        load_error=RuntimeError("atomic write failed"),
    )

    load_rows.assert_called_once()
    assert stats["market_internals"] == 0
    assert "atomic write failed" in str(stats["market_internals_load_error"])
    assert stats["degraded_reasons"] == ["market internals persistence failed"]
    assert stats["degraded"] is True
    assert stats["success"] is True


def test_weekly_partial_fred_result_is_visible_but_not_a_required_step_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    result = _partial_result()
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = result
    notifier = mock.MagicMock()

    with (
        mock.patch.object(weekly.psycopg, "connect", return_value=_connection()),
        mock.patch.object(weekly, "PolygonClient"),
        mock.patch.object(weekly, "SyncRateLimiter"),
        mock.patch.object(weekly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(weekly, "get_last_date", return_value=date(2026, 8, 27)),
        mock.patch.object(weekly, "FredClient", return_value=fred),
        mock.patch.object(weekly, "load_market_internals", return_value=1) as load_rows,
        mock.patch.object(weekly, "get_notifier", return_value=notifier),
        mock.patch(
            "sawa.mcp_query_insights.analyze_query_log", return_value={"summary": {}}
        ),
    ):
        stats = weekly.run_weekly(
            api_key="offline-key",
            database_url="mock-db",
            output_dir=tmp_path,
            skip_economy=True,
            skip_overviews=True,
            skip_news=True,
            skip_corporate_actions=True,
            skip_character=True,
            logger=logging.getLogger(__name__),
        )

    load_rows.assert_called_once_with(mock.ANY, result.rows, mock.ANY)
    assert stats["market_internals_failures"] == result.failure_details
    assert stats["degraded"] is True
    assert stats["success"] is True
    assert "step_errors" not in stats
    assert stats["degraded_reasons"] == [
        "market internals partial FRED series failure: vix3m (ReadTimeout)"
    ]
    notifier.send.assert_called_once()


def test_weekly_market_internal_write_failure_is_degraded(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = _clean_result()

    with (
        mock.patch.object(weekly.psycopg, "connect", return_value=_connection()),
        mock.patch.object(weekly, "PolygonClient"),
        mock.patch.object(weekly, "SyncRateLimiter"),
        mock.patch.object(weekly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(weekly, "get_last_date", return_value=date(2026, 8, 27)),
        mock.patch.object(weekly, "FredClient", return_value=fred),
        mock.patch.object(
            weekly,
            "load_market_internals",
            side_effect=RuntimeError("atomic write failed"),
        ),
        mock.patch.object(weekly, "get_notifier", return_value=mock.MagicMock()),
        mock.patch(
            "sawa.mcp_query_insights.analyze_query_log", return_value={"summary": {}}
        ),
    ):
        stats = weekly.run_weekly(
            api_key="offline-key",
            database_url="mock-db",
            output_dir=tmp_path,
            skip_economy=True,
            skip_overviews=True,
            skip_news=True,
            skip_corporate_actions=True,
            skip_character=True,
            logger=logging.getLogger(__name__),
        )

    assert "atomic write failed" in stats["market_internals_error"]
    assert stats["market_internals_degraded"] is True
    assert "market internals update failed" in stats["degraded_reasons"]
    assert stats["degraded"] is True
    assert stats["success"] is True


def test_coldstart_all_fred_series_failure_is_degraded_and_nonfatal(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("AAPL\n")
    result = _all_failed_result()
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = result
    polygon = mock.MagicMock()
    polygon.get_trading_days.return_value = []
    notifier = mock.MagicMock()

    with mock.patch.object(psycopg, "connect", return_value=_connection()), mock.patch.object(
        coldstart, "PolygonClient", return_value=polygon
    ), mock.patch.object(coldstart, "PolygonS3Client"), mock.patch.object(
        coldstart, "SyncRateLimiter"
    ), mock.patch.object(
        coldstart, "get_sql_files", return_value=[tmp_path / "00_setup.sql"]
    ), mock.patch.object(
        coldstart, "get_existing_tickers_from_db", return_value={"AAPL"}
    ), mock.patch.object(
        coldstart, "download_economy", return_value={}
    ), mock.patch.object(
        coldstart, "load_economy"
    ), mock.patch.object(
        coldstart, "FredClient", return_value=fred
    ), mock.patch.object(
        coldstart, "load_market_internals"
    ) as load_rows, mock.patch.object(
        coldstart, "populate_index_constituents", return_value={}
    ), mock.patch.object(
        coldstart, "get_notifier", return_value=notifier
    ):
        stats = coldstart.run_coldstart(
            api_key="offline-key",
            s3_access_key="offline-access",
            s3_secret_key="offline-secret",
            database_url="mock-db",
            schema_dir=tmp_path / "schema",
            output_dir=tmp_path / "output",
            symbols_file=symbols_file,
            drop_tables=False,
            skip_prices=True,
            skip_fundamentals=True,
            skip_overviews=True,
            skip_economy=False,
            skip_ratios=True,
            skip_news=True,
            logger=logging.getLogger(__name__),
        )

    load_rows.assert_not_called()
    assert stats["market_internals"] == 0
    assert stats["market_internals_error"] == "all FRED series failed"
    assert stats["market_internals_failures"] == result.failure_details
    assert stats["degraded"] is True
    assert stats["success"] is True
    assert stats["degraded_reasons"] == [
        "market internals failed (all FRED series)"
    ]
    notifier.send.assert_called_once()


def test_coldstart_market_internal_write_failure_aborts_success(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("AAPL\n", encoding="utf-8")
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = _clean_result()
    polygon = mock.MagicMock()
    polygon.get_trading_days.return_value = []

    with (
        mock.patch.object(psycopg, "connect", return_value=_connection()),
        mock.patch.object(coldstart, "PolygonClient", return_value=polygon),
        mock.patch.object(coldstart, "PolygonS3Client"),
        mock.patch.object(coldstart, "SyncRateLimiter"),
        mock.patch.object(
            coldstart, "get_sql_files", return_value=[tmp_path / "00_setup.sql"]
        ),
        mock.patch.object(
            coldstart, "get_existing_tickers_from_db", return_value={"AAPL"}
        ),
        mock.patch.object(coldstart, "download_economy", return_value={}),
        mock.patch.object(coldstart, "load_economy"),
        mock.patch.object(coldstart, "FredClient", return_value=fred),
        mock.patch.object(
            coldstart,
            "load_market_internals",
            side_effect=RuntimeError("atomic write failed"),
        ),
        mock.patch.object(coldstart, "populate_index_constituents", return_value={}),
        mock.patch.object(coldstart, "get_notifier", return_value=mock.MagicMock()),
    ):
        with pytest.raises(RuntimeError, match="atomic write failed"):
            coldstart.run_coldstart(
                api_key="offline-key",
                s3_access_key="offline-access",
                s3_secret_key="offline-secret",
                database_url="mock-db",
                schema_dir=tmp_path / "schema",
                output_dir=tmp_path / "output",
                symbols_file=symbols_file,
                drop_tables=False,
                skip_prices=True,
                skip_fundamentals=True,
                skip_overviews=True,
                skip_economy=False,
                skip_ratios=True,
                skip_news=True,
                logger=logging.getLogger(__name__),
            )


def test_backfill_all_fred_series_failure_returns_one_without_loading(
    monkeypatch,
) -> None:
    # Importing the script normally calls load_dotenv at module scope. Patch it
    # before import/reload so this regression never reads a configured URL.
    with mock.patch("dotenv.load_dotenv", return_value=False):
        module_name = "scripts.backfill_market_internals"
        if module_name in sys.modules:
            backfill = importlib.reload(sys.modules[module_name])
        else:
            backfill = importlib.import_module(module_name)

    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    monkeypatch.setenv("DATABASE_URL", "mock-db")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_market_internals.py",
            "--start-date",
            "2026-08-01",
            "--end-date",
            "2026-08-28",
        ],
    )

    conn = _connection()
    cursor = mock.MagicMock(name="mock_cursor")
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    cursor.fetchone.return_value = (True,)
    conn.cursor.return_value = cursor
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = _all_failed_result()

    with mock.patch.object(psycopg, "connect", return_value=conn) as connect, mock.patch.object(
        backfill, "FredClient", return_value=fred
    ), mock.patch.object(
        backfill, "load_market_internals"
    ) as load_rows, mock.patch.object(
        backfill, "setup_logging", return_value=logging.getLogger(__name__)
    ):
        exit_code = backfill.main()

    assert exit_code == 1
    connect.assert_called_once_with("mock-db")
    fred.get_market_internals.assert_called_once_with("2026-08-01", "2026-08-28")
    fred.close.assert_called_once()
    load_rows.assert_not_called()


def test_backfill_market_internal_write_failure_returns_one(monkeypatch) -> None:
    with mock.patch("dotenv.load_dotenv", return_value=False):
        module_name = "scripts.backfill_market_internals"
        if module_name in sys.modules:
            backfill = importlib.reload(sys.modules[module_name])
        else:
            backfill = importlib.import_module(module_name)

    monkeypatch.setenv("FRED_API_KEY", "offline-test-key")
    monkeypatch.setenv("DATABASE_URL", "mock-db")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_market_internals.py",
            "--start-date",
            "2026-08-01",
            "--end-date",
            "2026-08-28",
        ],
    )

    conn = _connection()
    cursor = mock.MagicMock(name="mock_cursor")
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    cursor.fetchone.return_value = (True,)
    conn.cursor.return_value = cursor
    fred = mock.MagicMock()
    fred.get_market_internals.return_value = _clean_result()

    with (
        mock.patch.object(psycopg, "connect", return_value=conn),
        mock.patch.object(backfill, "FredClient", return_value=fred),
        mock.patch.object(
            backfill,
            "load_market_internals",
            side_effect=RuntimeError("atomic write failed"),
        ) as load_rows,
        mock.patch.object(
            backfill, "setup_logging", return_value=logging.getLogger(__name__)
        ),
    ):
        exit_code = backfill.main()

    assert exit_code == 1
    load_rows.assert_called_once_with(mock.ANY, _clean_result().rows, mock.ANY)
