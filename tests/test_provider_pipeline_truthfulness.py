"""Provider outages must not make persistent CSV artifacts look freshly downloaded."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from unittest import mock

import pytest

from sawa import coldstart, quarterly, weekly
from sawa.api.s3 import BulkPriceRows
from sawa.domain.exceptions import ProviderError
from sawa.provider_downloads import bind_provider_record


@pytest.fixture(autouse=True)
def _verified_coldstart_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep provider-focused coldstart tests past the schema boundary."""
    monkeypatch.setattr(coldstart, "validate_schema_files", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(coldstart, "verify_tables", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_views", lambda _conn: [])
    monkeypatch.setattr(coldstart, "verify_materialized_views", lambda _conn: [])


def _connection() -> mock.MagicMock:
    conn = mock.MagicMock(name="offline_connection")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    return conn


class _OutageClient:
    def get_trading_days(self, *_: object, **__: object) -> list[str]:
        return []

    def get_fundamentals(self, *_: object, **__: object) -> list[dict[str, object]]:
        raise RuntimeError("provider unavailable")

    def get_ratios(self, *_: object, **__: object) -> list[dict[str, object]]:
        raise RuntimeError("provider unavailable")

    def get_ticker_details(self, *_: object, **__: object) -> dict[str, object]:
        raise RuntimeError("provider unavailable")

    def get_economy_data(self, *_: object, **__: object) -> list[dict[str, object]]:
        raise RuntimeError("provider unavailable")


def _seed_stale_artifacts(root: Path) -> None:
    artifacts = {
        root / "overviews" / "overviews.csv": "ticker,name\nOLD,Old Company\n",
        root / "fundamentals" / "balance_sheets.csv": (
            "tickers,period_end\nOLD,2020-01-01\n"
        ),
        root / "fundamentals" / "cash_flow.csv": (
            "tickers,period_end\nOLD,2020-01-01\n"
        ),
        root / "fundamentals" / "income_statements.csv": (
            "tickers,period_end\nOLD,2020-01-01\n"
        ),
        root / "ratios" / "ratios.csv": "ticker,date\nOLD,2020-01-01\n",
        root / "economy" / "treasury_yields.csv": "date,value\n2020-01-01,1\n",
    }
    for path, contents in artifacts.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")


def test_bind_provider_record_rejects_mismatch_without_mutating_source() -> None:
    source = {"tickers": ["MSFT"], "value": 1}

    with pytest.raises(ProviderError, match="mismatched ticker"):
        bind_provider_record(source, "AAPL", output_field="tickers")

    assert source == {"tickers": ["MSFT"], "value": 1}


def test_bind_provider_record_rejects_mixed_provider_identities() -> None:
    source = {"tickers": ["AAPL", "MSFT"], "value": 1}

    with pytest.raises(ProviderError, match="mismatched ticker"):
        bind_provider_record(source, "AAPL", output_field="tickers")

    assert source["tickers"] == ["AAPL", "MSFT"]


def test_present_but_empty_provider_identity_is_a_failed_request(
    tmp_path: Path,
) -> None:
    class _EmptyIdentityClient:
        def get_fundamentals(
            self, endpoint: str, *, ticker: str, **kwargs: object
        ) -> list[dict[str, object]]:
            return [{"tickers": [], "period_end": "2026-06-30"}]

        def get_ratios(self, ticker: str) -> list[dict[str, object]]:
            return [{"tickers": [], "date": "2026-06-30"}]

    client = _EmptyIdentityClient()
    fundamentals_dir = tmp_path / "fundamentals"
    ratios_dir = tmp_path / "ratios"

    fundamentals = quarterly.download_fundamentals(
        client,  # type: ignore[arg-type]
        ["AAPL"],
        "2026-01-01",
        "2026-08-01",
        fundamentals_dir,
        logging.getLogger(__name__),
    )
    ratios = quarterly.download_ratios(
        client,  # type: ignore[arg-type]
        ["AAPL"],
        ratios_dir,
        logging.getLogger(__name__),
    )

    assert fundamentals.all_failed is True
    assert all(
        request == {
            "requested": 1,
            "succeeded": 0,
            "failed": 1,
            "rows": 0,
            "artifact_written": False,
        }
        for request in fundamentals.requests.values()
    )
    assert ratios.all_failed is True
    assert ratios.summary() == {
        "requested": 1,
        "succeeded": 0,
        "failed": 1,
        "rows": 0,
        "artifact_written": False,
    }
    assert list(fundamentals_dir.glob("*.csv")) == []
    assert list(ratios_dir.glob("*.csv")) == []


def test_quarterly_outage_does_not_load_stale_artifacts(tmp_path: Path) -> None:
    _seed_stale_artifacts(tmp_path)
    client = _OutageClient()

    with (
        mock.patch.object(quarterly.psycopg, "connect", return_value=_connection()),
        mock.patch.object(quarterly, "PolygonClient", return_value=client),
        mock.patch.object(quarterly, "SyncRateLimiter"),
        mock.patch.object(quarterly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(quarterly, "get_last_date", return_value=date(2026, 1, 1)),
        mock.patch.object(quarterly, "load_fundamentals") as load_fundamentals,
        mock.patch.object(quarterly, "load_ratios") as load_ratios,
    ):
        stats = quarterly.run_quarterly(
            api_key="offline-key",
            database_url="offline-db",
            output_dir=tmp_path,
            logger=logging.getLogger(__name__),
        )

    load_fundamentals.assert_not_called()
    load_ratios.assert_not_called()
    assert stats["success"] is False
    assert set(stats["step_errors"]) == {"fundamentals", "ratios"}
    assert stats["fundamentals_requests"]["balance-sheets"]["failed"] == 1
    assert stats["ratios_requests"]["failed"] == 1


def test_weekly_outage_does_not_load_stale_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    _seed_stale_artifacts(tmp_path)
    client = _OutageClient()

    with (
        mock.patch.object(weekly.psycopg, "connect", return_value=_connection()),
        mock.patch.object(weekly, "PolygonClient", return_value=client),
        mock.patch.object(weekly, "SyncRateLimiter"),
        mock.patch.object(weekly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(weekly, "get_last_date", return_value=date(2026, 1, 1)),
        mock.patch.object(weekly, "load_companies") as load_companies,
        mock.patch.object(weekly, "load_economy") as load_economy,
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
            skip_news=True,
            skip_corporate_actions=True,
            skip_character=True,
            logger=logging.getLogger(__name__),
        )

    load_companies.assert_not_called()
    load_economy.assert_not_called()
    assert stats["success"] is False
    assert set(stats["step_errors"]) == {"economy", "overviews"}
    assert stats["overviews_requests"]["failed"] == 1
    assert stats["economy_requests"]["treasury-yields"]["failed"] == 1


def test_coldstart_outage_does_not_load_stale_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    output_dir = tmp_path / "output"
    _seed_stale_artifacts(output_dir)
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("AAPL\n", encoding="utf-8")
    client = _OutageClient()

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
            return_value=([], []),
        ),
        mock.patch.object(
            coldstart,
            "get_existing_tickers_from_db",
            return_value={"AAPL"},
        ),
        mock.patch.object(coldstart, "load_companies") as load_companies,
        mock.patch.object(coldstart, "load_fundamentals") as load_fundamentals,
        mock.patch.object(coldstart, "load_ratios") as load_ratios,
        mock.patch.object(coldstart, "load_economy") as load_economy,
        mock.patch.object(coldstart, "populate_index_constituents", return_value={}),
    ):
        stats = coldstart.run_coldstart(
            api_key="offline-key",
            s3_access_key="offline-access",
            s3_secret_key="offline-secret",
            database_url="offline-db",
            schema_dir=tmp_path / "schema",
            output_dir=output_dir,
            symbols_file=symbols_file,
            skip_prices=True,
            skip_news=True,
            logger=logging.getLogger(__name__),
        )

    load_companies.assert_not_called()
    load_fundamentals.assert_not_called()
    load_ratios.assert_not_called()
    load_economy.assert_not_called()
    assert stats["success"] is False
    assert set(stats["fatal_reasons"]) == {
        "provider step failed (economy)",
        "provider step failed (fundamentals)",
        "provider step failed (overviews)",
        "provider step failed (ratios)",
    }


def test_partial_fundamentals_download_is_degraded_and_binds_requested_symbol(
    tmp_path: Path,
) -> None:
    provider_row = {"tickers": ["AAPL"], "period_end": "2026-06-30"}

    class _PartialClient:
        def get_fundamentals(
            self, endpoint: str, *, ticker: str, **_: object
        ) -> list[dict[str, object]]:
            if ticker == "MSFT":
                raise RuntimeError("one request failed")
            return [provider_row]

    result = quarterly.download_fundamentals(
        _PartialClient(),  # type: ignore[arg-type]
        ["AAPL", "MSFT"],
        "2026-01-01",
        "2026-08-01",
        tmp_path,
        logging.getLogger(__name__),
    )

    assert result.has_failures is True
    assert result.all_failed is False
    assert result.requests["balance-sheets"] == {
        "requested": 2,
        "succeeded": 1,
        "failed": 1,
        "rows": 1,
        "artifact_written": True,
    }
    assert provider_row == {"tickers": ["AAPL"], "period_end": "2026-06-30"}


def test_quarterly_fails_if_one_whole_feed_fails_but_loads_fresh_siblings(
    tmp_path: Path,
) -> None:
    _seed_stale_artifacts(tmp_path)

    class _OneFeedOutageClient(_OutageClient):
        def get_fundamentals(
            self, endpoint: str, *, ticker: str, **_: object
        ) -> list[dict[str, object]]:
            if endpoint == "balance-sheets":
                raise RuntimeError("feed unavailable")
            return [{"tickers": [ticker], "period_end": "2026-06-30"}]

    with (
        mock.patch.object(quarterly.psycopg, "connect", return_value=_connection()),
        mock.patch.object(
            quarterly,
            "PolygonClient",
            return_value=_OneFeedOutageClient(),
        ),
        mock.patch.object(quarterly, "SyncRateLimiter"),
        mock.patch.object(quarterly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(quarterly, "get_last_date", return_value=date(2026, 1, 1)),
        mock.patch.object(quarterly, "load_fundamentals") as load_fundamentals,
    ):
        stats = quarterly.run_quarterly(
            api_key="offline-key",
            database_url="offline-db",
            output_dir=tmp_path,
            skip_ratios=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    load_fundamentals.assert_called_once()
    assert load_fundamentals.call_args.kwargs["only_tables"] == {
        "cash_flows",
        "income_statements",
    }


def test_weekly_empty_overview_response_is_not_clean_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    _seed_stale_artifacts(tmp_path)
    client = mock.MagicMock()
    client.get_ticker_details.return_value = {}

    with (
        mock.patch.object(weekly.psycopg, "connect", return_value=_connection()),
        mock.patch.object(weekly, "PolygonClient", return_value=client),
        mock.patch.object(weekly, "SyncRateLimiter"),
        mock.patch.object(weekly, "get_symbols_from_db", return_value=["AAPL"]),
        mock.patch.object(weekly, "get_last_date", return_value=date(2026, 1, 1)),
        mock.patch.object(weekly, "load_companies") as load_companies,
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

    load_companies.assert_not_called()
    assert stats["success"] is False
    assert "overviews" in stats["step_errors"]


def test_coldstart_aborts_before_download_when_symbol_source_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("\n# none\n", encoding="utf-8")
    client = mock.MagicMock()

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
            return_value=([], []),
        ),
    ):
        stats = coldstart.run_coldstart(
            api_key="offline-key",
            s3_access_key="offline-access",
            s3_secret_key="offline-secret",
            database_url="offline-db",
            schema_dir=tmp_path / "schema",
            output_dir=tmp_path / "output",
            symbols_file=symbols_file,
            logger=logging.getLogger(__name__),
        )

    assert stats == {"success": False, "error": "no symbols resolved"}
    client.get_trading_days.assert_not_called()


def test_coldstart_missing_price_source_never_loads_stale_price_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    output_dir = tmp_path / "output"
    stale_price = output_dir / "prices" / "OLD.csv"
    stale_price.parent.mkdir(parents=True)
    stale_price.write_text(
        "date,symbol,open,close,high,low,volume\n"
        "2020-01-02,OLD,1,1,1,1,1\n",
        encoding="utf-8",
    )
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("AAPL\n", encoding="utf-8")
    client = mock.MagicMock()
    client.get_trading_days.return_value = ["2026-01-02"]
    s3_client = mock.MagicMock()
    s3_client.download_and_parse.return_value = BulkPriceRows(
        [], source_found=False
    )

    with (
        mock.patch.object(coldstart, "PolygonClient", return_value=client),
        mock.patch.object(coldstart, "PolygonS3Client", return_value=s3_client),
        mock.patch.object(coldstart, "SyncRateLimiter"),
        mock.patch.object(
            coldstart,
            "calculate_date_range",
            return_value=(date(2026, 1, 2), date(2026, 1, 2)),
        ),
        mock.patch("psycopg.connect", return_value=_connection()),
        mock.patch.object(
            coldstart, "get_sql_files", return_value=[tmp_path / "00_setup.sql"]
        ),
        mock.patch.object(
            coldstart,
            "execute_sql_files_atomically",
            return_value=([], []),
        ),
        mock.patch.object(coldstart, "load_prices") as load_prices,
        mock.patch.object(coldstart, "populate_index_constituents", return_value={}),
    ):
        stats = coldstart.run_coldstart(
            api_key="offline-key",
            s3_access_key="offline-access",
            s3_secret_key="offline-secret",
            database_url="offline-db",
            schema_dir=tmp_path / "schema",
            output_dir=output_dir,
            symbols_file=symbols_file,
            skip_fundamentals=True,
            skip_overviews=True,
            skip_economy=True,
            skip_ratios=True,
            skip_news=True,
            logger=logging.getLogger(__name__),
        )

    load_prices.assert_not_called()
    assert stats["success"] is False
    assert "provider step failed (prices)" in stats["fatal_reasons"]
    assert stats["price_requests"]["missing_dates"] == 1
    assert stale_price.read_text(encoding="utf-8").startswith("date,symbol")


def test_price_download_tracks_partial_requested_date_failure(tmp_path: Path) -> None:
    class _PartialS3:
        def download_and_parse(
            self, target_date: date, symbols: set[str]
        ) -> BulkPriceRows:
            if target_date == date(2026, 1, 2):
                return BulkPriceRows(
                    [
                        {
                            "date": "2026-01-02",
                            "symbol": "AAPL",
                            "open": 1,
                            "close": 1,
                            "high": 1,
                            "low": 1,
                            "volume": 1,
                        }
                    ],
                    source_found=True,
                )
            return BulkPriceRows([], source_found=False)

    result = coldstart.download_prices(
        _PartialS3(),  # type: ignore[arg-type]
        {"AAPL"},
        date(2026, 1, 2),
        date(2026, 1, 5),
        ["2026-01-02", "2026-01-05"],
        tmp_path,
        logging.getLogger(__name__),
    )

    assert int(result) == 1
    assert result.requested_dates == 2
    assert result.sourced_dates == 1
    assert result.missing_dates == ("2026-01-05",)
    assert result.has_source_failures is True
    assert result.artifact_files == {"AAPL.csv"}


def test_price_download_does_not_treat_untyped_empty_response_as_sourced(
    tmp_path: Path,
) -> None:
    class _MixedS3:
        def download_and_parse(
            self, target_date: date, symbols: set[str]
        ) -> BulkPriceRows | list[dict[str, object]]:
            if target_date == date(2026, 1, 2):
                return BulkPriceRows(
                    [
                        {
                            "symbol": "AAPL",
                            "open": 1,
                            "close": 1,
                            "high": 1,
                            "low": 1,
                            "volume": 1,
                        }
                    ],
                    source_found=True,
                )
            return []

    result = coldstart.download_prices(
        _MixedS3(),  # type: ignore[arg-type]
        {"AAPL"},
        date(2026, 1, 2),
        date(2026, 1, 5),
        ["2026-01-02", "2026-01-05", "2026-01-05"],
        tmp_path,
        logging.getLogger(__name__),
    )

    assert int(result) == 1
    assert result.requested_dates == 2
    assert result.sourced_dates == 1
    assert result.failed_dates == {"2026-01-05": "untyped bulk price response"}


def test_coldstart_rejects_traversal_symbol_before_provider_or_file_access(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("../outside\n", encoding="utf-8")
    client = mock.MagicMock()

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
            return_value=([], []),
        ),
    ):
        stats = coldstart.run_coldstart(
            api_key="offline-key",
            s3_access_key="offline-access",
            s3_secret_key="offline-secret",
            database_url="offline-db",
            schema_dir=tmp_path / "schema",
            output_dir=tmp_path / "output",
            symbols_file=symbols_file,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert stats["error"] == "invalid symbols in resolved universe"
    assert stats["invalid_symbols"] == ["../outside"]
    client.get_trading_days.assert_not_called()
    assert not (tmp_path / "outside.csv").exists()


def test_price_download_rejects_unrequested_traversal_row_without_writing(
    tmp_path: Path,
) -> None:
    class _MaliciousS3:
        def download_and_parse(
            self, target_date: date, symbols: set[str]
        ) -> BulkPriceRows:
            return BulkPriceRows(
                [
                    {
                        "symbol": "../outside",
                        "open": 1,
                        "close": 1,
                        "high": 1,
                        "low": 1,
                        "volume": 1,
                    }
                ],
                source_found=True,
            )

    staging = tmp_path / "stage"
    result = coldstart.download_prices(
        _MaliciousS3(),  # type: ignore[arg-type]
        {"AAPL"},
        date(2026, 1, 2),
        date(2026, 1, 2),
        ["2026-01-02"],
        staging,
        logging.getLogger(__name__),
    )

    assert int(result) == 0
    assert result.failed_dates == {
        "2026-01-02": "invalid bulk price data: Invalid ticker format: ../OUTSIDE"
    }
    assert not (tmp_path / "outside.csv").exists()
    assert list(staging.glob("*.csv")) == []
