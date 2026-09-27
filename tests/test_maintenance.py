"""Offline coverage for recurring maintenance and independent recovery."""

import csv
import logging
from datetime import date
from types import SimpleNamespace
from unittest import mock

import pytest

from sawa import coldstart, earnings, maintenance, quarterly, weekly

LOG = logging.getLogger(__name__)


def connection():
    conn = mock.MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = conn.cursor.return_value
    return conn


def test_fundamentals_track_each_ticker_and_feed_independently(tmp_path):
    conn = connection()
    conn.cursor.return_value.fetchall.side_effect = [
        [("AAPL", date(2026, 9, 1)), ("MSFT", date(2025, 1, 1))],
        [("AAPL", date(2026, 8, 1))],
        [],
    ]
    windows = quarterly.get_fundamental_start_dates(conn, date(2026, 9, 27))
    client = mock.Mock()
    client.get_fundamentals.return_value = []
    quarterly.download_fundamentals(
        client, ["AAPL", "MSFT"], None, "2026-09-27", tmp_path, LOG, filing_start_dates=windows
    )
    calls = {
        (call.args[0], call.kwargs["ticker"]): call.kwargs
        for call in client.get_fundamentals.call_args_list
    }
    assert calls["balance-sheets", "AAPL"]["filing_date_gte"] == "2026-05-04"
    assert calls["balance-sheets", "MSFT"]["filing_date_gte"] == "2024-09-03"
    assert calls["cash-flow", "AAPL"]["filing_date_gte"] == "2026-04-03"
    # Missing siblings get all available history, not the newest balance watermark.
    assert calls["cash-flow", "MSFT"]["filing_date_gte"] is None
    assert calls["income-statements", "AAPL"]["start_date"] is None
    assert calls["income-statements", "AAPL"]["filing_date_gte"] is None


def test_macro_revision_window_and_treasury_allowlist(tmp_path):
    with mock.patch.object(weekly, "get_last_date", return_value=date(2026, 8, 1)):
        windows = weekly.get_economy_start_dates(object(), date(2026, 9, 27))
    assert set(windows.values()) == {"2025-08-01"}
    client = mock.Mock()
    client.get_economy_data.return_value = [{"date": "2025-09-01"}]
    outcome = weekly.download_economy(
        client, "2025-08-01", "2026-09-27", tmp_path, LOG, endpoints={"treasury-yields"}
    )
    assert outcome.artifacts == {"treasury_yields"}
    client.get_economy_data.assert_called_once_with("treasury-yields", "2025-08-01", "2026-09-27")


def test_full_history_load_orders_amendments_after_original_filings(tmp_path):
    client = mock.Mock()
    client.get_fundamentals.return_value = [
        {"tickers": ["AAPL"], "filing_date": "2026-09-01", "total_assets": 20},
        {"tickers": ["AAPL"], "filing_date": "2026-08-01", "total_assets": 10},
    ]
    quarterly.download_fundamentals(
        client, ["AAPL"], None, "2026-09-27", tmp_path, LOG, filing_start_dates={}
    )
    with (tmp_path / "balance_sheets.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert [row["total_assets"] for row in rows] == ["10", "20"]
    assert all(
        call.kwargs["filing_date_gte"] is None for call in client.get_fundamentals.call_args_list
    )


def test_maintenance_keeps_independent_stages_running_and_retries_reconciliation(tmp_path):
    with (
        mock.patch.object(maintenance, "refresh_universe", side_effect=RuntimeError("offline")),
        mock.patch.object(maintenance, "run_quarterly", return_value={"success": False}) as q,
        mock.patch.object(maintenance, "run_earnings_update", return_value={"success": True}) as e,
    ):
        result = maintenance.run_maintenance("key", "db", tmp_path)
    assert set(result["errors"]) == {"universe", "fundamentals"}
    assert not result["success"]
    assert q.call_args.kwargs["full_history"] is True
    e.assert_called_once()
    assert not (tmp_path / ".fundamentals_reconciled").exists()


def test_successful_reconciliation_survives_unrelated_stage_failure(tmp_path):
    with (
        mock.patch.object(maintenance, "run_quarterly", return_value={"success": True}) as q,
        mock.patch.object(maintenance, "run_earnings_update", return_value={"success": False}),
    ):
        result = maintenance.run_maintenance("key", "db", tmp_path, skip_universe=True)
        assert not result["success"]
        assert q.call_args.kwargs["full_history"] is True
        maintenance.run_maintenance("key", "db", tmp_path, skip_universe=True)
        assert q.call_args.kwargs["full_history"] is False
    assert not maintenance._reconciliation_due(tmp_path / ".fundamentals_reconciled", date.today())
    assert maintenance._reconciliation_due(tmp_path / ".fundamentals_reconciled", date(2099, 1, 1))


def test_maintenance_dry_run_has_no_external_effects(tmp_path):
    with (
        mock.patch.object(maintenance, "refresh_universe") as universe,
        mock.patch.object(maintenance, "run_quarterly") as financials,
        mock.patch.object(maintenance, "run_earnings_update") as reports,
    ):
        result = maintenance.run_maintenance("key", "db", tmp_path, dry_run=True)
    assert result["success"] and result["dry_run"]
    assert result["planned"] == ["universe", "fundamentals", "earnings"]
    for callback in (universe, financials, reports):
        callback.assert_not_called()
    assert not list(tmp_path.iterdir())


def test_universe_onboards_before_replacing_memberships_and_retries_missing_prices():
    conn = connection()
    conn.execute.return_value.fetchall.return_value = [("AAPL", True, True), ("MSFT", True, False)]
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.get_ticker_details.side_effect = lambda symbol: {"ticker": symbol, "name": symbol}
    result = coldstart.IndexPopulationResult(requested=1)
    result["custom"] = 3
    events = []
    with (
        mock.patch.object(maintenance.psycopg, "connect", return_value=conn),
        mock.patch.object(
            maintenance,
            "_index_fetchers",
            return_value=[("custom", lambda log: ["AAPL", "MSFT", "NVDA"])],
        ),
        mock.patch.object(maintenance, "PolygonClient", return_value=client),
        mock.patch.object(maintenance, "SyncRateLimiter"),
        mock.patch.object(
            maintenance, "insert_company", side_effect=lambda *a: events.append("company") or True
        ),
        mock.patch.object(
            maintenance,
            "fetch_and_insert_prices",
            side_effect=lambda *a: events.append("prices") or 10,
        ),
        mock.patch.object(
            maintenance,
            "populate_index_constituents",
            side_effect=lambda *a, **k: events.append("indices") or result,
        ) as indices,
    ):
        outcome = maintenance.refresh_universe("key", "db", LOG)
    assert outcome["success"]
    assert outcome["onboarded"] == ["MSFT", "NVDA"]
    assert events == ["company", "prices", "company", "prices", "indices"]
    assert indices.call_args.kwargs["require_complete"] is True
    assert indices.call_args.kwargs["prefetched_symbols"] == {"custom": ["AAPL", "MSFT", "NVDA"]}


def test_missing_company_preserves_index_membership_before_delete():
    conn = connection()
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = (1, 2)
    cursor.fetchall.return_value = [("AAPL",)]
    result = coldstart.populate_index_constituents(
        conn, LOG, prefetched_symbols={"custom": ["AAPL", "MSFT"]}, require_complete=True
    )
    assert "custom" in result.failures
    assert not any("DELETE FROM" in str(call.args[0]) for call in cursor.execute.call_args_list)


def test_price_failure_preserves_affected_index_even_after_company_commit():
    conn = connection()
    conn.execute.return_value.fetchall.return_value = [("AAPL", True, True)]
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.get_ticker_details.return_value = {"ticker": "MSFT", "name": "Microsoft"}
    indices_result = coldstart.IndexPopulationResult(requested=1)
    indices_result["healthy"] = 1
    with (
        mock.patch.object(maintenance.psycopg, "connect", return_value=conn),
        mock.patch.object(
            maintenance,
            "_index_fetchers",
            return_value=[
                ("affected", lambda log: ["AAPL", "MSFT"]),
                ("healthy", lambda log: ["AAPL"]),
            ],
        ),
        mock.patch.object(maintenance, "PolygonClient", return_value=client),
        mock.patch.object(maintenance, "SyncRateLimiter"),
        mock.patch.object(maintenance, "insert_company", return_value=True) as company,
        mock.patch.object(
            maintenance, "fetch_and_insert_prices", side_effect=RuntimeError("offline")
        ),
        mock.patch.object(
            maintenance, "populate_index_constituents", return_value=indices_result
        ) as indices,
    ):
        result = maintenance.refresh_universe("key", "db", LOG)
    company.assert_called_once()
    assert not result["success"]
    assert "onboard:MSFT" in result["failures"]
    assert "affected" in result["failures"]
    assert indices.call_args.kwargs["prefetched_symbols"] == {"healthy": ["AAPL"]}


@pytest.mark.parametrize(
    "statuses,failed", [([403, 500], True), ([403, 200], False), ([200], False)]
)
def test_yahoo_http_failures_are_distinct_from_unsupported_empty(statuses, failed):
    import yfinance as yf
    from curl_cffi.requests import Session

    def ticker_factory(symbol, *, session):
        def fetch(*, limit):
            # Simulate yfinance's retry sequence, preserving both transport calls.
            for _ in statuses:
                session.get("https://finance.yahoo.com/calendar/earnings?symbol=AAPL")
            return None

        return SimpleNamespace(get_earnings_dates=fetch)

    with (
        mock.patch.object(
            Session,
            "request",
            side_effect=[SimpleNamespace(status_code=status) for status in statuses],
        ) as transport,
        mock.patch.object(yf, "Ticker", side_effect=ticker_factory),
    ):
        if failed:
            with pytest.raises(earnings.ProviderError, match="HTTP 500 after retries"):
                earnings.fetch_earnings("AAPL")
        else:
            assert earnings.fetch_earnings("AAPL") == []
        assert transport.call_count == len(statuses)


def test_yahoo_parser_preserves_missing_eps_and_new_york_report_date():
    import pandas as pd
    import yfinance as yf

    frame = pd.DataFrame(
        {"EPS Estimate": [float("nan")], "Reported EPS": [2.1], "Surprise(%)": [None]},
        index=[pd.Timestamp("2026-09-26T00:30:00Z")],
    )
    with mock.patch.object(yf, "Ticker") as provider:
        provider.return_value.get_earnings_dates.return_value = frame
        rows = earnings.fetch_earnings("BRK.B")
    assert provider.call_args.args[0] == "BRK-B"
    assert rows[0]["report_date"] == date(2026, 9, 25)
    assert rows[0]["timing"] == "AMC"
    assert rows[0]["eps_estimate"] is None
    assert rows[0]["eps_actual"] == 2.1


@pytest.mark.parametrize(
    "reported_at,expected_timing",
    [
        ("2026-09-25 09:29:59", "BMO"),
        ("2026-09-25 09:30:00", "DMH"),
        ("2026-09-25 11:00:00", "DMH"),
        ("2026-09-25 12:00:00", "DMH"),
        ("2026-09-25 15:59:59", "DMH"),
        ("2026-09-25 16:00:00", "AMC"),
        # The Friday after Thanksgiving has a scheduled 13:00 ET close.
        ("2026-11-27 12:59:59", "DMH"),
        ("2026-11-27 13:00:00", "AMC"),
        # Nontrading-day reports use conventional 09:30/16:00 boundaries.
        ("2026-09-27 09:29:59", "BMO"),
        ("2026-09-27 09:30:00", "DMH"),
        ("2026-09-27 13:00:00", "DMH"),
        ("2026-09-27 16:00:00", "AMC"),
    ],
)
def test_yahoo_earnings_timing_respects_session_boundaries(reported_at, expected_timing):
    import pandas as pd
    import yfinance as yf

    frame = pd.DataFrame(
        {"EPS Estimate": [1.0], "Reported EPS": [1.1], "Surprise(%)": [10.0]},
        index=[pd.Timestamp(reported_at, tz="America/New_York")],
    )
    with mock.patch.object(yf, "Ticker") as provider:
        provider.return_value.get_earnings_dates.return_value = frame
        rows = earnings.fetch_earnings("AAPL")
    assert rows[0]["timing"] == expected_timing


def test_earnings_failure_is_not_reported_as_empty_or_rolled_into_success():
    conn = connection()
    conn.execute.return_value.fetchall.return_value = [("AAPL",), ("MSFT",)]
    with (
        mock.patch.object(earnings.psycopg, "connect", return_value=conn),
        mock.patch.object(earnings, "fetch_earnings", side_effect=[[{}], RuntimeError("offline")]),
        mock.patch.object(earnings, "persist_earnings", return_value=1),
    ):
        result = earnings.run_earnings_update("db", request_interval=0)
    assert not result["success"]
    assert result["persisted"] == 1
    assert result["succeeded"] == 1
    assert result["empty"] == 0
    assert set(result["failures"]) == {"MSFT"}


def test_earnings_ticker_transaction_does_not_swallow_persistence_failure():
    conn = connection()
    conn.execute.side_effect = RuntimeError("write failed")
    row = {"ticker": "AAPL", "report_date": date(2026, 9, 1), "eps_actual": 2}
    with pytest.raises(RuntimeError, match="write failed"):
        earnings.persist_earnings(conn, [row])
    assert conn.transaction.return_value.__exit__.call_args.args[0] is RuntimeError
