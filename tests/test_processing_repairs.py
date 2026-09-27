"""Regression coverage for the production failures found in September 2026."""

import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import mock

import httpx

from sawa import cli, daily, stock_character_batch
from sawa.api.cboe import CboeClient
from sawa.database.load import PersistenceResult
from sawa.utils.logging import prune_old_run_logs
from tests.test_daily import FakeConnection
from tests.test_doctor import _WEEKLY_TABLES
from tests.test_doctor import FakeConnection as DoctorConnection
from tests.test_stock_character_atomicity import _analysis, _Connection, _prepare_worker


def test_cboe_recovers_missing_history_on_current_host() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.host == "cdn-api.cboe.com"
        if request.url.path.endswith(".csv"):
            return httpx.Response(200, text=(
                "DATE,OPEN,HIGH,LOW,CLOSE\n"
                "09/22/2026,1,1,1,14.21\n"
                "09/23/2026,1,1,1,15.18\n"
                "09/24/2026,1,1,1,15.67\n"
                "09/25/2026,1,1,1,14.87\n"
            ))
        symbol = Path(request.url.path).stem
        return httpx.Response(200, json={
            "symbol": symbol,
            "data": {"close": 14.87, "last_trade_time": "2026-09-25T16:15:01"},
        })

    with CboeClient() as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(respond))
        result = client.get_market_internals("2026-09-23", "2026-09-25")

    assert [row["date"] for row in result] == ["2026-09-23", "2026-09-24", "2026-09-25"]
    assert result[0]["vix"] == 15.18
    assert result[1]["vix3m"] == 15.67
    assert not result.failures
    assert len(requests) == 4


def test_cboe_history_failure_is_visible_even_when_latest_quotes_work() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(".csv"):
            return httpx.Response(200, text="DATE,CLOSE\n09/23/2026,NaN\n")
        return httpx.Response(200, json={
            "symbol": Path(request.url.path).stem,
            "data": {"close": 15, "last_trade_time": "2026-09-25T16:15:01"},
        })

    with CboeClient() as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(respond))
        result = client.get_market_internals("2026-09-23", "2026-09-25")
    assert len(result) == 1
    assert len(result.failures) == 2
    assert not result.all_quotes_failed


def test_same_date_price_revision_refreshes_materialized_extremes() -> None:
    conn = FakeConnection([
        ("mv_52week_extremes",),
        (date(2026, 9, 25), date(2026, 9, 25)),
    ])
    assert daily.refresh_52week_extremes_if_needed(
        conn, logging.getLogger(__name__), prices_changed=True
    )
    assert conn.cursor_obj.statements[-1] == "REFRESH MATERIALIZED VIEW mv_52week_extremes"
    assert conn.commits == 1


def test_character_does_not_stamp_stale_inputs_as_new_classification(monkeypatch) -> None:
    conn = _Connection()
    _prepare_worker(monkeypatch, conn, _analysis())
    monkeypatch.setattr(stock_character_batch, "_run_date", date(2026, 9, 25))
    analyze = mock.Mock()
    monkeypatch.setattr(stock_character_batch, "analyze_stock", analyze)
    result = stock_character_batch._process_ticker("AAPL")
    assert result["stale_source"] is True
    assert result["classified"] is False
    assert conn.commits == 0
    analyze.assert_not_called()


def test_character_health_counts_only_latest_eligible_active_rows() -> None:
    from sawa.doctor import run_doctor_on_connection

    conn = DoctorConnection(tables=_WEEKLY_TABLES, character_tickers=5, active_count=100)
    checks = run_doctor_on_connection(conn, job="weekly", today=date(2026, 5, 15))
    coverage = next(c for c in checks if c.name == "stock_character_classification.coverage")
    assert coverage.status == "WARN"
    assert coverage.observed == 5
    query = next(q for q in conn.queries if "eligible_character_prices" in q)
    assert "sc.run_date = %s" in query
    assert "c.active = true" in query
    assert "HAVING MAX(sp.date) = %s" in query


def test_post_daily_doctor_cannot_accept_five_day_stale_volatility() -> None:
    from sawa.doctor import run_doctor_on_connection

    conn = DoctorConnection(
        tables={"companies", "stock_prices", "technical_indicators", "news_articles",
                "market_internals", "treasury_yields", "stock_prices_live", "mv_52week_extremes"},
        latest_price_date=date(2026, 9, 25),
        market_latest=(date(2026, 9, 22), date(2026, 9, 22), date(2026, 9, 24)),
    )
    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 9, 26))
    assert {c.name for c in checks if c.status == "FAIL"} >= {
        "market_internals.vix.latest_date", "market_internals.vix3m.latest_date"
    }


def test_treasury_daily_only_loads_fresh_treasury_artifact(monkeypatch) -> None:
    conn = mock.MagicMock()
    client = mock.MagicMock()
    client.__enter__.return_value.get_economy_data.return_value = [
        {"date": "2026-09-25", "yield_10_year": 4.0}
    ]
    monkeypatch.setattr(daily.psycopg, "connect", mock.Mock(return_value=conn))
    monkeypatch.setattr(daily, "PolygonClient", mock.Mock(return_value=client))
    monkeypatch.setattr(daily, "get_last_date", lambda *_a: date(2026, 9, 1))
    monkeypatch.setattr(daily, "get_market_date", lambda: date(2026, 9, 27))
    persisted = PersistenceResult(
        1, table="treasury_yields", artifact_found=True, source_rows=1, eligible_rows=1
    )
    with mock.patch(
        "sawa.database.load.load_economy", return_value={"treasury_yields": persisted}
    ) as load:
        assert daily.refresh_treasury_yields("test", "test", logging.getLogger(__name__)) == 1
    assert load.call_args.kwargs["only_tables"] == {"treasury_yields"}
    client.__enter__.return_value.get_economy_data.assert_called_once_with(
        "treasury-yields", "2026-08-28", "2026-09-27"
    )


def test_log_retention_preserves_audits_symlinks_and_recently_written_runs(tmp_path) -> None:
    now = datetime(2026, 9, 27)
    old = now - timedelta(days=100)
    old_run = tmp_path / f"daily_{old:%Y%m%d_%H%M%S}.log"
    old_run.write_text("old")
    os.utime(old_run, (old.timestamp(), old.timestamp()))
    active_run = tmp_path / f"intraday_{old:%Y%m%d_%H%M%S}.log"
    active_run.write_text("still active")
    audit = tmp_path / "execute_query.jsonl"
    audit.write_text("audit")
    symlink = tmp_path / f"weekly_{old:%Y%m%d_%H%M%S}.log"
    symlink.symlink_to(audit)
    assert prune_old_run_logs(tmp_path, 90, now=now) == 1
    assert not old_run.exists()
    assert active_run.exists() and audit.exists() and symlink.is_symlink()


def test_maintenance_cli_passes_recovery_options_and_reports_failure(monkeypatch) -> None:
    monkeypatch.setattr(cli, "setup_logging", lambda *_a, **_k: logging.getLogger(__name__))
    monkeypatch.setattr("sys.argv", [
        "sawa", "maintenance", "--api-key", "test", "--database-url", "test",
        "--full-history", "--dry-run", "--skip-earnings",
    ])
    with mock.patch("sawa.maintenance.run_maintenance", return_value={"success": False}) as run:
        assert cli.main() == 1
    assert run.call_args.kwargs["full_history"] is True
    assert run.call_args.kwargs["dry_run"] is True
    assert run.call_args.kwargs["skip_earnings"] is True
