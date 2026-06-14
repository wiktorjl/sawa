import logging
import os
from datetime import date
from pathlib import Path
from typing import Any
from unittest import mock

from sawa import weekly


class _FakeCursor:
    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, *args: object, **kwargs: object) -> None:
        return None

    def fetchone(self) -> Any:
        return (None,)

    def fetchall(self) -> list[Any]:
        return []


class _FakeConn:
    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def commit(self) -> None:
        return None


def _run_weekly_with_mocks(**overrides: Any) -> dict[str, Any]:
    """Run run_weekly with all external effects mocked (no DB / no network)."""
    defaults: dict[str, Any] = {
        "download_overviews": mock.DEFAULT,
        "download_economy": {"treasury-yields": 1},
        "load_companies": None,
        "load_economy": None,
        "load_news": 5,
        "run_corporate_actions_update": {"splits_loaded": 0, "split_tickers": []},
        "character": {"classified": 2, "total": 2, "errors": 0},
    }
    defaults.update(overrides)
    os.environ.pop("FRED_API_KEY", None)
    with mock.patch.object(weekly, "psycopg") as mpg, mock.patch.object(
        weekly, "PolygonClient"
    ), mock.patch.object(weekly, "SyncRateLimiter"), mock.patch.object(
        weekly, "get_symbols_from_db", return_value=["AAPL", "MSFT"]
    ), mock.patch.object(weekly, "get_last_date", return_value=date(2026, 1, 1)), mock.patch.object(
        weekly, "download_overviews", return_value=3
    ) as movr, mock.patch.object(
        weekly, "download_economy", return_value=defaults["download_economy"]
    ), mock.patch.object(weekly, "load_companies"), mock.patch.object(
        weekly, "load_economy"
    ), mock.patch.object(
        weekly, "load_news", return_value=defaults["load_news"]
    ), mock.patch.object(
        weekly,
        "run_corporate_actions_update",
        return_value=defaults["run_corporate_actions_update"],
    ), mock.patch(
        "sawa.split_adjust.refresh_split_adjusted_prices",
        return_value={"success": True, "prices_updated": 100},
    ) as madj, mock.patch(
        "sawa.ta_backfill.recompute_ta_for_tickers",
        return_value={"success": True, "deleted": 10, "indicators_calculated": 12},
    ) as mrec, mock.patch(
        "sawa.stock_character_batch.run_stock_character_batch",
        return_value=defaults["character"],
    ), mock.patch.object(weekly, "get_notifier"), mock.patch.object(
        weekly, "alert_missing_api_key"
    ):
        if overrides.get("download_overviews") is RuntimeError:
            movr.side_effect = RuntimeError("bad overviews.csv")
        mpg.connect.return_value = _FakeConn()
        stats = weekly.run_weekly(
            api_key="k",
            database_url="db",
            output_dir=Path("/tmp/sawa_test_weekly"),
            logger=logging.getLogger(__name__),
        )
        stats["_adj_mock"] = madj
        stats["_rec_mock"] = mrec
    return stats


def test_weekly_recomputes_ta_after_split_adjust() -> None:
    stats = _run_weekly_with_mocks(
        run_corporate_actions_update={"splits_loaded": 1, "split_tickers": ["KLAC"]},
    )
    assert stats["_adj_mock"].called
    assert stats["_rec_mock"].called
    assert stats["_rec_mock"].call_args.kwargs["tickers"] == ["KLAC"]
    assert stats["split_ta_recompute"]["indicators_calculated"] == 12
    assert stats["success"] is True


def test_weekly_early_step_failure_does_not_abort_later_steps() -> None:
    stats = _run_weekly_with_mocks(download_overviews=RuntimeError)
    # The overviews step failed but the run continued through the later steps.
    assert stats["success"] is False
    assert "overviews" in stats["step_errors"]
    assert stats["economy"] == {"treasury-yields": 1}
    assert stats["news"] == 5
    assert stats["character"] == {"classified": 2, "total": 2, "errors": 0}


def test_get_economy_start_dates_uses_each_table(monkeypatch) -> None:
    last_dates = {
        "treasury_yields": date(2026, 4, 16),
        "inflation": date(2025, 12, 1),
        "inflation_expectations": date(2026, 1, 1),
        "labor_market": None,
    }
    calls: list[str] = []

    def fake_get_last_date(conn: object, table_name: str) -> date | None:
        calls.append(table_name)
        return last_dates[table_name]

    monkeypatch.setattr(weekly, "get_last_date", fake_get_last_date)

    result = weekly.get_economy_start_dates(object(), date(2026, 4, 24))

    assert result == {
        "treasury-yields": "2026-04-16",
        "inflation": "2025-12-01",
        "inflation-expectations": "2026-01-01",
        "labor-market": "2025-04-24",
    }
    assert calls == list(weekly.ECONOMY_ENDPOINT_TABLES.values())


def test_download_economy_uses_endpoint_specific_start_dates(tmp_path) -> None:
    calls: list[tuple[str, str, str]] = []
    start_dates = {
        "treasury-yields": "2026-04-16",
        "inflation": "2025-12-01",
        "inflation-expectations": "2026-01-01",
        "labor-market": "2025-04-24",
    }

    class FakeClient:
        def get_economy_data(
            self,
            endpoint: str,
            start_date: str,
            end_date: str,
        ) -> list[dict[str, Any]]:
            calls.append((endpoint, start_date, end_date))
            return [{"date": start_date, "value": endpoint}]

    stats = weekly.download_economy(
        FakeClient(),  # type: ignore[arg-type]
        "2020-01-01",
        "2026-04-24",
        tmp_path,
        logging.getLogger(__name__),
        start_dates=start_dates,
    )

    assert calls == [
        ("treasury-yields", "2026-04-16", "2026-04-24"),
        ("inflation", "2025-12-01", "2026-04-24"),
        ("inflation-expectations", "2026-01-01", "2026-04-24"),
        ("labor-market", "2025-04-24", "2026-04-24"),
    ]
    assert stats == {
        "treasury-yields": 1,
        "inflation": 1,
        "inflation-expectations": 1,
        "labor-market": 1,
    }
    assert (tmp_path / "treasury_yields.csv").exists()
    assert (tmp_path / "inflation.csv").exists()
    assert (tmp_path / "inflation_expectations.csv").exists()
    assert (tmp_path / "labor_market.csv").exists()
