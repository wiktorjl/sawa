import logging
from datetime import date
from typing import Any

from sawa import weekly


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
