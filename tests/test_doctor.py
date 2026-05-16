from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sawa.doctor import format_checks, run_doctor_on_connection, summarize_checks


class FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.query = ""
        self.params: tuple[Any, ...] = ()

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        self.query = query
        self.params = params

    def fetchone(self) -> tuple[Any, ...]:
        return self.conn.fetchone(self.query, self.params)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


class FakeConnection:
    def __init__(
        self,
        *,
        tables: set[str],
        active_count: int = 100,
        latest_price_date: date | None = date(2026, 5, 14),
        price_tickers: int = 100,
        price_rows: int = 1000,
        latest_price_tickers: int = 100,
        latest_price_rows: int = 100,
        recent_baseline_tickers: int = 100,
        bad_latest_rows: int = 0,
        latest_news: datetime | None = None,
    ) -> None:
        self.queries: list[str] = []
        self.tables = tables
        self.active_count = active_count
        self.latest_price_date = latest_price_date
        self.price_tickers = price_tickers
        self.price_rows = price_rows
        self.latest_price_tickers = latest_price_tickers
        self.latest_price_rows = latest_price_rows
        self.recent_baseline_tickers = recent_baseline_tickers
        self.bad_latest_rows = bad_latest_rows
        self.latest_news = latest_news or datetime(2026, 5, 14, tzinfo=timezone.utc)

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def fetchone(self, query: str, params: tuple[Any, ...]) -> tuple[Any, ...]:
        compact = " ".join(query.split())
        self.queries.append(compact)

        if "to_regclass" in compact:
            table = str(params[0]).removeprefix("public.")
            return (table if table in self.tables else None,)

        if "COUNT(*) FROM companies WHERE active = true" in compact:
            return (self.active_count,)

        if "WITH recent_dates AS" in compact:
            return (self.recent_baseline_tickers,)

        if (
            "SELECT COUNT(DISTINCT sp.ticker), COUNT(*) FROM stock_prices sp" in compact
            and "JOIN companies" in compact
            and "sp.date = (SELECT MAX(date)" in compact
        ):
            return (self.latest_price_tickers, self.latest_price_rows)

        if "FROM stock_prices" in compact and "COUNT(DISTINCT" in compact:
            if "JOIN companies" in compact and "sp.date = (SELECT MAX(date)" in compact:
                return (self.latest_price_tickers, self.latest_price_rows)
            return (self.latest_price_date, self.price_tickers, self.price_rows)

        if "malformed OHLCV" in compact:
            return (self.bad_latest_rows,)

        if "open IS NULL" in compact:
            return (self.bad_latest_rows,)

        if "FROM technical_indicators" in compact:
            return (self.latest_price_date, self.price_tickers)

        if "FROM market_internals" in compact:
            return (self.latest_price_date,)

        if "FROM news_articles" in compact:
            return (self.latest_news, 25)

        if "FROM mv_52week_extremes" in compact:
            return (self.latest_price_date, self.latest_price_date)

        raise AssertionError(f"Unexpected query: {compact}")


def test_doctor_daily_passes_when_latest_price_coverage_is_good() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "stock_prices_live",
            "mv_52week_extremes",
        }
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    summary = summarize_checks(checks)

    assert summary["success"] is True
    assert summary["failed"] == 0
    assert any(c.name == "stock_prices.latest_coverage" for c in checks)
    assert any(c.name == "mv_52week_extremes.freshness" for c in checks)


def test_doctor_stops_at_missing_required_schema() -> None:
    conn = FakeConnection(tables={"companies", "stock_prices"})

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    summary = summarize_checks(checks)

    assert summary["success"] is False
    assert summary["failed"] == 5
    assert [c.name for c in checks] == [
        "schema.companies",
        "schema.stock_prices",
        "schema.technical_indicators",
        "schema.news_articles",
        "schema.market_internals",
        "schema.stock_prices_live",
        "schema.mv_52week_extremes",
    ]


def test_doctor_fails_when_latest_price_coverage_is_too_low() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        latest_price_tickers=80,
        latest_price_rows=80,
        recent_baseline_tickers=100,
    )

    checks = run_doctor_on_connection(
        conn,
        job="daily",
        today=date(2026, 5, 15),
        min_coverage=0.95,
    )
    failed = {c.name: c for c in checks if c.status == "FAIL"}

    assert "stock_prices.latest_coverage" in failed
    assert failed["stock_prices.latest_coverage"].observed == 80
    assert "expected at least 95" in failed["stock_prices.latest_coverage"].message
    assert "stock_prices.latest_rows" not in failed
    assert summarize_checks(checks)["success"] is False


def test_latest_rows_does_not_duplicate_latest_coverage_failure() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        active_count=5889,
        recent_baseline_tickers=5889,
        latest_price_tickers=5004,
        latest_price_rows=5004,
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["stock_prices.latest_rows"].status == "PASS"
    assert by_name["stock_prices.latest_rows"].observed == 5004
    assert by_name["stock_prices.latest_coverage"].status == "FAIL"
    assert "expected at least 5006" in by_name["stock_prices.latest_coverage"].message


def test_doctor_uses_recent_daily_baseline_not_broad_reference_index() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "stock_prices_live",
            "mv_52week_extremes",
            "indices",
            "index_constituents",
        },
        active_count=10401,
        price_tickers=10392,
        price_rows=10000,
        recent_baseline_tickers=5004,
        latest_price_tickers=5004,
        latest_price_rows=5004,
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["stock_prices.expected_universe"].observed == 5004
    assert "recent daily stock_prices baseline" in by_name["stock_prices.expected_universe"].message
    assert by_name["stock_prices.latest_coverage"].status == "PASS"
    assert not any("index_constituents" in query for query in conn.queries)
    assert summarize_checks(checks)["success"] is True


def test_same_day_news_is_not_marked_stale() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        latest_news=datetime(2026, 5, 15, 21, 0, tzinfo=timezone.utc),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "PASS"


def test_format_checks_includes_summary_counts() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        latest_price_date=date(2026, 4, 1),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    output = format_checks(checks)

    assert "Database Doctor" in output
    assert "Summary:" in output
    assert "stock_prices.latest_date" in output
