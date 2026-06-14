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
        null_sic: int = 0,
        null_mcap: int = 0,
        economy_latest: date | None = None,
        character_run: date | None = None,
        character_tickers: int = 100,
        corporate_action_rows: int = 5,
        quarterly_latest: date | None = None,
        quarterly_rows: int = 1000,
        post_split_checked: int = 0,
        post_split_flagged: int = 0,
        post_split_worst: str = "",
    ) -> None:
        self.queries: list[str] = []
        self.tables = tables
        self.active_count = active_count
        self.null_sic = null_sic
        self.null_mcap = null_mcap
        self.latest_price_date = latest_price_date
        self.price_tickers = price_tickers
        self.price_rows = price_rows
        self.latest_price_tickers = latest_price_tickers
        self.latest_price_rows = latest_price_rows
        self.recent_baseline_tickers = recent_baseline_tickers
        self.bad_latest_rows = bad_latest_rows
        self.latest_news = latest_news or datetime(2026, 5, 14, tzinfo=timezone.utc)
        self.economy_latest = economy_latest or date(2026, 5, 14)
        self.character_run = character_run or date(2026, 5, 14)
        self.character_tickers = character_tickers
        self.corporate_action_rows = corporate_action_rows
        self.quarterly_latest = quarterly_latest or date(2026, 5, 14)
        self.quarterly_rows = quarterly_rows
        self.post_split_checked = post_split_checked
        self.post_split_flagged = post_split_flagged
        self.post_split_worst = post_split_worst

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def fetchone(self, query: str, params: tuple[Any, ...]) -> tuple[Any, ...]:
        compact = " ".join(query.split())
        self.queries.append(compact)

        if "to_regclass" in compact:
            table = str(params[0]).removeprefix("public.")
            return (table if table in self.tables else None,)

        if "FILTER (WHERE sic_code IS NULL)" in compact:
            return (self.active_count, self.null_sic, self.null_mcap)

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

        # Post-split TA recompute check: returns (checked, flagged, worst).
        # Matched before the generic technical_indicators branch because its
        # CTE also references FROM technical_indicators.
        if "recent_splits" in compact and "stored_sma_50" in compact:
            return (
                self.post_split_checked,
                self.post_split_flagged,
                self.post_split_worst,
            )

        if "FROM technical_indicators" in compact:
            return (self.latest_price_date, self.price_tickers)

        if "FROM market_internals" in compact:
            return (self.latest_price_date,)

        if "FROM news_articles" in compact:
            return (self.latest_news, 25)

        if "FROM mv_52week_extremes" in compact:
            return (self.latest_price_date, self.latest_price_date)

        if "FROM stock_character_classification" in compact:
            return (self.character_run, self.character_tickers)

        if compact.startswith("SELECT MAX(date) FROM") and any(
            t in compact
            for t in (
                "treasury_yields",
                "inflation",
                "inflation_expectations",
                "labor_market",
            )
        ):
            return (self.economy_latest,)

        if compact.startswith("SELECT COUNT(*) FROM") and (
            "stock_splits" in compact or "dividends" in compact
        ):
            return (self.corporate_action_rows,)

        if "SELECT MAX(period_end), COUNT(*)" in compact or (
            "SELECT MAX(date), COUNT(*)" in compact
            and "financial_ratios" in compact
        ):
            return (self.quarterly_latest, self.quarterly_rows)

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


_DAILY_TABLES = {
    "companies",
    "stock_prices",
    "technical_indicators",
    "news_articles",
    "market_internals",
    "stock_prices_live",
    "mv_52week_extremes",
}

_WEEKLY_TABLES = {
    "companies",
    "stock_prices",
    "treasury_yields",
    "inflation",
    "inflation_expectations",
    "labor_market",
    "stock_splits",
    "dividends",
    "stock_character_classification",
    "stock_character_scorecard",
}

_QUARTERLY_TABLES = {
    "companies",
    "stock_prices",
    "financial_ratios",
    "balance_sheets",
    "income_statements",
    "cash_flows",
}


def test_doctor_weekly_passes_when_cadence_is_fresh() -> None:
    conn = FakeConnection(
        tables=_WEEKLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        economy_latest=date(2026, 5, 13),
        character_run=date(2026, 5, 12),
        character_tickers=100,
    )

    checks = run_doctor_on_connection(conn, job="weekly", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert summarize_checks(checks)["success"] is True
    assert by_name["treasury_yields.latest_date"].status == "PASS"
    assert by_name["stock_character_classification.latest_run"].status == "PASS"


def test_doctor_weekly_fails_on_stale_treasury_and_character() -> None:
    # Fresh stock_prices (daily feed) but a stale weekly cadence must still flip
    # the exit code: treasury_yields and the character run are FAIL-capable.
    conn = FakeConnection(
        tables=_WEEKLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        economy_latest=date(2026, 1, 1),
        character_run=date(2026, 1, 1),
    )

    checks = run_doctor_on_connection(conn, job="weekly", today=date(2026, 5, 15))
    failed = {c.name for c in checks if c.status == "FAIL"}

    assert summarize_checks(checks)["success"] is False
    assert "treasury_yields.latest_date" in failed
    assert "stock_character_classification.latest_run" in failed
    # Slow series and coverage stay WARN, not FAIL.
    assert "inflation.latest_date" not in failed
    assert "labor_market.latest_date" not in failed
    assert "stock_character_classification.coverage" not in failed


def test_doctor_quarterly_fails_on_stale_fundamentals() -> None:
    conn = FakeConnection(
        tables=_QUARTERLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        quarterly_latest=date(2025, 1, 1),
        quarterly_rows=1000,
    )

    checks = run_doctor_on_connection(conn, job="quarterly", today=date(2026, 5, 15))
    failed = {c.name for c in checks if c.status == "FAIL"}

    assert summarize_checks(checks)["success"] is False
    assert "financial_ratios.latest_date" in failed
    assert "balance_sheets.latest_date" in failed
    assert "income_statements.latest_date" in failed
    assert "cash_flows.latest_date" in failed


def test_doctor_quarterly_passes_when_fundamentals_fresh() -> None:
    conn = FakeConnection(
        tables=_QUARTERLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        quarterly_latest=date(2026, 4, 5),
        quarterly_rows=1000,
    )

    checks = run_doctor_on_connection(conn, job="quarterly", today=date(2026, 5, 15))

    assert summarize_checks(checks)["success"] is True


def test_post_split_ta_check_flags_uncomputed_tickers() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES | {"stock_splits"},
        post_split_checked=150,
        post_split_flagged=3,
        post_split_worst="KLAC, XXII, AERT",
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    check = by_name["technical_indicators.post_split_recompute"]
    assert check.status == "WARN"
    assert check.observed == 3
    assert "KLAC" in check.message
    # Detection-gap signal stays a WARN so the live DB (which has split
    # tickers awaiting recompute) does not flip the exit code prematurely.
    assert summarize_checks(checks)["success"] is True


def test_post_split_ta_check_passes_when_recomputed() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES | {"stock_splits"},
        post_split_checked=150,
        post_split_flagged=0,
        post_split_worst="",
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    check = by_name["technical_indicators.post_split_recompute"]
    assert check.status == "PASS"
    assert check.observed == 0


def test_post_split_ta_check_skipped_without_stock_splits_table() -> None:
    conn = FakeConnection(tables=_DAILY_TABLES)

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))

    assert not any(
        c.name == "technical_indicators.post_split_recompute" for c in checks
    )


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
