"""Static checks for schema invariants that protect no-drop upgrades."""

from pathlib import Path

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "sqlschema"


def _read(name: str) -> str:
    return (SCHEMA_DIR / name).read_text()


def test_news_tickers_are_intentionally_independent_of_companies() -> None:
    sql = _read("10_news.sql")

    assert "news_article_tickers" in sql
    assert "news_sentiment" in sql
    assert "REFERENCES companies" not in sql


def test_stock_character_tables_cascade_with_companies_on_fresh_schema() -> None:
    sql = _read("27_stock_character.sql")

    assert sql.count(
        "ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE"
    ) == 4


def test_stock_character_existing_tables_get_future_safe_foreign_keys() -> None:
    sql = _read("33_schema_integrity_and_time_semantics.sql")

    for table in (
        "stock_character_classification",
        "stock_character_baseline",
        "stock_character_flags",
        "stock_character_scorecard",
    ):
        assert f"ALTER TABLE {table}" in sql
    assert sql.count("FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE") == 4
    assert sql.count("NOT VALID;") == 4


def test_intraday_schema_uses_timestamptz_and_market_timezone() -> None:
    intraday = _read("21_intraday_prices.sql")
    migration = _read("33_schema_integrity_and_time_semantics.sql")

    assert "timestamp TIMESTAMPTZ NOT NULL" in intraday
    assert "timestamp without time zone" in migration
    assert "timestamp AT TIME ZONE 'UTC'" in migration
    assert "America/New_York" in intraday
    assert "America/New_York" in migration


def test_initial_technical_indicator_metadata_is_replayable() -> None:
    sql = _read("11_technical_indicators.sql")

    assert "CREATE TABLE IF NOT EXISTS technical_indicators" in sql
    assert "CREATE TABLE IF NOT EXISTS technical_indicator_metadata" in sql
    assert "ON CONFLICT (indicator_name) DO UPDATE SET" in sql


def test_market_internals_uses_trailing_rank_not_global_percent_rank() -> None:
    for name in ("26_market_internals.sql", "29_consolidate_vix.sql"):
        sql = _read(name)

        assert "PERCENT_RANK" not in sql
        assert "LEFT JOIN LATERAL" in sql
        assert "LIMIT 252" in sql


def test_gics_sector_function_is_stable_not_immutable() -> None:
    sql = _read("13_gics_sector_function.sql")

    assert "STABLE" in sql
    assert "IMMUTABLE" not in sql
