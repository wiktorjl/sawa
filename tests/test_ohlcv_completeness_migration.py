"""Safety contract for the forward-only OHLCV completeness migration."""

import re
from pathlib import Path

SCHEMA_DIR = Path(__file__).parents[1] / "sqlschema"
OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")


def _without_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def test_fresh_schema_requires_complete_daily_and_intraday_bars() -> None:
    for filename in ("02_market_data.sql", "21_intraday_prices.sql"):
        sql = (SCHEMA_DIR / filename).read_text(encoding="utf-8")
        for column in OHLCV_COLUMNS:
            assert re.search(rf"(?m)^\s*{column}\s+.*\bNOT NULL,\s*$", sql)


def test_upgrade_guard_is_non_destructive_and_forward_enforcing() -> None:
    sql = (SCHEMA_DIR / "44_ohlcv_completeness.sql").read_text(encoding="utf-8")
    executable = _without_comments(sql).upper()

    assert "DELETE FROM" not in executable
    assert "UPDATE STOCK_PRICES" not in executable
    assert "DROP NOT NULL" not in executable
    assert sql.count("NOT VALID") >= 2
    assert "stock_prices_ohlcv_complete" in sql
    assert "stock_prices_intraday_ohlcv_complete" in sql
    assert "ALTER TABLE STOCK_PRICES" not in executable
    assert "FROM STOCK_PRICES" not in executable
    assert "ALTER TABLE public.stock_prices" in sql
    assert "ALTER TABLE public.stock_prices_intraday" in sql
    assert "FROM public.stock_prices" in sql
    assert "FROM public.stock_prices_intraday" in sql
    assert sql.count("FROM pg_catalog.pg_constraint") == 2
    assert sql.count("pg_catalog.to_regclass") == 2
    assert "FROM pg_constraint" not in executable
    assert " TO_REGCLASS(" not in executable
    for column in OHLCV_COLUMNS:
        assert f"ALTER COLUMN {column} SET NOT NULL" in sql


def test_existing_ohlcv_sanity_migration_no_longer_deletes_legacy_rows() -> None:
    sql = (SCHEMA_DIR / "41_dashboard_spine_and_integrity.sql").read_text(
        encoding="utf-8"
    )
    executable = _without_comments(sql).upper()

    assert "DELETE FROM STOCK_PRICES" not in executable
    assert "STOCK_PRICES_OHLCV_SANE" in executable
    assert "NOT VALID" in executable
    assert "ROWS PRESERVED" in sql.upper()
    assert "FROM pg_catalog.pg_constraint" in sql

    intraday_sql = (
        SCHEMA_DIR / "43_intraday_ohlc_sanity_check.sql"
    ).read_text(encoding="utf-8")
    assert "FROM pg_catalog.pg_constraint" in intraday_sql


def test_upgrade_guard_checks_for_legacy_nulls_before_not_null_promotion() -> None:
    sql = (SCHEMA_DIR / "44_ohlcv_completeness.sql").read_text(encoding="utf-8")

    for table in ("stock_prices", "stock_prices_intraday"):
        null_probe = rf"SELECT 1\s+FROM public\.{table}\s+WHERE open IS NULL"
        assert re.search(null_probe, sql)
    assert "rows preserved" in sql
