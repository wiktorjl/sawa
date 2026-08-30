"""Static checks for schema invariants that protect no-drop upgrades."""

import re
from pathlib import Path

from sawa.database.schema import REQUIRED_SCHEMA_FILENAMES

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "sqlschema"


def _read(name: str) -> str:
    return (SCHEMA_DIR / name).read_text()


def test_required_schema_manifest_matches_shipped_numbered_files() -> None:
    shipped = {path.name for path in SCHEMA_DIR.glob("[0-9][0-9]_*.sql")}

    assert REQUIRED_SCHEMA_FILENAMES == shipped


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


def test_intraday_bar_size_upgrade_preserves_rows_and_replaces_identity() -> None:
    fresh = _read("21_intraday_prices.sql")
    migration = _read("45_intraday_bar_size_identity.sql")

    assert "PRIMARY KEY (ticker, timestamp, bar_size_minutes)" in fresh
    assert fresh.count("NUMERIC(20, 8) NOT NULL") == 4
    assert "UPDATE public.stock_prices_intraday" in migration
    assert "WHERE bar_size_minutes IS NULL" in migration
    assert "PRIMARY KEY (ticker, timestamp, bar_size_minutes)" in migration
    assert "CHECK (bar_size_minutes IN (1, 5, 15, 30, 60))" in migration
    assert "chosen_intraday_resolution" in migration
    assert "chosen.bar_size_minutes = spi.bar_size_minutes" in migration
    assert "AS covered_minutes" in migration
    assert "source_minute_mask" in migration
    assert "pg_catalog.replace" in migration
    assert "pg_catalog.bit_count" not in migration
    assert "stock_prices_intraday_lineage_consistent" in migration
    assert "pg_catalog.sum(spi.source_minute_count)" in migration
    assert "pg_catalog.generate_series" not in migration
    assert "coverage.covered_minutes DESC" in migration
    assert "coverage.first_bar ASC" in migration
    assert "coverage.covered_end DESC" in migration
    assert "coverage.bar_size_minutes ASC" in migration
    assert "TIME '09:30:00'" in migration
    assert "TIME '16:00:00'" in migration


def test_every_price_precision_migration_is_monotonic_at_final_scale() -> None:
    for name in ("24_widen_price_precision.sql", "42_widen_price_for_reverse_splits.sql"):
        sql = _read(name)
        assert re.search(r"TYPE\s+NUMERIC\(16\s*,\s*4\)", sql, re.I) is None
        assert sql.count("TYPE NUMERIC(20, 8)") == 8


def test_initial_technical_indicator_metadata_is_replayable() -> None:
    sql = _read("11_technical_indicators.sql")

    assert "CREATE TABLE IF NOT EXISTS technical_indicators" in sql
    assert "CREATE TABLE IF NOT EXISTS technical_indicator_metadata" in sql
    assert "ON CONFLICT (indicator_name) DO UPDATE SET" in sql


def test_nasdaq_seed_defers_to_legacy_and_migration_preserves_memberships() -> None:
    seed = _read("12_indices.sql")
    migration = _read("34_rename_nasdaq5000.sql")

    assert "WHERE NOT EXISTS" in seed
    assert "code = 'nasdaq5000'" in seed
    assert "INSERT INTO index_constituents" in migration
    assert "ON CONFLICT (index_id, ticker) DO NOTHING" in migration
    assert "nasdaq_legacy_" in migration


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


def test_dividend_identity_archives_conflicts_before_normalized_unique_index() -> None:
    sql = _read("46_dividend_identity.sql")
    archive_insert = sql.index("INSERT INTO public.dividend_identity_conflicts")
    canonical_delete = sql.index("DELETE FROM public.dividends")
    unique_index = sql.index("CREATE UNIQUE INDEX")

    assert archive_insert < canonical_delete < unique_index
    assert "RETURNING original_dividend_id" in sql
    assert "USING archived AS a" in sql
    assert "COALESCE(dividend_type, ''::character varying)" in sql
    assert "DROP CONSTRAINT" in sql
    assert "DROP INDEX IF EXISTS public.dividends_normalized_identity_uidx" in sql
    assert "CREATE UNIQUE INDEX dividends_normalized_identity_uidx" in sql


def test_replayed_schema_files_do_not_delete_rows_or_tables() -> None:
    """The documented --no-drop path must be data-preserving by construction."""
    destructive = re.compile(
        r"\b(?:DELETE\s+FROM|TRUNCATE|DROP\s+TABLE|DROP\s+COLUMN)\b",
        re.I,
    )

    for path in sorted(SCHEMA_DIR.glob("[0-9][0-9]_*.sql")):
        executable_lines = "\n".join(
            line for line in path.read_text().splitlines() if not line.lstrip().startswith("--")
        )
        destructive_matches = list(destructive.finditer(executable_lines))
        if path.name == "46_dividend_identity.sql":
            # Migration 46 is the sole narrow exception: every non-survivor is
            # first copied in full to a durable archive and DELETE is joined to
            # the INSERT ... RETURNING set in the same atomic statement.
            assert len(destructive_matches) == 1
            assert destructive_matches[0].group(0).upper().startswith("DELETE FROM")
            assert "INSERT INTO public.dividend_identity_conflicts" in executable_lines
            assert "USING archived AS a" in executable_lines
        else:
            assert destructive_matches == [], path.name
        assert re.search(r"\bDROP\b[^;\n]*\bCASCADE\b", executable_lines, re.I) is None, (
            path.name
        )
