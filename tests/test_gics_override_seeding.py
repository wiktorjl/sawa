"""Static regressions for foreign-key-safe legacy GICS override seeding.

These tests intentionally never open a database connection. Migration 37's
set-based function and company trigger are the single source of truth for both
fresh installs and upgrades.
"""

import re
from pathlib import Path

SCHEMA_FILE = Path(__file__).resolve().parents[1] / "sqlschema" / "37_gics_overrides.sql"
LEGACY_TICKERS = {"ASML", "ARM", "PDD", "TRI", "FER", "CCEP"}


def _read_sql() -> str:
    return SCHEMA_FILE.read_text()


def _seed_rows(sql: str) -> set[tuple[str, ...]]:
    """Extract the six-column VALUES rows from the seed function."""
    values = sql.split("        VALUES", 1)[1].split("    )\n    INSERT", 1)[0]
    fields = r"\s*,\s*".join([r"'([^']+)'" for _ in range(6)])
    return set(re.findall(rf"\(\s*{fields}\s*\)", values))


def test_schema_only_seed_is_foreign_key_safe_for_empty_companies() -> None:
    sql = _read_sql()

    assert "REFERENCES companies(ticker) ON DELETE CASCADE" in sql
    assert "INNER JOIN public.companies AS company ON company.ticker = seed.ticker" in sql
    assert "INSERT INTO companies" not in sql


def test_seed_function_has_exactly_six_legacy_defaults() -> None:
    rows = _seed_rows(_read_sql())

    assert {row[0] for row in rows} == LEGACY_TICKERS
    assert len(rows) == 6


def test_partial_company_load_seeds_only_requested_matching_ticker() -> None:
    sql = _read_sql()

    assert "requested_ticker VARCHAR(10) DEFAULT NULL" in sql
    assert "WHERE requested_ticker IS NULL OR seed.ticker = requested_ticker" in sql
    assert "PERFORM public.seed_legacy_gics_overrides(NEW.ticker);" in sql
    assert "WHEN (NEW.ticker IN ('ASML', 'ARM', 'PDD', 'TRI', 'FER', 'CCEP'))" in sql


def test_existing_override_is_preserved_and_repeated_seeds_are_idempotent() -> None:
    sql = _read_sql()

    assert "ON CONFLICT (ticker) DO NOTHING" in sql
    assert "DO UPDATE" not in sql
    assert "CREATE OR REPLACE FUNCTION public.seed_legacy_gics_overrides" in sql
    assert "CREATE OR REPLACE TRIGGER" not in sql
    assert "FROM pg_catalog.pg_trigger AS t" in sql
    assert "t.tgrelid = 'public.companies'::pg_catalog.regclass" in sql
    assert "CREATE TRIGGER seed_legacy_gics_override_after_company_change" in sql


def test_upgrade_backfills_existing_matching_companies() -> None:
    sql = _read_sql()

    assert sql.rstrip().endswith("SELECT public.seed_legacy_gics_overrides();")


def test_trigger_functions_use_invoker_rights_and_safe_search_path() -> None:
    sql = _read_sql()

    assert sql.count("SECURITY INVOKER") == 2
    assert sql.count("SET search_path = pg_catalog, public") == 2
    assert "SECURITY DEFINER" not in sql
    assert "AFTER INSERT OR UPDATE ON public.companies" in sql
