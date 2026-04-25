"""Tests for database economy repository row mapping."""

from datetime import date
from decimal import Decimal

from sawa.repositories.database import DatabaseEconomyRepository


def test_treasury_yield_mapper_uses_schema_column_names() -> None:
    """Treasury yield rows should map the wide database column names."""
    repo = DatabaseEconomyRepository("postgresql://example")

    yields = repo._row_to_yield(
        {
            "date": date(2026, 4, 24),
            "yield_1_month": "4.91",
            "yield_3_month": "4.85",
            "yield_6_month": "4.77",
            "yield_1_year": "4.42",
            "yield_2_year": "4.05",
            "yield_5_year": "4.10",
            "yield_10_year": "4.28",
            "yield_30_year": "4.73",
        }
    )

    assert yields.yield_1mo == Decimal("4.91")
    assert yields.yield_10yr == Decimal("4.28")


def test_inflation_mapper_flattens_wide_rows() -> None:
    """Inflation wide rows should be returned as narrow indicator entries."""
    repo = DatabaseEconomyRepository("postgresql://example")

    rows = repo._row_to_inflation(
        {
            "date": date(2026, 3, 1),
            "cpi": "320.120",
            "cpi_core": "325.500",
            "cpi_year_over_year": "0.031",
            "pce": None,
            "pce_core": "128.900",
            "pce_spending": None,
        }
    )

    assert [row.indicator for row in rows] == [
        "cpi",
        "cpi_core",
        "cpi_year_over_year",
        "pce_core",
    ]
    assert rows[0].value == Decimal("320.120")
    assert rows[0].change_yoy == Decimal("0.031")


def test_inflation_mapper_filters_aliases() -> None:
    """Inflation aliases should map to the underlying wide-table indicator."""
    repo = DatabaseEconomyRepository("postgresql://example")

    rows = repo._row_to_inflation(
        {
            "date": date(2026, 3, 1),
            "cpi": "320.120",
            "cpi_year_over_year": "0.031",
        },
        indicator="inflation-yoy",
    )

    assert len(rows) == 1
    assert rows[0].indicator == "cpi_year_over_year"
    assert rows[0].value == Decimal("0.031")


def test_labor_market_mapper_flattens_and_filters_wide_rows() -> None:
    """Labor market wide rows should support narrow indicator filtering."""
    repo = DatabaseEconomyRepository("postgresql://example")

    rows = repo._row_to_labor_market(
        {
            "date": date(2026, 3, 1),
            "unemployment_rate": "0.039",
            "labor_force_participation_rate": "0.626",
            "avg_hourly_earnings": "35.42",
            "job_openings": None,
        },
        indicator="avg-hourly-earnings",
    )

    assert len(rows) == 1
    assert rows[0].indicator == "avg_hourly_earnings"
    assert rows[0].value == Decimal("35.42")
