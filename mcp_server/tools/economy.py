"""Economy data MCP tools (treasury yields, inflation, labor market)."""

import logging
from datetime import date
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


# --- Async service-based implementations ---


async def get_economy_data_async(
    indicator_type: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Get economy data via service layer (async)."""
    from ..services import get_economy_service

    service = get_economy_service()

    if indicator_type == "treasury_yields":
        return await service.get_treasury_yields(start_date, end_date, limit)
    elif indicator_type == "inflation":
        return await service.get_inflation(start_date, end_date, limit)
    elif indicator_type == "labor_market":
        return await service.get_labor_market(start_date, end_date, limit)
    elif indicator_type == "inflation_expectations":
        # Fall back to SQL for this one (not in repository)
        return _get_inflation_expectations(start_date, end_date or date.today().isoformat(), limit)
    else:
        raise ValueError(f"Invalid indicator_type: {indicator_type}")


# --- Sync SQL-based implementations (original) ---


def get_economy_data(
    indicator_type: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get economic indicators for a date range.

    Args:
        indicator_type: One of "treasury_yields", "inflation",
                       "inflation_expectations", or "labor_market"
        start_date: Start date in YYYY-MM-DD format
        end_date: End date (defaults to today)
        limit: Maximum rows (default: 100, max: 1000)

    Returns:
        List of economic indicator records
    """
    limit = min(limit, 1000)

    if end_date is None:
        end_date = date.today().isoformat()

    valid_indicators = {
        "treasury_yields": _get_treasury_yields,
        "inflation": _get_inflation,
        "inflation_expectations": _get_inflation_expectations,
        "labor_market": _get_labor_market,
    }

    if indicator_type not in valid_indicators:
        raise ValueError(
            f"Invalid indicator_type: {indicator_type}. "
            f"Must be one of: {', '.join(valid_indicators.keys())}"
        )

    return valid_indicators[indicator_type](start_date, end_date, limit)


def _get_treasury_yields(
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Get treasury yields data."""
    sql = """
        SELECT
            date,
            yield_1_month,
            yield_3_month,
            yield_6_month,
            yield_1_year,
            yield_2_year,
            yield_5_year,
            yield_10_year,
            yield_30_year
        FROM treasury_yields
        WHERE date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def _get_inflation(
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Get inflation data."""
    sql = """
        SELECT
            date,
            cpi,
            cpi_core,
            cpi_year_over_year as inflation_yoy,
            pce,
            pce_core
        FROM inflation
        WHERE date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def _get_inflation_expectations(
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Get inflation expectations data."""
    sql = """
        SELECT
            date,
            market_5_year,
            market_10_year,
            forward_years_5_to_10
        FROM inflation_expectations
        WHERE date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def _get_labor_market(
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Get labor market data."""
    sql = """
        SELECT
            date,
            unemployment_rate,
            labor_force_participation_rate,
            avg_hourly_earnings,
            job_openings
        FROM labor_market
        WHERE date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def get_economy_dashboard(limit: int = 10) -> list[dict[str, Any]]:
    """
    Get a summary view of the latest economic indicators.

    Args:
        limit: Number of recent data points (default: 10, max: 100)

    Returns:
        List of combined economy dashboard records
    """
    limit = min(limit, 100)

    sql = """
        SELECT
            date,
            yield_1_month,
            yield_3_month,
            yield_10_year,
            yield_30_year,
            cpi,
            cpi_year_over_year as inflation_yoy,
            market_5_year as inflation_expectation_5y,
            market_10_year as inflation_expectation_10y,
            unemployment_rate,
            job_openings
        FROM v_economy_dashboard
        LIMIT %(limit)s
    """

    return execute_query(sql, {"limit": limit})
