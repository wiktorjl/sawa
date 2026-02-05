"""
Earnings: Download and store earnings data from Yahoo Finance.

Purpose: Update earnings table with EPS estimates, actuals, and surprise data.
Re-entrant: Safe to run multiple times (upsert on unique constraints).
"""

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import yfinance as yf

from sawa.utils import setup_logging

# Default to 5 quarters of history (matches yfinance revenue data availability)
DEFAULT_QUARTERS = 5


def get_active_tickers(conn) -> list[str]:
    """Get list of active tickers from companies table."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies WHERE active = true ORDER BY ticker")
        return [row[0] for row in cur.fetchall()]


def fetch_earnings_yfinance(
    ticker: str,
    quarters: int = DEFAULT_QUARTERS,
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch earnings data for a ticker from Yahoo Finance.

    Args:
        ticker: Stock ticker symbol
        quarters: Number of quarters of history to fetch (default: 5)
        logger: Logger instance

    Returns:
        List of earnings records with EPS and revenue data
    """
    import math

    logger = logger or logging.getLogger(__name__)
    results: list[dict[str, Any]] = []

    def safe_decimal(val: Any) -> Decimal | None:
        """Convert value to Decimal, returning None for NaN/None."""
        if val is None:
            return None
        try:
            if math.isnan(val):
                return None
            return Decimal(str(val))
        except (ValueError, TypeError):
            return None

    def safe_int(val: Any) -> int | None:
        """Convert value to int, returning None for NaN/None."""
        if val is None:
            return None
        try:
            if math.isnan(val):
                return None
            return int(val)
        except (ValueError, TypeError):
            return None

    try:
        stock = yf.Ticker(ticker)
        earnings_dates = stock.earnings_dates

        if earnings_dates is None or len(earnings_dates) == 0:
            logger.debug(f"{ticker}: No earnings data available")
            return results

        # Get quarterly income statement for actual revenue
        quarterly_revenue: dict[str, int] = {}
        try:
            income_stmt = stock.quarterly_income_stmt
            if income_stmt is not None and len(income_stmt) > 0:
                # Find Total Revenue row
                for idx in income_stmt.index:
                    if "total revenue" in str(idx).lower():
                        for col in income_stmt.columns:
                            period_end = col.date() if hasattr(col, "date") else col
                            revenue = income_stmt.loc[idx, col]
                            if revenue is not None and not math.isnan(revenue):
                                # Key by fiscal period end date
                                quarterly_revenue[str(period_end)] = int(revenue)
                        break
        except Exception:
            pass  # Revenue data is optional

        # Get revenue estimate for upcoming earnings from calendar
        upcoming_revenue_estimate: int | None = None
        try:
            calendar = stock.calendar
            if calendar and "Revenue Average" in calendar:
                upcoming_revenue_estimate = safe_int(calendar["Revenue Average"])
        except Exception:
            pass

        # Calculate cutoff date (approximately 3 months per quarter)
        cutoff = datetime.now() - timedelta(days=quarters * 91)

        for idx, row in earnings_dates.iterrows():
            # idx is the earnings date (timestamp with timezone)
            earnings_dt = idx.to_pydatetime()

            # Skip if older than cutoff
            if earnings_dt.replace(tzinfo=None) < cutoff:
                continue

            # Extract EPS data
            eps_estimate = safe_decimal(row.get("EPS Estimate"))
            eps_actual = safe_decimal(row.get("Reported EPS"))
            surprise_pct = safe_decimal(row.get("Surprise(%)"))

            # Derive fiscal quarter and year from earnings date
            # Earnings reported in Q1 (Jan-Mar) are typically for previous Q4
            # This is approximate - companies have different fiscal years
            fiscal_year = earnings_dt.year
            month = earnings_dt.month
            if month in (1, 2, 3):
                fiscal_quarter = "Q4"
                fiscal_year -= 1
            elif month in (4, 5, 6):
                fiscal_quarter = "Q1"
            elif month in (7, 8, 9):
                fiscal_quarter = "Q2"
            else:  # 10, 11, 12
                fiscal_quarter = "Q3"

            # Derive timing from time (before/after market hours)
            hour = earnings_dt.hour
            if hour < 9:  # Before 9:30 AM ET typically
                timing = "BMO"
            elif hour >= 16:  # After 4 PM ET
                timing = "AMC"
            else:
                timing = "DMH"

            # Match revenue to fiscal period
            # Fiscal period end is typically last day of the quarter
            revenue_actual: int | None = None
            revenue_estimate: int | None = None

            # Calculate expected fiscal period end date
            quarter_end_month = {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}[fiscal_quarter]
            quarter_end_year = fiscal_year if fiscal_quarter != "Q4" else fiscal_year
            if fiscal_quarter == "Q4":
                quarter_end_year = fiscal_year  # Q4 of fiscal_year ends in Dec of fiscal_year

            # Try to find matching revenue in quarterly_revenue
            # Look for period ends around the expected quarter end
            for period_str, revenue in quarterly_revenue.items():
                period_date = datetime.strptime(period_str, "%Y-%m-%d").date()
                # Check if this period matches our fiscal quarter
                if period_date.year == quarter_end_year and period_date.month == quarter_end_month:
                    revenue_actual = revenue
                    break

            # For upcoming earnings (no actual EPS), use revenue estimate from calendar
            if eps_actual is None and upcoming_revenue_estimate:
                revenue_estimate = upcoming_revenue_estimate

            results.append(
                {
                    "ticker": ticker,
                    "report_date": earnings_dt.date(),
                    "fiscal_quarter": fiscal_quarter,
                    "fiscal_year": fiscal_year,
                    "timing": timing,
                    "eps_estimate": eps_estimate,
                    "eps_actual": eps_actual,
                    "surprise_pct": surprise_pct,
                    "revenue_estimate": revenue_estimate,
                    "revenue_actual": revenue_actual,
                }
            )

    except Exception as e:
        logger.debug(f"{ticker}: Error fetching earnings - {e}")

    return results


def load_earnings(
    conn,
    earnings: list[dict[str, Any]],
    logger: logging.Logger,
) -> int:
    """Load earnings into database using upsert."""
    if not earnings:
        return 0

    insert_sql = """
        INSERT INTO earnings (
            ticker, report_date, fiscal_quarter, fiscal_year,
            timing, eps_estimate, eps_actual, surprise_pct,
            revenue_estimate, revenue_actual
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, report_date) DO UPDATE SET
            fiscal_quarter = EXCLUDED.fiscal_quarter,
            fiscal_year = EXCLUDED.fiscal_year,
            timing = EXCLUDED.timing,
            eps_estimate = EXCLUDED.eps_estimate,
            eps_actual = EXCLUDED.eps_actual,
            surprise_pct = EXCLUDED.surprise_pct,
            revenue_estimate = EXCLUDED.revenue_estimate,
            revenue_actual = EXCLUDED.revenue_actual,
            updated_at = CURRENT_TIMESTAMP
    """

    loaded = 0
    with conn.cursor() as cur:
        for earn in earnings:
            try:
                cur.execute(
                    insert_sql,
                    (
                        earn["ticker"],
                        earn["report_date"],
                        earn["fiscal_quarter"],
                        earn["fiscal_year"],
                        earn["timing"],
                        earn["eps_estimate"],
                        earn["eps_actual"],
                        earn["surprise_pct"],
                        earn.get("revenue_estimate"),
                        earn.get("revenue_actual"),
                    ),
                )
                loaded += 1
            except psycopg.errors.ForeignKeyViolation:
                conn.rollback()
                logger.debug(f"Skipping earnings for unknown ticker: {earn['ticker']}")
                continue
            except Exception as e:
                conn.rollback()
                logger.debug(f"Error loading earnings for {earn['ticker']}: {e}")
                continue

    conn.commit()
    return loaded


def run_earnings_update(
    database_url: str,
    tickers: list[str] | None = None,
    quarters: int = DEFAULT_QUARTERS,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Download and store earnings data from Yahoo Finance.

    Args:
        database_url: PostgreSQL connection URL
        tickers: List of tickers to fetch (default: all active)
        quarters: Number of quarters of history to fetch (default: 5)
        dry_run: If True, show what would be done without writing
        logger: Logger instance

    Returns:
        Statistics dictionary with counts
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {
        "success": False,
        "tickers_processed": 0,
        "earnings_fetched": 0,
        "earnings_loaded": 0,
        "errors": 0,
    }

    logger.info("=" * 60)
    logger.info("EARNINGS UPDATE - Yahoo Finance")
    logger.info("=" * 60)
    logger.info(f"History: {quarters} quarters")

    with psycopg.connect(database_url) as conn:
        # Get tickers if not provided
        if tickers is None:
            tickers = get_active_tickers(conn)
            logger.info(f"Found {len(tickers)} active tickers")
        else:
            logger.info(f"Processing {len(tickers)} specified tickers")

        if dry_run:
            logger.info(f"\n[DRY RUN] Would fetch earnings for {len(tickers)} tickers")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        all_earnings: list[dict[str, Any]] = []

        # Fetch earnings for each ticker
        logger.info("\nFetching earnings data...")
        for i, ticker in enumerate(tickers, 1):
            if i % 50 == 0:
                logger.info(f"  Progress: {i}/{len(tickers)} ({len(all_earnings)} earnings found)")

            earnings = fetch_earnings_yfinance(ticker, quarters, logger)
            if earnings:
                all_earnings.extend(earnings)
                stats["tickers_processed"] += 1
            else:
                stats["errors"] += 1

        stats["earnings_fetched"] = len(all_earnings)
        logger.info(
            f"  Found {len(all_earnings)} earnings records from "
            f"{stats['tickers_processed']} tickers"
        )

        # Load into database
        if all_earnings:
            logger.info("\nLoading earnings into database...")
            stats["earnings_loaded"] = load_earnings(conn, all_earnings, logger)
            logger.info(f"  Loaded {stats['earnings_loaded']} earnings records")

    stats["success"] = True
    logger.info("\n" + "=" * 60)
    logger.info("EARNINGS UPDATE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"  Tickers processed: {stats['tickers_processed']}")
    logger.info(f"  Earnings fetched: {stats['earnings_fetched']}")
    logger.info(f"  Earnings loaded: {stats['earnings_loaded']}")
    if stats["errors"]:
        logger.info(f"  Tickers with no data: {stats['errors']}")

    return stats
