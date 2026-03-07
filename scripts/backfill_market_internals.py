#!/usr/bin/env python3
"""
Backfill market internals data from FRED.

Fetches VIX (VIXCLS), VIX3M (VXVCLS), and HY Spread (BAMLH0A0HYM2)
from the FRED API and loads into the market_internals table.

Usage:
    python scripts/backfill_market_internals.py
    python scripts/backfill_market_internals.py --start-date 2021-02-12
    python scripts/backfill_market_internals.py --dry-run

Requires:
    FRED_API_KEY environment variable
    DATABASE_URL environment variable
"""

import argparse
import os
import sys
from datetime import date

from dotenv import load_dotenv

load_dotenv()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sawa.api.fred import FredClient
from sawa.database.load import load_market_internals
from sawa.utils import setup_logging
from sawa.utils.dates import DATE_FORMAT


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill market internals from FRED")
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date YYYY-MM-DD (default: earliest stock_prices date)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch but do not load")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    fred_api_key = os.environ.get("FRED_API_KEY")
    database_url = os.environ.get("DATABASE_URL")

    if not fred_api_key:
        logger.error("FRED_API_KEY environment variable is required")
        logger.error("Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html")
        return 1
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        return 1

    import psycopg

    # Determine date range
    if args.start_date:
        start_date = args.start_date
    else:
        # Use earliest stock_prices date
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MIN(date) FROM stock_prices")
                row = cur.fetchone()
                if row and row[0]:
                    start_date = row[0].strftime(DATE_FORMAT)
                else:
                    start_date = "2021-01-01"
        logger.info(f"Using earliest stock_prices date: {start_date}")

    end_date = args.end_date or date.today().strftime(DATE_FORMAT)

    logger.info("=" * 60)
    logger.info("MARKET INTERNALS BACKFILL")
    logger.info("=" * 60)
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info("Source: FRED API")

    # Ensure table exists
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'market_internals'
                )
            """)
            row = cur.fetchone()
            if not (row and row[0]):
                logger.info("Creating market_internals table...")
                schema_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "sqlschema",
                    "26_market_internals.sql",
                )
                with open(schema_path) as f:
                    cur.execute(f.read())
                conn.commit()
                logger.info("Table created.")

    # Fetch data from FRED
    fred_client = FredClient(fred_api_key, logger)
    try:
        rows = fred_client.get_market_internals(start_date, end_date)
    finally:
        fred_client.close()

    # Enrich with VIX OHLC from Polygon daily index bars
    polygon_api_key = os.environ.get("POLYGON_API_KEY")
    if polygon_api_key:
        from sawa.api.client import PolygonClient
        from sawa.daily import _enrich_vix_ohlc

        polygon_client = PolygonClient(polygon_api_key, logger)
        try:
            _enrich_vix_ohlc(polygon_client, rows, start_date, end_date, logger)
        finally:
            polygon_client.close()
    else:
        logger.info("  POLYGON_API_KEY not set, skipping VIX OHLC enrichment")

    if not rows:
        logger.warning("No data returned from FRED")
        return 1

    logger.info(f"Fetched {len(rows)} rows from FRED")

    # Show sample
    sample = rows[:3] + rows[-3:] if len(rows) > 6 else rows
    for r in sample:
        vix = r.get("vix_close", "N/A")
        vix3m = r.get("vix3m", "N/A")
        hy = r.get("hy_spread", "N/A")
        logger.info(f"  {r['date']}: VIX={vix} VIX3M={vix3m} HY_Spread={hy}")

    if args.dry_run:
        logger.info(f"\n[DRY RUN] Would load {len(rows)} rows. Exiting.")
        return 0

    # Load into database
    with psycopg.connect(database_url) as conn:
        loaded = load_market_internals(conn, rows, logger)

    logger.info(f"\nBackfill complete: {loaded} rows loaded into market_internals")
    return 0


if __name__ == "__main__":
    sys.exit(main())
