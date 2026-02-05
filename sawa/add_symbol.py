"""
Add new symbols to the database.

Usage:
    sawa add-symbol AAPL MSFT       # Add specific symbols
    sawa add-symbol --file stocks.txt  # Add from file (one per line)
"""

import logging
from datetime import date
from typing import Any

import psycopg
from psycopg import sql

from sawa.api import PolygonClient
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.dates import DATE_FORMAT, timestamp_to_date

# Ratios columns mapping
RATIO_COLUMNS = [
    "ticker", "date", "average_volume", "cash", "current", "debt_to_equity",
    "dividend_yield", "earnings_per_share", "enterprise_value", "ev_to_ebitda",
    "ev_to_sales", "free_cash_flow", "market_cap", "price", "price_to_book",
    "price_to_cash_flow", "price_to_earnings", "price_to_free_cash_flow",
    "price_to_sales", "quick", "return_on_assets", "return_on_equity",
]


def get_existing_symbols(conn) -> set[str]:
    """Get set of symbols already in database."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies")
        return {row[0] for row in cur.fetchall()}


def insert_company(conn, data: dict[str, Any], logger: logging.Logger) -> bool:
    """Insert company into database."""
    if not data:
        return False

    # Flatten nested fields
    flat: dict[str, Any] = {}
    for k, v in data.items():
        if isinstance(v, dict):
            for sub_k, sub_v in v.items():
                flat[f"{k}_{sub_k}"] = sub_v
        else:
            flat[k] = v

    query = sql.SQL("""
        INSERT INTO companies (
            ticker, name, description, market, type, locale, currency_name,
            active, list_date, primary_exchange, cik, sic_code, sic_description,
            market_cap, weighted_shares_outstanding, total_employees,
            homepage_url, phone_number, address_address1, address_city,
            address_state, address_postal_code
        ) VALUES (
            %(ticker)s, %(name)s, %(description)s, %(market)s, %(type)s,
            %(locale)s, %(currency_name)s, %(active)s, %(list_date)s,
            %(primary_exchange)s, %(cik)s, %(sic_code)s, %(sic_description)s,
            %(market_cap)s, %(weighted_shares_outstanding)s, %(total_employees)s,
            %(homepage_url)s, %(phone_number)s, %(address_address1)s,
            %(address_city)s, %(address_state)s, %(address_postal_code)s
        )
        ON CONFLICT (ticker) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            market_cap = EXCLUDED.market_cap,
            total_employees = EXCLUDED.total_employees
    """)

    params = {
        "ticker": flat.get("ticker"),
        "name": flat.get("name"),
        "description": flat.get("description"),
        "market": flat.get("market"),
        "type": flat.get("type"),
        "locale": flat.get("locale"),
        "currency_name": flat.get("currency_name"),
        "active": flat.get("active"),
        "list_date": flat.get("list_date"),
        "primary_exchange": flat.get("primary_exchange"),
        "cik": flat.get("cik"),
        "sic_code": flat.get("sic_code"),
        "sic_description": flat.get("sic_description"),
        "market_cap": flat.get("market_cap"),
        "weighted_shares_outstanding": flat.get("weighted_shares_outstanding"),
        "total_employees": flat.get("total_employees"),
        "homepage_url": flat.get("homepage_url"),
        "phone_number": flat.get("phone_number"),
        "address_address1": flat.get("address_address1"),
        "address_city": flat.get("address_city"),
        "address_state": flat.get("address_state"),
        "address_postal_code": flat.get("address_postal_code"),
    }

    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()
    return True


def fetch_and_insert_ratios(
    conn,
    client: PolygonClient,
    symbol: str,
    logger: logging.Logger,
) -> int:
    """Fetch ratios via API and insert into database."""
    try:
        ratios = client.get_ratios(symbol)
    except Exception as e:
        logger.warning(f"  Failed to fetch ratios: {e}")
        return 0

    if not ratios:
        return 0

    query = sql.SQL("""
        INSERT INTO financial_ratios (
            ticker, date, average_volume, cash, current, debt_to_equity,
            dividend_yield, earnings_per_share, enterprise_value, ev_to_ebitda,
            ev_to_sales, free_cash_flow, market_cap, price, price_to_book,
            price_to_cash_flow, price_to_earnings, price_to_free_cash_flow,
            price_to_sales, quick, return_on_assets, return_on_equity
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (ticker, date) DO UPDATE SET
            average_volume = EXCLUDED.average_volume,
            market_cap = EXCLUDED.market_cap,
            price = EXCLUDED.price,
            price_to_earnings = EXCLUDED.price_to_earnings,
            dividend_yield = EXCLUDED.dividend_yield
    """)

    inserted = 0
    with conn.cursor() as cur:
        for r in ratios:
            r["ticker"] = symbol
            try:
                cur.execute(query, (
                    r.get("ticker"),
                    r.get("date"),
                    r.get("average_volume"),
                    r.get("cash"),
                    r.get("current"),
                    r.get("debt_to_equity"),
                    r.get("dividend_yield"),
                    r.get("earnings_per_share"),
                    r.get("enterprise_value"),
                    r.get("ev_to_ebitda"),
                    r.get("ev_to_sales"),
                    r.get("free_cash_flow"),
                    r.get("market_cap"),
                    r.get("price"),
                    r.get("price_to_book"),
                    r.get("price_to_cash_flow"),
                    r.get("price_to_earnings"),
                    r.get("price_to_free_cash_flow"),
                    r.get("price_to_sales"),
                    r.get("quick"),
                    r.get("return_on_assets"),
                    r.get("return_on_equity"),
                ))
                inserted += 1
            except Exception as e:
                logger.debug(f"  Ratio insert error: {e}")
                conn.rollback()
    conn.commit()
    return inserted


def fetch_and_insert_fundamentals(
    conn,
    client: PolygonClient,
    symbol: str,
    start_date: str,
    end_date: str,
    logger: logging.Logger,
) -> dict[str, int]:
    """Fetch fundamentals via API and insert into database."""
    stats: dict[str, int] = {}

    endpoints = {
        "balance-sheets": "balance_sheets",
        "income-statements": "income_statements",
        "cash-flow": "cash_flows",
    }

    for api_endpoint, table_name in endpoints.items():
        try:
            data = client.get_fundamentals(
                api_endpoint,
                ticker=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as e:
            logger.debug(f"  Failed to fetch {api_endpoint}: {e}")
            stats[table_name] = 0
            continue

        if not data:
            stats[table_name] = 0
            continue

        # Get table columns
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND column_name NOT IN ('id', 'created_at')
            """, (table_name,))
            db_columns = {row[0] for row in cur.fetchall()}

        inserted = 0
        for record in data:
            # Clean up tickers field
            if "tickers" in record and isinstance(record["tickers"], list):
                record["ticker"] = record["tickers"][0] if record["tickers"] else symbol
            elif "tickers" not in record:
                record["ticker"] = symbol

            # Map record to columns
            row_data = {}
            for key, value in record.items():
                col_name = key.lower().replace(" ", "_").replace("-", "_")
                if col_name in db_columns:
                    row_data[col_name] = value
                elif key == "tickers":
                    row_data["ticker"] = value if isinstance(value, str) else symbol

            if not row_data:
                continue

            # Build upsert query
            cols = list(row_data.keys())
            cols_sql = sql.SQL(", ").join(map(sql.Identifier, cols))
            vals_sql = sql.SQL(", ").join([sql.Placeholder()] * len(cols))

            query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
                sql.Identifier(table_name),
                cols_sql,
                vals_sql,
            )

            try:
                with conn.cursor() as cur:
                    cur.execute(query, list(row_data.values()))
                inserted += 1
            except Exception as e:
                logger.debug(f"  {table_name} insert error: {e}")
                conn.rollback()

        conn.commit()
        stats[table_name] = inserted

    return stats


def fetch_and_insert_prices(
    conn,
    client: PolygonClient,
    symbol: str,
    start_date: str,
    end_date: str,
    logger: logging.Logger,
) -> int:
    """Fetch prices via API and insert into database."""
    try:
        data = client.get(
            "aggregates",
            path_params={"ticker": symbol, "start": start_date, "end": end_date},
            params={"adjusted": "true", "limit": 50000},
        )
    except Exception as e:
        logger.warning(f"  Failed to fetch prices: {e}")
        return 0

    results = data.get("results", [])
    if not results:
        return 0

    query = sql.SQL("""
        INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """)

    inserted = 0
    with conn.cursor() as cur:
        for r in results:
            if r.get("t"):
                price_date = timestamp_to_date(r["t"]).strftime(DATE_FORMAT)
                cur.execute(query, (
                    symbol,
                    price_date,
                    r.get("o"),
                    r.get("h"),
                    r.get("l"),
                    r.get("c"),
                    r.get("v"),
                ))
                inserted += 1
    conn.commit()
    return inserted


def run_add_symbols(
    api_key: str,
    database_url: str,
    symbols: list[str],
    years: int = 5,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Add new symbols to database.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        symbols: List of ticker symbols to add
        years: Years of price history to fetch
        dry_run: If True, show what would be done
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False, "added": [], "failed": [], "skipped": []}

    logger.info("=" * 60)
    logger.info("ADD SYMBOLS")
    logger.info("=" * 60)
    logger.info(f"Symbols to add: {', '.join(symbols)}")

    # Initialize client
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    # Calculate date range
    end_date = date.today()
    start_date = date(end_date.year - years, end_date.month, end_date.day)
    start_str = start_date.strftime(DATE_FORMAT)
    end_str = end_date.strftime(DATE_FORMAT)
    logger.info(f"Price history: {start_str} to {end_str} ({years} years)")

    if dry_run:
        logger.info("\n[DRY RUN] Would add:")
        for sym in symbols:
            logger.info(f"  - {sym}")
        stats["success"] = True
        stats["dry_run"] = True
        return stats

    try:
        with psycopg.connect(database_url) as conn:
            existing = get_existing_symbols(conn)
            logger.info(f"Existing symbols in database: {len(existing)}")

        for i, symbol in enumerate(symbols, 1):
            symbol = symbol.upper().strip()
            logger.info(f"\n[{i}/{len(symbols)}] Processing {symbol}...")

            if symbol in existing:
                logger.info("  Already exists, updating...")

            # Fetch company details
            rate_limiter.acquire()
            logger.info("  Fetching company details...")
            company_data = client.get_ticker_details(symbol)

            if not company_data:
                logger.warning(f"  Could not fetch company details for {symbol}")
                stats["failed"].append(symbol)
                continue

            # Insert company
            with psycopg.connect(database_url) as conn:
                insert_company(conn, company_data, logger)
            logger.info(f"  Inserted company: {company_data.get('name', symbol)}")

            # Fetch and insert prices
            rate_limiter.acquire()
            logger.info(f"  Fetching {years} years of price history...")
            with psycopg.connect(database_url) as conn:
                price_count = fetch_and_insert_prices(
                    conn, client, symbol, start_str, end_str, logger
                )
            logger.info(f"  Inserted {price_count} price records")

            # Fetch and insert ratios
            rate_limiter.acquire()
            logger.info("  Fetching financial ratios...")
            with psycopg.connect(database_url) as conn:
                ratio_count = fetch_and_insert_ratios(conn, client, symbol, logger)
            logger.info(f"  Inserted {ratio_count} ratio records")

            # Fetch and insert fundamentals
            rate_limiter.acquire()
            logger.info("  Fetching fundamentals...")
            with psycopg.connect(database_url) as conn:
                fund_stats = fetch_and_insert_fundamentals(
                    conn, client, symbol, start_str, end_str, logger
                )
            total_fund = sum(fund_stats.values())
            logger.info(f"  Inserted {total_fund} fundamental records")

            if symbol in existing:
                stats["skipped"].append(symbol)  # Updated existing
            else:
                stats["added"].append(symbol)

        stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info("ADD SYMBOLS COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Added: {len(stats['added'])}")
        logger.info(f"  Updated: {len(stats['skipped'])}")
        logger.info(f"  Failed: {len(stats['failed'])}")

    except Exception as e:
        logger.error(f"Add symbols failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
