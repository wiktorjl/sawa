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
from sawa.daily import fetch_prices_via_api, insert_prices
from sawa.domain.exceptions import ProviderError
from sawa.provider_downloads import bind_provider_record
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.dates import DATE_FORMAT
from sawa.utils.security import redact_sensitive_text
from sawa.utils.symbols import validate_ticker


class FundamentalLoadResult(dict[str, int]):
    """Per-table counts plus endpoint failures that ordinary dict callers ignore."""

    def __init__(self) -> None:
        super().__init__()
        self.failures: dict[str, str] = {}


class RatioLoadResult(int):
    """Committed ratio count with row-level persistence failures attached."""

    failed_rows: int

    def __new__(cls, value: int, failed_rows: int = 0) -> "RatioLoadResult":
        result = super().__new__(cls, value)
        result.failed_rows = failed_rows
        return result


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
) -> RatioLoadResult:
    """Fetch ratios via API and insert into database."""
    symbol = validate_ticker(symbol)
    ratios = client.get_ratios(symbol)

    if not isinstance(ratios, list):
        raise ProviderError("Invalid ratios response", provider="polygon")

    bound_ratios = [
        bind_provider_record(record, symbol, output_field="ticker")
        for record in ratios
    ]

    if not bound_ratios:
        return RatioLoadResult(0)

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
    failed_rows = 0
    with conn.cursor() as cur:
        for r in bound_ratios:
            cur.execute("SAVEPOINT ratio_row")
            try:
                cur.execute(
                    query,
                    (
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
                    ),
                )
                affected = max(cur.rowcount, 0)
            except psycopg.Error as e:
                cur.execute("ROLLBACK TO SAVEPOINT ratio_row")
                cur.execute("RELEASE SAVEPOINT ratio_row")
                failed_rows += 1
                logger.debug(f"  Ratio insert error: {redact_sensitive_text(e)}")
            else:
                cur.execute("RELEASE SAVEPOINT ratio_row")
                inserted += affected
    conn.commit()
    return RatioLoadResult(inserted, failed_rows)


def fetch_and_insert_fundamentals(
    conn,
    client: PolygonClient,
    symbol: str,
    start_date: str,
    end_date: str,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> FundamentalLoadResult:
    """Fetch fundamentals via API and insert into database."""
    symbol = validate_ticker(symbol)
    stats = FundamentalLoadResult()

    endpoints = {
        "balance-sheets": "balance_sheets",
        "income-statements": "income_statements",
        "cash-flow": "cash_flows",
    }

    for api_endpoint, table_name in endpoints.items():
        try:
            if rate_limiter is not None:
                rate_limiter.acquire()
            data = client.get_fundamentals(
                api_endpoint,
                ticker=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as e:
            message = redact_sensitive_text(e)
            logger.debug(f"  Failed to fetch {api_endpoint}: {message}")
            stats[table_name] = 0
            stats.failures[table_name] = message
            continue

        if not isinstance(data, list) or any(
            not isinstance(record, dict) for record in data
        ):
            message = "invalid provider response"
            logger.debug(f"  Failed to fetch {api_endpoint}: {message}")
            stats[table_name] = 0
            stats.failures[table_name] = message
            continue

        if not data:
            stats[table_name] = 0
            continue

        # Get table columns
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND column_name NOT IN ('id', 'created_at')
            """,
                (table_name,),
            )
            db_columns = {row[0] for row in cur.fetchall()}

        inserted = 0
        failed_rows = 0
        with conn.cursor() as cur:
            for record in data:
                try:
                    bound_record = bind_provider_record(
                        record,
                        symbol,
                        output_field="ticker",
                    )
                except ProviderError:
                    failed_rows += 1
                    logger.debug(
                        f"  {table_name} row has invalid or mismatched ticker identity"
                    )
                    continue

                # Map record to columns
                row_data: dict[str, Any] = {}
                for key, value in bound_record.items():
                    if key in {"ticker", "tickers"}:
                        continue
                    col_name = key.lower().replace(" ", "_").replace("-", "_")
                    if col_name in db_columns:
                        row_data[col_name] = value
                if "ticker" in db_columns:
                    row_data["ticker"] = symbol

                if not row_data:
                    failed_rows += 1
                    continue

                # Build upsert query
                cols = list(row_data.keys())
                cols_sql = sql.SQL(", ").join(map(sql.Identifier, cols))
                vals_sql = sql.SQL(", ").join([sql.Placeholder()] * len(cols))

                query = sql.SQL(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING"
                ).format(
                    sql.Identifier(table_name),
                    cols_sql,
                    vals_sql,
                )

                cur.execute("SAVEPOINT fundamental_row")
                try:
                    cur.execute(query, list(row_data.values()))
                    affected = max(cur.rowcount, 0)
                except psycopg.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT fundamental_row")
                    cur.execute("RELEASE SAVEPOINT fundamental_row")
                    failed_rows += 1
                    logger.debug(
                        f"  {table_name} insert error: {redact_sensitive_text(e)}"
                    )
                else:
                    cur.execute("RELEASE SAVEPOINT fundamental_row")
                    inserted += affected

        conn.commit()
        stats[table_name] = inserted
        if failed_rows:
            stats.failures[table_name] = (
                f"{failed_rows} of {len(data)} provider rows were rejected "
                "or failed to persist"
            )

    return stats


def fetch_and_insert_prices(
    conn,
    client: PolygonClient,
    symbol: str,
    start_date: str,
    end_date: str,
    logger: logging.Logger,
) -> int:
    """Fetch validated prices and insert them through the shared daily loader."""
    provider_stats: dict[str, Any] = {}
    prices = fetch_prices_via_api(
        client,
        [symbol],
        start_date,
        end_date,
        logger,
        stats=provider_stats,
    )
    if provider_stats.get("fetch_errors"):
        raise ProviderError("Price history request failed", provider="polygon")
    if not prices:
        message = (
            "Price history contained no valid rows"
            if provider_stats.get("provider_price_rows", 0)
            else "Price history returned no rows"
        )
        raise ProviderError(message, provider="polygon")
    return insert_prices(conn, prices, logger)


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
    stats: dict[str, Any] = {
        "success": False,
        "added": [],
        "failed": [],
        "skipped": [],
        "degraded": False,
        "degraded_symbols": [],
        "feed_failures": {},
    }

    if isinstance(years, bool) or not isinstance(years, int) or not 1 <= years <= 50:
        raise ValueError("years must be an integer between 1 and 50")

    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for candidate in symbols:
        label = redact_sensitive_text(candidate).replace("\n", " ").replace("\r", " ")[:32]
        try:
            if not isinstance(candidate, str):
                raise ValueError("ticker must be a string")
            symbol = validate_ticker(candidate)
        except (AttributeError, TypeError, ValueError):
            logger.warning("Rejected invalid ticker input")
            stats["failed"].append(label or "<invalid>")
            continue
        if symbol not in seen:
            seen.add(symbol)
            normalized_symbols.append(symbol)
    symbols = normalized_symbols

    logger.info("=" * 60)
    logger.info("ADD SYMBOLS")
    logger.info("=" * 60)
    logger.info(f"Symbols to add: {', '.join(symbols) if symbols else '(none valid)'}")

    # Calculate date range
    end_date = date.today()
    try:
        start_date = date(end_date.year - years, end_date.month, end_date.day)
    except ValueError:
        # A leap-day run replaying to a non-leap year starts on February 28.
        start_date = date(end_date.year - years, end_date.month, 28)
    start_str = start_date.strftime(DATE_FORMAT)
    end_str = end_date.strftime(DATE_FORMAT)
    logger.info(f"Price history: {start_str} to {end_str} ({years} years)")

    if dry_run:
        logger.info("\n[DRY RUN] Would add:")
        for sym in symbols:
            logger.info(f"  - {sym}")
        stats["success"] = bool(symbols)
        stats["degraded"] = bool(stats["failed"])
        stats["dry_run"] = True
        return stats

    if not symbols:
        stats["degraded"] = bool(stats["failed"])
        return stats

    # Initialize the provider only after validation and the dry-run exit.
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    def record_feed_failure(symbol: str, feed: str, error: object) -> None:
        message = redact_sensitive_text(error)
        failures = stats["feed_failures"].setdefault(symbol, {})
        failures[feed] = message
        if symbol not in stats["degraded_symbols"]:
            stats["degraded_symbols"].append(symbol)
        logger.warning(f"  {symbol}: {feed} failed: {message}")

    try:
        with psycopg.connect(database_url) as conn:
            existing = get_existing_symbols(conn)
            logger.info(f"Existing symbols in database: {len(existing)}")

        for i, symbol in enumerate(symbols, 1):
            logger.info(f"\n[{i}/{len(symbols)}] Processing {symbol}...")

            if symbol in existing:
                logger.info("  Already exists, updating...")

            try:
                # Fetch company details
                rate_limiter.acquire()
                logger.info("  Fetching company details...")
                company_data = client.get_ticker_details(symbol)

                if not isinstance(company_data, dict) or not company_data:
                    raise ProviderError(
                        "Company details unavailable", provider="polygon"
                    )
                company_data = bind_provider_record(
                    company_data,
                    symbol,
                    output_field="ticker",
                )

                # Insert company
                with psycopg.connect(database_url) as conn:
                    if not insert_company(conn, company_data, logger):
                        raise RuntimeError("Company record was not persisted")
                logger.info(f"  Inserted company: {company_data.get('name', symbol)}")

            except Exception as company_error:
                record_feed_failure(symbol, "company", company_error)
                if symbol not in existing:
                    stats["failed"].append(symbol)
                    continue

            primary_failed = False
            try:
                rate_limiter.acquire()
                logger.info(f"  Fetching {years} years of price history...")
                with psycopg.connect(database_url) as conn:
                    price_count = fetch_and_insert_prices(
                        conn, client, symbol, start_str, end_str, logger
                    )
                logger.info(f"  Inserted {price_count} price records")
            except Exception as price_error:
                primary_failed = True
                record_feed_failure(symbol, "prices", price_error)

            try:
                rate_limiter.acquire()
                logger.info("  Fetching financial ratios...")
                with psycopg.connect(database_url) as conn:
                    ratio_count = fetch_and_insert_ratios(conn, client, symbol, logger)
                logger.info(f"  Inserted {ratio_count} ratio records")
                failed_ratio_rows = getattr(ratio_count, "failed_rows", 0)
                if failed_ratio_rows:
                    record_feed_failure(
                        symbol,
                        "ratios",
                        f"{failed_ratio_rows} of "
                        f"{failed_ratio_rows + int(ratio_count)} ratio rows "
                        "failed to persist",
                    )
            except Exception as ratio_error:
                record_feed_failure(symbol, "ratios", ratio_error)

            try:
                logger.info("  Fetching fundamentals...")
                with psycopg.connect(database_url) as conn:
                    fund_stats = fetch_and_insert_fundamentals(
                        conn,
                        client,
                        symbol,
                        start_str,
                        end_str,
                        logger,
                        rate_limiter,
                    )
                total_fund = sum(fund_stats.values())
                logger.info(f"  Inserted {total_fund} fundamental records")
                for feed, message in fund_stats.failures.items():
                    record_feed_failure(symbol, feed, message)
            except Exception as fundamentals_error:
                record_feed_failure(symbol, "fundamentals", fundamentals_error)

            if primary_failed:
                stats["failed"].append(symbol)
            elif symbol in existing:
                stats["skipped"].append(symbol)  # Updated existing
            else:
                stats["added"].append(symbol)

        stats["success"] = bool(stats["added"] or stats["skipped"])
        stats["degraded"] = bool(stats["failed"] or stats["feed_failures"])
        stats["success_count"] = len(stats["added"]) + len(stats["skipped"])
        logger.info("\n" + "=" * 60)
        logger.info("ADD SYMBOLS COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Added: {len(stats['added'])}")
        logger.info(f"  Updated: {len(stats['skipped'])}")
        logger.info(f"  Failed: {len(stats['failed'])}")
        logger.info(f"  Degraded: {len(stats['degraded_symbols'])}")

    except Exception as e:
        message = redact_sensitive_text(e)
        logger.error(f"Add symbols failed: {message}")
        stats["error"] = message
        raise

    return stats
