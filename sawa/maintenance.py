"""Recurring discovery, financial statements/ratios, and earnings maintenance."""

import logging
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

import psycopg

from sawa.add_symbol import fetch_and_insert_prices, insert_company
from sawa.api import PolygonClient
from sawa.coldstart import (
    MAXIMUM_INDEX_SOURCE_COUNTS,
    MINIMUM_INDEX_SOURCE_COUNTS,
    _index_fetchers,
    populate_index_constituents,
)
from sawa.earnings import run_earnings_update
from sawa.provider_downloads import bind_provider_record
from sawa.quarterly import run_quarterly
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.security import redact_sensitive_text
from sawa.utils.symbols import validate_ticker


def _reconciliation_due(path: Path, today: date) -> bool:
    try:
        previous = date.fromisoformat(path.read_text().strip())
    except (OSError, ValueError):
        return True
    return previous.year != today.year or previous.month != today.month


def _record_reconciliation(path: Path, today: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(today.isoformat() + "\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def refresh_universe(api_key: str, database_url: str, logger: logging.Logger) -> dict[str, Any]:
    """Discover once, onboard missing companies/history, then refresh membership.

    No company is deleted or deactivated merely because a listing source omits
    it. Failed sources keep their prior membership; an incomplete onboarding
    also preserves the affected index's prior snapshot.
    """
    stats: dict[str, Any] = {"success": False, "sources": {}, "onboarded": [], "failures": {}}
    snapshots: dict[str, list[str]] = {}
    for code, fetcher in _index_fetchers(api_key):
        try:
            raw = fetcher(logger)
            if not isinstance(raw, list):
                raise ValueError("Constituent source returned a non-list response")
            symbols = sorted({validate_ticker(symbol) for symbol in raw})
            if (
                not MINIMUM_INDEX_SOURCE_COUNTS.get(code, 1)
                <= len(symbols)
                <= (MAXIMUM_INDEX_SOURCE_COUNTS.get(code, 100_000))
            ):
                raise ValueError(f"Implausible constituent count: {len(symbols)}")
            snapshots[code] = symbols
            stats["sources"][code] = len(symbols)
        except Exception as exc:
            stats["failures"][code] = redact_sensitive_text(exc)
    discovered = sorted({symbol for symbols in snapshots.values() for symbol in symbols})
    with psycopg.connect(database_url) as conn:
        known = {
            ticker: (active, has_prices)
            for ticker, active, has_prices in conn.execute(
                "SELECT c.ticker, c.active, EXISTS (SELECT 1 FROM stock_prices p "
                "WHERE p.ticker = c.ticker) FROM companies c"
            ).fetchall()
        }
    pending = [symbol for symbol in discovered if symbol not in known or not all(known[symbol])]
    stats["pending"] = len(pending)
    today = date.today()
    start = today.replace(year=today.year - 5, day=min(today.day, 28))
    limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)
    failed_onboarding: set[str] = set()
    with PolygonClient(api_key, logger) as client:
        for symbol in pending:
            try:
                limiter.acquire()
                details = client.get_ticker_details(symbol)
                if not isinstance(details, dict) or not details:
                    raise ValueError("Company details unavailable")
                details = bind_provider_record(details, symbol, output_field="ticker")
                if details.get("active") is False:
                    raise ValueError("Discovered listing is inactive at the provider")
                details.setdefault("active", True)
                with psycopg.connect(database_url) as conn:
                    if not insert_company(conn, details, logger):
                        raise RuntimeError("Company was not persisted")
                    conn.execute("UPDATE companies SET active = true WHERE ticker = %s", (symbol,))
                if symbol not in known or not known[symbol][1]:
                    limiter.acquire()
                    with psycopg.connect(database_url) as conn:
                        fetch_and_insert_prices(
                            conn, client, symbol, start.isoformat(), today.isoformat(), logger
                        )
                stats["onboarded"].append(symbol)
            except Exception as exc:
                failed_onboarding.add(symbol)
                stats["failures"][f"onboard:{symbol}"] = redact_sensitive_text(exc)
    complete_snapshots = {}
    for code, symbols in snapshots.items():
        incomplete = failed_onboarding.intersection(symbols)
        if incomplete:
            stats["failures"][code] = (
                f"Preserving membership: {len(incomplete)} constituents failed onboarding"
            )
        else:
            complete_snapshots[code] = symbols
    if complete_snapshots:
        with psycopg.connect(database_url) as conn:
            indices = populate_index_constituents(
                conn,
                logger,
                api_key=api_key,
                prefetched_symbols=complete_snapshots,
                require_complete=True,
            )
        stats["indices"] = indices.summary()
        stats["failures"].update(indices.failures)
    stats["success"] = bool(snapshots) and not stats["failures"]
    stats["degraded"] = not stats["success"]
    return stats


def run_maintenance(
    api_key: str,
    database_url: str,
    output_dir: Path = Path("data"),
    *,
    skip_universe: bool = False,
    skip_fundamentals: bool = False,
    skip_earnings: bool = False,
    full_history: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run independent maintenance stages; never mark incomplete work successful."""
    log = logger or logging.getLogger(__name__)
    stats: dict[str, Any] = {"success": False, "steps": {}, "errors": {}}
    today = date.today()
    reconciliation_path = output_dir / ".fundamentals_reconciled"
    full_history = full_history or _reconciliation_due(reconciliation_path, today)
    stats["full_history"] = full_history
    stages = []
    if not skip_universe:
        stages.append(("universe", lambda: refresh_universe(api_key, database_url, log)))
    if not skip_fundamentals:
        stages.append(
            (
                "fundamentals",
                lambda: run_quarterly(
                    api_key,
                    database_url,
                    output_dir,
                    logger=log,
                    full_history=full_history,
                ),
            )
        )
    if not skip_earnings:
        stages.append(("earnings", lambda: run_earnings_update(database_url, logger=log)))
    if not stages:
        raise ValueError("At least one maintenance stage must be enabled")
    if dry_run:
        log.info(
            "[DRY RUN] Maintenance stages: %s; full history: %s",
            ", ".join(name for name, _ in stages),
            full_history,
        )
        return {**stats, "success": True, "dry_run": True, "planned": [name for name, _ in stages]}
    for name, run in stages:
        try:
            result = run()
            stats["steps"][name] = result
            if not result.get("success") or result.get("degraded"):
                raise RuntimeError(f"{name} returned an incomplete result")
            if name == "fundamentals" and full_history:
                _record_reconciliation(reconciliation_path, today)
        except Exception as exc:
            stats["errors"][name] = redact_sensitive_text(exc)
            log.error("Maintenance %s failed: %s", name, stats["errors"][name])
    stats["success"] = not stats["errors"]
    stats["degraded"] = bool(stats["errors"])
    return stats
