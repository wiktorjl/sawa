"""Persistent issuer boundaries for ticker-reuse price quarantines."""

from collections.abc import Iterable, Mapping
from datetime import date
from typing import Any


def get_identity_price_cutoffs(
    conn, tickers: Iterable[str] | None = None,
) -> dict[str, date]:
    """Read reviewed identity cutoffs; legacy split-basis archives are unrelated.

    Missing archives are normal on a fresh installation. Existing identity
    evidence must still match the company registry; a later issuer change
    requires review instead of silently reusing an obsolete boundary.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_catalog.to_regclass('public.stock_prices_unadjustable_archive')")
        row = cur.fetchone()
        if not row or not row[0]:
            return {}
        selected = sorted(set(tickers)) if tickers is not None else None
        cur.execute(
            "SELECT DISTINCT a.ticker, a.stale_basis_cutoff, "
            "pg_catalog.to_jsonb(a)->'identity_evidence'->>'current_cik', c.cik "
            "FROM public.stock_prices_unadjustable_archive a "
            "LEFT JOIN public.companies c ON c.ticker = a.ticker "
            "WHERE pg_catalog.to_jsonb(a)->>'archive_reason' = 'identity_mismatch'"
            + (" AND a.ticker = ANY(%s)" if selected is not None else ""),
            (selected,) if selected is not None else (),
        )
        rows = cur.fetchall()
    cutoffs: dict[str, date] = {}
    for ticker, cutoff, expected_cik, current_cik in rows:
        if (
            type(cutoff) is not date
            or not str(expected_cik).isdecimal()
            or not str(current_cik).isdecimal()
            or int(expected_cik) != int(current_cik)
        ):
            raise ValueError(f"{ticker}: quarantined issuer boundary requires identity review")
        cutoffs[ticker] = max(cutoff, cutoffs.get(ticker, cutoff))
    return cutoffs


def price_precedes_identity_cutoff(
    row: Mapping[str, Any], cutoffs: Mapping[str, date],
) -> bool:
    """True only for a row in an explicitly reviewed obsolete-issuer range."""
    cutoff = cutoffs.get(str(row.get("ticker", "")).upper())
    if cutoff is None:
        return False
    raw_date = row.get("date")
    session = raw_date if type(raw_date) is date else date.fromisoformat(str(raw_date))
    return session < cutoff


def filter_identity_price_rows(
    rows: list[dict[str, Any]], cutoffs: Mapping[str, date],
) -> tuple[list[dict[str, Any]], int]:
    retained = [row for row in rows if not price_precedes_identity_cutoff(row, cutoffs)]
    return retained, len(rows) - len(retained)
