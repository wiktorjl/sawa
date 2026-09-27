"""Scheduled Yahoo earnings ingestion with atomic per-ticker persistence."""

import logging
import math
import time
from typing import Any
from urllib.parse import urlsplit

import psycopg

from sawa.domain.exceptions import ProviderError
from sawa.utils.market_hours import regular_session_close
from sawa.utils.security import redact_sensitive_text


def _new_yahoo_session():
    """Observe final earnings HTTP outcomes without interrupting auth retries.

    yfinance's scraper returns None for both a legitimate no-table answer and
    an HTTP error page. Its transport retries authentication internally, so
    raising immediately from a response hook would prevent that recovery.
    """
    from curl_cffi.requests import Session

    class EarningsSession(Session):
        last_earnings_status: int | None = None

        def request(self, method: str, url: str, *args, **kwargs):
            response = super().request(method, url, *args, **kwargs)
            parsed = urlsplit(url)
            if parsed.hostname == "finance.yahoo.com" and parsed.path == "/calendar/earnings":
                # A recovered retry replaces the earlier failed status.
                self.last_earnings_status = response.status_code
            return response

    return EarningsSession(impersonate="chrome")


def _number(value: Any) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    if math.isnan(parsed):
        return None
    if not math.isfinite(parsed):
        raise ValueError("Non-finite earnings value")
    return parsed


def fetch_earnings(ticker: str) -> list[dict[str, Any]]:
    """Fetch a provider response; exceptions remain failures, empty is explicit.

    Yahoo supplies report dates, not fiscal quarter identifiers. Do not invent
    fiscal periods from the calendar quarter of a reporting date.
    """
    import yfinance as yf

    with _new_yahoo_session() as session:
        frame = yf.Ticker(ticker.replace(".", "-"), session=session).get_earnings_dates(limit=20)
        status = session.last_earnings_status
        if status is not None and status >= 400:
            raise ProviderError(
                f"Yahoo earnings page returned HTTP {status} after retries", provider="yahoo"
            )
    if frame is None or frame.empty:
        return []
    rows = []
    for timestamp, row in frame.iterrows():
        if not hasattr(timestamp, "date") or timestamp != timestamp:
            raise ValueError("Earnings response has an invalid report date")
        if getattr(timestamp, "tzinfo", None) is not None:
            timestamp = timestamp.tz_convert("America/New_York")
        report_time = (timestamp.hour, timestamp.minute)
        session_close = regular_session_close(timestamp.date())
        # Yahoo can list weekend/holiday reports; retain the conventional
        # session boundaries then, and honor scheduled half days otherwise.
        close_time = (session_close.hour, session_close.minute) if session_close else (16, 0)
        timing = "BMO" if report_time < (9, 30) else "AMC" if report_time >= close_time else "DMH"
        rows.append(
            {
                "ticker": ticker,
                "report_date": timestamp.date(),
                "timing": timing,
                "eps_estimate": _number(row["EPS Estimate"]),
                "eps_actual": _number(row["Reported EPS"]),
                "surprise_pct": _number(row["Surprise(%)"]),
            }
        )
    return rows


def persist_earnings(conn, rows: list[dict[str, Any]]) -> int:
    """Commit a complete ticker response or roll it back, including date repairs."""
    if not rows:
        return 0
    incoming_dates = [row["report_date"] for row in rows]
    with conn.transaction():
        for row in rows:
            if row["eps_actual"] is not None:
                conn.execute(
                    "DELETE FROM earnings WHERE ticker = %(ticker)s "
                    "AND eps_actual IS NULL AND ABS(report_date - %(report_date)s) <= 7 "
                    "AND report_date <> ALL(%(incoming_dates)s)",
                    {**row, "incoming_dates": incoming_dates},
                )
            conn.execute(
                """INSERT INTO earnings
                    (ticker, report_date, timing, eps_estimate, eps_actual, surprise_pct)
                VALUES (%(ticker)s, %(report_date)s, %(timing)s, %(eps_estimate)s,
                        %(eps_actual)s, %(surprise_pct)s)
                ON CONFLICT (ticker, report_date) DO UPDATE SET
                    timing = COALESCE(EXCLUDED.timing, earnings.timing),
                    eps_estimate = COALESCE(EXCLUDED.eps_estimate, earnings.eps_estimate),
                    eps_actual = COALESCE(EXCLUDED.eps_actual, earnings.eps_actual),
                    surprise_pct = COALESCE(EXCLUDED.surprise_pct, earnings.surprise_pct),
                    updated_at = NOW()""",
                row,
            )
    return len(rows)


def run_earnings_update(
    database_url: str,
    *,
    logger: logging.Logger | None = None,
    request_interval: float = 2.0,
) -> dict[str, Any]:
    """Refresh earnings for operating companies; funds do not report EPS."""
    log = logger or logging.getLogger(__name__)
    stats: dict[str, Any] = {
        "success": False,
        "requested": 0,
        "succeeded": 0,
        "empty": 0,
        "persisted": 0,
        "failures": {},
    }
    # Autocommit lets each transaction below be an independent committed ticker,
    # so a provider outage does not roll back earlier successful tickers.
    with psycopg.connect(database_url, autocommit=True) as conn:
        tickers = [
            row[0]
            for row in conn.execute(
                "SELECT ticker FROM companies WHERE active = true "
                "AND (type IN ('CS', 'ADRC') OR type IS NULL) "
                "ORDER BY market_cap DESC NULLS LAST, ticker"
            ).fetchall()
        ]
        stats["requested"] = len(tickers)
        for position, ticker in enumerate(tickers):
            if position and request_interval:
                time.sleep(request_interval)
            try:
                rows = fetch_earnings(ticker)
                stats["persisted"] += persist_earnings(conn, rows)
                stats["succeeded"] += 1
                stats["empty"] += int(not rows)
            except Exception as exc:
                message = redact_sensitive_text(exc)
                stats["failures"][ticker] = message
                log.warning("Earnings %s failed: %s", ticker, message)
            if (position + 1) % 100 == 0:
                log.info(
                    "Earnings: %s/%s tickers, %s rows, %s failures",
                    position + 1,
                    len(tickers),
                    stats["persisted"],
                    len(stats["failures"]),
                )
    stats["success"] = bool(stats["persisted"]) and not stats["failures"]
    stats["degraded"] = not stats["success"]
    if not stats["persisted"]:
        stats["error"] = "Earnings refresh produced no persisted observations"
    return stats
