"""Dated market moves independently checked during historical price recovery.

Reviewed 2026-09-29 against Yahoo Finance split-adjusted daily closes, with
Polygon/Yahoo agreement across the five overlapping provider-window bars.
Evidence and input hashes are recorded in docs/LOG_SCAN_2026-09-29.md and the
recovery artifacts. These entries permit no price changes and supply no rebase
factor: they only explain a particular close-to-close move. Ticker, both dates,
and the observed ratio must match; issuer and split-ledger checks still apply.
"""

from datetime import date
from decimal import Decimal

_MOVES = {
    ("EJH", date(2021, 8, 2), date(2021, 8, 3)): Decimal("0.38899803536345776"),
    ("OCG", date(2021, 3, 12), date(2021, 3, 15)): Decimal("2.2037914691943128"),
    ("RCON", date(2021, 6, 10), date(2021, 6, 11)): Decimal("0.48979591836734694"),
    ("RVPH", date(2021, 4, 23), date(2021, 4, 26)): Decimal("2.0138888888888889"),
    ("EEIQ", date(2021, 9, 28), date(2021, 9, 29)): Decimal("0.74485125858123570"),
    ("EEIQ", date(2021, 9, 29), date(2021, 9, 30)): Decimal("0.63133640552995392"),
}


def is_reviewed_price_move(ticker: str, left: date, right: date, step: Decimal) -> bool:
    """Accept only the reviewed dated ratio, with 0.5% rounding tolerance."""
    expected = _MOVES.get((ticker, left, right))
    return (
        expected is not None and step.is_finite() and step > 0
        and abs(step / expected - 1) <= Decimal("0.005")
    )
