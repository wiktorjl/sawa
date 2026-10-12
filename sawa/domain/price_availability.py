"""Reviewed reasons an active listing can have no prices in our history window."""

from datetime import date
from typing import Any


def is_explicit_empty_price_response(response: Any, ticker: str) -> bool:
    """Require a successful, matching aggregate response with explicit zero counts."""
    return (
        isinstance(response, dict)
        and response.get("ticker") == ticker
        and response.get("status") in {"OK", "DELAYED"}
        and response.get("adjusted") is True
        and type(response.get("queryCount")) is int
        and response["queryCount"] == 0
        and type(response.get("resultsCount")) is int
        and response["resultsCount"] == 0
        and response.get("results", []) == []
    )


# Reviewed 2026-09-29 against the latest annual report and a successful empty
# five-year Polygon response. This is issuer-specific, not a general exception
# for empty responses. Every maintenance run still requests prices, so a resumed
# listing is automatically ingested. A changed issuer must be reviewed again.
_SVA_HALT_SOURCE = (
    "https://www.sec.gov/Archives/edgar/data/1084201/"
    "000119312526258534/sva-20251231.htm"
)


def reviewed_empty_price_history(
    ticker: str, details: dict[str, Any], start: date,
) -> str | None:
    """Explain a verified empty window only for the matching active issuer."""
    cik = str(details.get("cik", ""))
    if (
        ticker == "SVA"
        and details.get("active") is True
        and cik.isdecimal() and int(cik) == 1084201
        and start >= date(2019, 2, 22)
    ):
        return "Trading halted since 2019-02-22; no prices in requested window; " + _SVA_HALT_SOURCE
    return None
