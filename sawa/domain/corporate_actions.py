"""Domain models for corporate actions (splits, dividends, earnings)."""

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from sawa.utils.symbols import validate_ticker

_MAX_INTEGER = 2_147_483_647
_MAX_BIGINT = 9_223_372_036_854_775_807
_MAX_NUMERIC_10_4 = Decimal("1000000")
_NUMERIC_10_4_SCALE = Decimal("0.0001")
# 24 = semi-monthly and 52 = weekly are ordinary distribution schedules for
# income ETFs, not provider corruption. The set stays an allowlist so a
# genuinely bogus frequency is still rejected.
_DIVIDEND_FREQUENCIES = {0, 1, 2, 4, 12, 24, 52}


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if (
        not parsed.is_finite()
        or parsed != parsed.to_integral_value()
        or parsed <= 0
        or parsed > _MAX_INTEGER
    ):
        raise ValueError(f"{field} must be a positive integer")
    return int(parsed)


def is_unrepresentable_split_ratio(data: object) -> bool:
    """Whether a provider split record carries a non-integer share ratio.

    Polygon reports mutual-fund reorganizations through the splits endpoint
    with fractional ratios (NSNRX 1:0.9668, NIPMY 1:1.5). ``stock_splits``
    stores integer share counts, so such a record is unrepresentable rather
    than malformed, and one of them must not fail the batch that also carries
    real equity splits. Anything else — a missing, non-numeric, zero, negative,
    or out-of-range ratio — stays malformed and is left to the strict parser.
    """
    if not isinstance(data, dict):
        return False
    for field in ("split_from", "split_to"):
        value = data.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            return False
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return False
        if not parsed.is_finite() or parsed <= 0 or parsed > _MAX_INTEGER:
            return False
    return any(
        Decimal(str(data[field])) != Decimal(str(data[field])).to_integral_value()
        for field in ("split_from", "split_to")
    )


def _optional_numeric_10_4(
    value: object,
    field: str,
    *,
    positive: bool = False,
) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite numeric value")
    try:
        parsed = Decimal(str(value))
        rounded = parsed.quantize(_NUMERIC_10_4_SCALE, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite numeric value") from exc
    if (
        not parsed.is_finite()
        or abs(rounded) >= _MAX_NUMERIC_10_4
        or (positive and rounded <= 0)
    ):
        qualifier = "positive " if positive else ""
        raise ValueError(f"{field} must be a {qualifier}finite NUMERIC(10,4) value")
    return rounded


def _optional_bigint(value: object, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if (
        not parsed.is_finite()
        or parsed != parsed.to_integral_value()
        or abs(parsed) > _MAX_BIGINT
    ):
        raise ValueError(f"{field} must fit a signed BIGINT")
    return int(parsed)


@dataclass
class StockSplit:
    """Stock split record."""

    ticker: str
    execution_date: date
    split_from: int
    split_to: int

    @property
    def ratio(self) -> str:
        """Return split ratio as string (e.g., '4:1')."""
        return f"{self.split_to}:{self.split_from}"

    @property
    def multiplier(self) -> float:
        """Return the price adjustment multiplier."""
        return self.split_to / self.split_from

    @classmethod
    def from_polygon(cls, data: dict) -> "StockSplit":
        """Create from Polygon API response."""
        return cls(
            ticker=validate_ticker(str(data["ticker"])),
            execution_date=date.fromisoformat(data["execution_date"]),
            split_from=_positive_integer(data["split_from"], "split_from"),
            split_to=_positive_integer(data["split_to"], "split_to"),
        )

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insertion."""
        return (
            self.ticker,
            self.execution_date,
            self.split_from,
            self.split_to,
        )

    @staticmethod
    def columns() -> list[str]:
        """Return column names for database insertion."""
        return ["ticker", "execution_date", "split_from", "split_to"]


@dataclass
class Dividend:
    """Dividend record."""

    ticker: str
    ex_dividend_date: date
    record_date: date | None = None
    pay_date: date | None = None
    cash_amount: Decimal | None = None
    declaration_date: date | None = None
    dividend_type: str | None = None  # CD, SC, LT, ST
    # 0=one-time, 1=annual, 2=semi-annual, 4=quarterly, 12=monthly,
    # 24=semi-monthly, 52=weekly
    frequency: int | None = None

    @classmethod
    def from_polygon(cls, data: dict) -> "Dividend":
        """Create from Polygon API response."""
        frequency = data.get("frequency")
        if isinstance(frequency, bool) or (
            frequency is not None
            and (not isinstance(frequency, int) or frequency not in _DIVIDEND_FREQUENCIES)
        ):
            raise ValueError(
                "frequency must be one of "
                + ", ".join(str(f) for f in sorted(_DIVIDEND_FREQUENCIES))
                + " when provided"
            )
        return cls(
            ticker=validate_ticker(str(data["ticker"])),
            ex_dividend_date=date.fromisoformat(data["ex_dividend_date"]),
            record_date=(
                date.fromisoformat(data["record_date"]) if data.get("record_date") else None
            ),
            pay_date=date.fromisoformat(data["pay_date"]) if data.get("pay_date") else None,
            cash_amount=_optional_numeric_10_4(
                data.get("cash_amount"),
                "cash_amount",
                positive=True,
            ),
            declaration_date=(
                date.fromisoformat(data["declaration_date"])
                if data.get("declaration_date")
                else None
            ),
            dividend_type=data.get("dividend_type"),
            frequency=frequency,
        )

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insertion."""
        return (
            self.ticker,
            self.ex_dividend_date,
            self.record_date,
            self.pay_date,
            self.cash_amount,
            self.declaration_date,
            self.dividend_type,
            self.frequency,
        )

    @staticmethod
    def columns() -> list[str]:
        """Return column names for database insertion."""
        return [
            "ticker",
            "ex_dividend_date",
            "record_date",
            "pay_date",
            "cash_amount",
            "declaration_date",
            "dividend_type",
            "frequency",
        ]


@dataclass
class Earnings:
    """Earnings report record."""

    ticker: str
    report_date: date | None = None
    fiscal_quarter: str | None = None  # Q1, Q2, Q3, Q4
    fiscal_year: int | None = None
    timing: str | None = None  # BMO, AMC, DMH
    eps_estimate: Decimal | None = None
    eps_actual: Decimal | None = None
    revenue_actual: int | None = None
    surprise_pct: Decimal | None = None

    @property
    def eps_surprise(self) -> Decimal | None:
        """Calculate EPS surprise (actual - estimate)."""
        if self.eps_actual is not None and self.eps_estimate is not None:
            return self.eps_actual - self.eps_estimate
        return None

    @property
    def eps_surprise_pct(self) -> Decimal | None:
        """Calculate EPS surprise percentage."""
        if self.eps_actual is not None and self.eps_estimate is not None and self.eps_estimate != 0:
            return (self.eps_actual - self.eps_estimate) / abs(self.eps_estimate) * 100
        return None

    @classmethod
    def from_polygon_event(cls, ticker: str, event: dict) -> "Earnings | None":
        """Create from Polygon ticker events API response."""
        if event.get("type") != "earnings":
            return None

        attrs = event.get("attributes", {})
        event_date = event.get("date")
        report_date: date | None = date.fromisoformat(event_date) if event_date else None

        return cls(
            ticker=validate_ticker(ticker),
            report_date=report_date,
            fiscal_quarter=attrs.get("fiscal_quarter"),
            fiscal_year=_optional_bigint(attrs.get("fiscal_year"), "fiscal_year"),
            timing=attrs.get("timing"),
            eps_estimate=_optional_numeric_10_4(
                attrs.get("eps_estimate"), "eps_estimate"
            ),
            eps_actual=_optional_numeric_10_4(
                attrs.get("eps_actual"), "eps_actual"
            ),
            revenue_actual=_optional_bigint(
                attrs.get("revenue_actual"), "revenue_actual"
            ),
        )

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insertion.

        Column order matches the migrated earnings schema (migration 19/20:
        revenue_estimate dropped, surprise_pct added).
        """
        return (
            self.ticker,
            self.report_date,
            self.fiscal_quarter,
            self.fiscal_year,
            self.timing,
            self.eps_estimate,
            self.eps_actual,
            self.revenue_actual,
            self.surprise_pct if self.surprise_pct is not None else self.eps_surprise_pct,
        )

    @staticmethod
    def columns() -> list[str]:
        """Return column names for database insertion."""
        return [
            "ticker",
            "report_date",
            "fiscal_quarter",
            "fiscal_year",
            "timing",
            "eps_estimate",
            "eps_actual",
            "revenue_actual",
            "surprise_pct",
        ]
