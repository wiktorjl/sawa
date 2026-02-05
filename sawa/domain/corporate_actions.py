"""Domain models for corporate actions (splits, dividends, earnings)."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


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
            ticker=data["ticker"],
            execution_date=date.fromisoformat(data["execution_date"]),
            split_from=data["split_from"],
            split_to=data["split_to"],
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
    frequency: int | None = None  # 0=one-time, 1=annual, 4=quarterly, 12=monthly

    @classmethod
    def from_polygon(cls, data: dict) -> "Dividend":
        """Create from Polygon API response."""
        return cls(
            ticker=data["ticker"],
            ex_dividend_date=date.fromisoformat(data["ex_dividend_date"]),
            record_date=(
                date.fromisoformat(data["record_date"]) if data.get("record_date") else None
            ),
            pay_date=date.fromisoformat(data["pay_date"]) if data.get("pay_date") else None,
            cash_amount=Decimal(str(data["cash_amount"])) if data.get("cash_amount") else None,
            declaration_date=(
                date.fromisoformat(data["declaration_date"])
                if data.get("declaration_date")
                else None
            ),
            dividend_type=data.get("dividend_type"),
            frequency=data.get("frequency"),
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
    revenue_estimate: int | None = None
    revenue_actual: int | None = None

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
            ticker=ticker,
            report_date=report_date,
            fiscal_quarter=attrs.get("fiscal_quarter"),
            fiscal_year=attrs.get("fiscal_year"),
            timing=attrs.get("timing"),
            eps_estimate=(
                Decimal(str(attrs["eps_estimate"])) if attrs.get("eps_estimate") else None
            ),
            eps_actual=Decimal(str(attrs["eps_actual"])) if attrs.get("eps_actual") else None,
            revenue_estimate=attrs.get("revenue_estimate"),
            revenue_actual=attrs.get("revenue_actual"),
        )

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insertion."""
        return (
            self.ticker,
            self.report_date,
            self.fiscal_quarter,
            self.fiscal_year,
            self.timing,
            self.eps_estimate,
            self.eps_actual,
            self.revenue_estimate,
            self.revenue_actual,
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
            "revenue_estimate",
            "revenue_actual",
        ]
