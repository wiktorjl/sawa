"""Stock character classification domain models.

Frozen dataclasses representing the three stages of stock character analysis:
classification, baseline profiling, and atypical behavior detection.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class CharacterClassification:
    """Stage 1: Character classification result for a ticker.

    Classifies a stock as range-bound, trending, or boom-bust based on
    Hurst exponent (DFA), ADX, regression R², and volatility-of-volatility.

    Attributes:
        ticker: Stock symbol (normalized to uppercase)
        run_date: Date the classification was computed
        character: One of 'range_bound', 'trending', 'boom_bust'
        confidence: 'HIGH' or 'MEDIUM'
        hurst_3yr: Hurst exponent over ~3 year window
        hurst_2yr: Hurst exponent over ~2 year window
        hurst_1yr: Hurst exponent over ~1 year window
        adx_avg: Average ADX over classification window
        regression_r2: R² of linear regression on log-prices
        vol_of_vol: Volatility of rolling volatility
        survivorship_flag: True if ticker existed for full window
    """

    ticker: str
    run_date: date
    character: str
    confidence: str
    hurst_3yr: Decimal | None = None
    hurst_2yr: Decimal | None = None
    hurst_1yr: Decimal | None = None
    adx_avg: Decimal | None = None
    regression_r2: Decimal | None = None
    vol_of_vol: Decimal | None = None
    survivorship_flag: bool = True

    def __post_init__(self) -> None:
        """Normalize ticker to uppercase."""
        object.__setattr__(self, "ticker", self.ticker.upper())

    @classmethod
    def column_names(cls) -> list[str]:
        """Return list of column names for database operations."""
        return [
            "ticker",
            "run_date",
            "character",
            "confidence",
            "hurst_3yr",
            "hurst_2yr",
            "hurst_1yr",
            "adx_avg",
            "regression_r2",
            "vol_of_vol",
            "survivorship_flag",
        ]

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insert."""
        return (
            self.ticker,
            self.run_date,
            self.character,
            self.confidence,
            self.hurst_3yr,
            self.hurst_2yr,
            self.hurst_1yr,
            self.adx_avg,
            self.regression_r2,
            self.vol_of_vol,
            self.survivorship_flag,
        )


@dataclass(frozen=True, slots=True)
class CharacterBaseline:
    """Stage 2: Baseline profile for a classified stock.

    Contains character-specific baseline metrics used to detect atypical
    behavior. Range-bound stocks get range/volume-profile fields, trending
    stocks get regression/SMA fields, and all stocks get common fields.

    Attributes:
        ticker: Stock symbol (normalized to uppercase)
        run_date: Date the baseline was computed
        character: Character type from classification stage
        atr_baseline: Average True Range baseline value
        atr_pct_baseline: ATR as percentage of price
        spy_corr_90d_mean: Mean 90-day rolling correlation with SPY
        spy_corr_90d_std: Std dev of 90-day rolling correlation with SPY
        gld_corr_90d_mean: Mean 90-day rolling correlation with GLD
        tlt_corr_90d_mean: Mean 90-day rolling correlation with TLT
        volume_sma20: 20-day volume simple moving average
        range_high: Upper bound of trading range (range-bound)
        range_low: Lower bound of trading range (range-bound)
        range_midpoint: Midpoint of trading range (range-bound)
        hvn_levels: High Volume Node price levels (range-bound)
        lvn_levels: Low Volume Node price levels (range-bound)
        typical_cycle_days: Average range cycle length in days (range-bound)
        volume_profile_source: Source method for volume profile data
        regression_slope: Log-price regression slope (trending)
        regression_intercept: Log-price regression intercept (trending)
        regression_r2: R² of regression fit (trending)
        residuals_std: Standard deviation of regression residuals (trending)
        residuals_2std: Two standard deviations of residuals (trending)
        expected_price_today: Expected price from regression (trending)
        sma_150_adherence_ratio: Touch-and-bounce ratio for 150-day SMA
        sma_200_adherence_ratio: Touch-and-bounce ratio for 200-day SMA
    """

    ticker: str
    run_date: date
    character: str

    # Common
    atr_baseline: Decimal | None = None
    atr_pct_baseline: Decimal | None = None
    spy_corr_90d_mean: Decimal | None = None
    spy_corr_90d_std: Decimal | None = None
    gld_corr_90d_mean: Decimal | None = None
    tlt_corr_90d_mean: Decimal | None = None
    volume_sma20: int | None = None

    # Range-bound
    range_high: Decimal | None = None
    range_low: Decimal | None = None
    range_midpoint: Decimal | None = None
    hvn_levels: tuple[Decimal, ...] | None = None
    lvn_levels: tuple[Decimal, ...] | None = None
    typical_cycle_days: Decimal | None = None
    volume_profile_source: str = "daily_approximation"

    # Trending
    regression_slope: Decimal | None = None
    regression_intercept: Decimal | None = None
    regression_r2: Decimal | None = None
    residuals_std: Decimal | None = None
    residuals_2std: Decimal | None = None
    expected_price_today: Decimal | None = None

    # SMA adherence
    sma_150_adherence_ratio: Decimal | None = None
    sma_200_adherence_ratio: Decimal | None = None

    def __post_init__(self) -> None:
        """Normalize ticker to uppercase."""
        object.__setattr__(self, "ticker", self.ticker.upper())

    @classmethod
    def column_names(cls) -> list[str]:
        """Return list of column names for database operations."""
        return [
            "ticker",
            "run_date",
            "character",
            # Common
            "atr_baseline",
            "atr_pct_baseline",
            "spy_corr_90d_mean",
            "spy_corr_90d_std",
            "gld_corr_90d_mean",
            "tlt_corr_90d_mean",
            "volume_sma20",
            # Range-bound
            "range_high",
            "range_low",
            "range_midpoint",
            "hvn_levels",
            "lvn_levels",
            "typical_cycle_days",
            "volume_profile_source",
            # Trending
            "regression_slope",
            "regression_intercept",
            "regression_r2",
            "residuals_std",
            "residuals_2std",
            "expected_price_today",
            # SMA adherence
            "sma_150_adherence_ratio",
            "sma_200_adherence_ratio",
        ]

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insert.

        Converts tuple fields to lists for PostgreSQL array compatibility.
        """
        return (
            self.ticker,
            self.run_date,
            self.character,
            # Common
            self.atr_baseline,
            self.atr_pct_baseline,
            self.spy_corr_90d_mean,
            self.spy_corr_90d_std,
            self.gld_corr_90d_mean,
            self.tlt_corr_90d_mean,
            self.volume_sma20,
            # Range-bound
            self.range_high,
            self.range_low,
            self.range_midpoint,
            list(self.hvn_levels) if self.hvn_levels is not None else None,
            list(self.lvn_levels) if self.lvn_levels is not None else None,
            self.typical_cycle_days,
            self.volume_profile_source,
            # Trending
            self.regression_slope,
            self.regression_intercept,
            self.regression_r2,
            self.residuals_std,
            self.residuals_2std,
            self.expected_price_today,
            # SMA adherence
            self.sma_150_adherence_ratio,
            self.sma_200_adherence_ratio,
        )


@dataclass(frozen=True, slots=True)
class CharacterFlag:
    """Stage 3: Individual atypical behavior flag.

    Represents a single detected atypical condition for a stock, such as
    volatility compression, decorrelation, or proximity to key levels.

    Attributes:
        ticker: Stock symbol (normalized to uppercase)
        run_date: Date the flag was detected
        flag: Flag identifier (e.g., 'vol_compression', 'decorrelation')
        value: Observed value that triggered the flag
        threshold: Threshold that was breached
    """

    ticker: str
    run_date: date
    flag: str
    value: Decimal | None = None
    threshold: Decimal | None = None

    def __post_init__(self) -> None:
        """Normalize ticker to uppercase."""
        object.__setattr__(self, "ticker", self.ticker.upper())

    @classmethod
    def column_names(cls) -> list[str]:
        """Return list of column names for database operations."""
        return [
            "ticker",
            "run_date",
            "flag",
            "value",
            "threshold",
        ]

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insert."""
        return (
            self.ticker,
            self.run_date,
            self.flag,
            self.value,
            self.threshold,
        )


@dataclass(frozen=True, slots=True)
class CharacterScorecard:
    """Combined scorecard summarizing a stock's character and current state.

    Merges classification, baseline, and flag data into a single view
    for screening and alerting.

    Attributes:
        ticker: Stock symbol (normalized to uppercase)
        run_date: Date the scorecard was generated
        character: Character type from classification
        confidence: Classification confidence level
        current_price: Most recent closing price
        price_percentile: Current price percentile within baseline range
        sigma_distance: Distance from regression in sigma units (trending)
        flag_count: Number of active atypical flags
        flags: Tuple of active flag identifiers
        atr_ratio: Recent ATR / baseline ATR
        spy_corr_recent: Recent SPY correlation
        spy_corr_baseline: Baseline SPY correlation mean
        at_hvn: True if price is near a High Volume Node
        in_lvn: True if price is in a Low Volume Node zone
        notes: Optional human-readable notes
    """

    ticker: str
    run_date: date
    character: str
    confidence: str
    current_price: Decimal | None = None
    price_percentile: Decimal | None = None
    sigma_distance: Decimal | None = None
    flag_count: int = 0
    flags: tuple[str, ...] = ()
    atr_ratio: Decimal | None = None
    spy_corr_recent: Decimal | None = None
    spy_corr_baseline: Decimal | None = None
    at_hvn: bool = False
    in_lvn: bool = False
    notes: str | None = None

    def __post_init__(self) -> None:
        """Normalize ticker to uppercase."""
        object.__setattr__(self, "ticker", self.ticker.upper())

    @classmethod
    def column_names(cls) -> list[str]:
        """Return list of column names for database operations."""
        return [
            "ticker",
            "run_date",
            "character",
            "confidence",
            "current_price",
            "price_percentile",
            "sigma_distance",
            "flag_count",
            "flags",
            "atr_ratio",
            "spy_corr_recent",
            "spy_corr_baseline",
            "at_hvn",
            "in_lvn",
            "notes",
        ]

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insert.

        Converts tuple fields to lists for PostgreSQL array compatibility.
        """
        return (
            self.ticker,
            self.run_date,
            self.character,
            self.confidence,
            self.current_price,
            self.price_percentile,
            self.sigma_distance,
            self.flag_count,
            list(self.flags) if self.flags else [],
            self.atr_ratio,
            self.spy_corr_recent,
            self.spy_corr_baseline,
            self.at_hvn,
            self.in_lvn,
            self.notes,
        )
