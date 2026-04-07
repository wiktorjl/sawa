# Stock Character Classification System — Build Spec

## Overview

A weekly batch pipeline that classifies stocks by their behavioral character, builds a baseline of "normal" behavior per stock, and flags names exhibiting atypical behavior in the recent window. The output is a ranked alert list usable by any trading style.

The core insight: most stocks don't have clean enough character to classify. The universe filter is the most important gate — noisy names self-select out. A clean list of 30-100 well-characterized stocks is more valuable than 5000 weak signals.

---

## Data

- **Source:** Internal datastore, 5000 top stocks, daily OHLCV
- **History:** ~3 years (~756 trading days)
- **Format:** Assume pandas DataFrame per ticker: columns `[date, open, high, low, close, volume]`
- **Batch cadence:** Weekly, run after Friday close
- **Parallelism:** Each stock is independent — embarrassingly parallel across all stages

### Known Limitations

- **Survivorship bias:** Universe is today's top 5000, not historical constituents. Range boundaries and trend classifications may reflect survivorship, not true character. Flag in output metadata.
- **Volume profile approximation:** Daily OHLCV only — no intraday data. HVN/LVN approximated from daily volume-at-price distribution. Mark as approximate in output.

---

## Architecture

Four sequential stages. Each stage is parallelizable across stocks.

```
Stage 1: Universe Filter + Character Classification
         → Input: full 3yr OHLCV per ticker
         → Output: {ticker: character_class, confidence, metadata}

Stage 2: Baseline Statistics
         → Input: classified tickers + OHLCV
         → Output: {ticker: baseline_stats dict}

Stage 3: Atypical Behavior Detection
         → Input: baseline_stats + recent 10d OHLCV window
         → Output: {ticker: [flags]}

Stage 4: Scorecard + Ranked Alert List
         → Input: all flags
         → Output: ranked CSV/JSON alert list
```

---

## Stage 1: Character Classification

### Primary Signal: Hurst Exponent via DFA

Use **Detrended Fluctuation Analysis (DFA)** — more robust than R/S analysis on financial return series.

Compute on **three overlapping windows** of the return series:
- Full 3yr (~756 days)
- Most recent 2yr (~504 days)
- Most recent 1yr (~252 days)

**Require consistency across all three windows before accepting classification.** A single Hurst reading is unreliable. Inconsistency across windows = boom-bust or unclassifiable.

```python
# Hurst thresholds
H < 0.45   → mean-reverting (range-bound candidate)
H > 0.55   → trending candidate
0.45–0.55  → random walk (likely unclassifiable)
```

### Secondary Confirmation per Class

| Class | Hurst Requirement | Secondary Confirmation |
|-------|-------------------|------------------------|
| Range-bound | < 0.45 all three windows | ADX(20) average < 25 over full history |
| Trending | > 0.55 all three windows | R² of OLS price regression > 0.70 |
| Boom-bust | Inconsistent across windows | High vol-of-vol: std(rolling_30d_vol) / mean(rolling_30d_vol) > 0.5 |
| Unclassifiable | Anything else | Drop from output entirely |

### Confidence Scoring

Assign confidence based on how clearly the stock passes its thresholds:

- **HIGH:** All three Hurst windows agree, secondary confirmation strong
- **MEDIUM:** All three windows agree, secondary confirmation marginal
- **LOW:** Drop — do not include in output

Only HIGH and MEDIUM confidence stocks proceed to Stage 2.

### Expected Output

Expect ~60-70% of 5000 names to be dropped as unclassifiable. This is correct behavior, not a bug.

```python
classification_result = {
    "ticker": "XYZ",
    "character": "range_bound",  # range_bound | trending | boom_bust
    "confidence": "HIGH",        # HIGH | MEDIUM
    "hurst_3yr": 0.38,
    "hurst_2yr": 0.41,
    "hurst_1yr": 0.39,
    "adx_avg": 18.2,             # range_bound only
    "regression_r2": None,       # trending only
    "vol_of_vol": None,          # boom_bust only
}
```

---

## Stage 2: Baseline Statistics

Compute per classified stock over the **full available history** (3yr). These stats define what "normal" looks like for this name.

### All Stocks

```python
baseline = {
    "atr_baseline": float,           # mean(ATR(14)) over full history
    "atr_pct_baseline": float,       # mean(ATR/close) — normalized for price
    "spy_corr_90d_rolling_mean": float,   # mean of rolling 90d correlation to SPY
    "spy_corr_90d_rolling_std": float,    # std of rolling 90d correlation to SPY
    "gld_corr_90d_rolling_mean": float,
    "tlt_corr_90d_rolling_mean": float,
    "volume_sma20": float,           # baseline average volume
}
```

### Range-bound Stocks (additional)

```python
range_baseline = {
    "range_high": float,             # rolling 3yr percentile 98th of close
    "range_low": float,              # rolling 3yr percentile 2nd of close
    "range_midpoint": float,
    "price_percentile_fn": callable, # function(price) → percentile in 3yr range
    "hvn_levels": list[float],       # approximate HVN prices (see below)
    "lvn_levels": list[float],       # approximate LVN prices (see below)
    "typical_cycle_days": float,     # optional: avg days between range extremes
}
```

### Trending Stocks (additional)

```python
trend_baseline = {
    "regression_slope": float,       # OLS slope (price per day)
    "regression_intercept": float,
    "regression_r2": float,
    "residuals_std": float,          # 1-sigma band around regression line
    "residuals_2std": float,         # 2-sigma band
    "expected_price_today": float,   # projection to current date
}
```

### Volume Profile Approximation (daily OHLCV)

Approximate HVN/LVN from daily volume distribution across price buckets:

1. Bin the 3yr price range into N buckets (suggest N=50)
2. For each day, assign volume to the bucket containing that day's typical price: `(high + low + close) / 3`
3. Normalize bucket volumes
4. HVN = buckets in top 30th percentile of volume
5. LVN = buckets in bottom 20th percentile of volume

Flag output as `volume_profile_source: "daily_approximation"`.

---

## Stage 3: Atypical Behavior Detection

**Recent window:** Last 10 trading days (configurable, suggest 10-15). Compute the following flags per stock.

### Flags: Range-bound Stocks

| Flag | Definition |
|------|-----------|
| `EXTREMUM_HIGH` | Current price percentile > 90th of 3yr range |
| `EXTREMUM_LOW` | Current price percentile < 10th of 3yr range |
| `COMPRESSION` | recent_ATR_10d / atr_baseline < 0.6 (volatility contraction) |
| `EXPANSION` | recent_ATR_10d / atr_baseline > 1.8 (volatility expansion — possible breakout) |
| `AT_HVN` | Current price within 1% of any identified HVN level |
| `IN_LVN` | Current price within an identified LVN zone (air pocket — fast move risk) |
| `DECORRELATION_SPY` | 10d rolling SPY correlation deviates from 90d baseline by > 2 stddev |
| `VOLUME_SPIKE` | 10d avg volume / volume_sma20 > 2.0 |
| `VOLUME_DROUGHT` | 10d avg volume / volume_sma20 < 0.5 |

### Flags: Trending Stocks

| Flag | Definition |
|------|-----------|
| `ABOVE_2SIGMA` | Current price > regression line + 2 × residuals_std |
| `BELOW_2SIGMA` | Current price < regression line - 2 × residuals_std |
| `SLOPE_BREAK` | 10d linear regression slope sign differs from historical slope |
| `VOL_SPIKE` | recent_ATR_10d / atr_baseline > 1.8 |
| `DECORRELATION_SPY` | Same as range-bound definition |
| `VOLUME_SPIKE` | Same as range-bound definition |

### SMA Adherence Check (all classified stocks)

If the stock has historically adhered to 150/200 SMA (backtest touch-and-bounce ratio > 60%):

| Flag | Definition |
|------|-----------|
| `AT_200SMA` | Current price within 1.5% of 200-day SMA |
| `AT_150SMA` | Current price within 1.5% of 150-day SMA |
| `BELOW_200SMA` | Price crossed below 200 SMA in last 10 days (for a historically above-SMA trending stock) |

---

## Stage 4: Output

### Scorecard per Stock

```python
scorecard = {
    "ticker": "XYZ",
    "character": "range_bound",
    "confidence": "HIGH",
    "price_percentile": 93.4,         # where in 3yr range
    "current_price": 184.20,
    "flags": ["EXTREMUM_HIGH", "COMPRESSION", "VOLUME_DROUGHT"],
    "flag_count": 3,
    "atr_ratio": 0.54,                # recent ATR / baseline ATR
    "spy_corr_recent": 0.71,
    "spy_corr_baseline": 0.82,
    "at_hvn": False,
    "in_lvn": False,
    "notes": "survivorship_bias_flag"  # if applicable
}
```

### Ranked Alert List

Sort by `flag_count` descending. Secondary sort by `confidence` (HIGH before MEDIUM).

Output as both CSV and JSON.

```
TICKER | CLASS      | CONF | PRICE_PCT | FLAGS                          | SCORE
XYZ    | range_bound| HIGH | 93rd      | EXTREMUM_HIGH,COMPRESSION      | 2
ABC    | range_bound| HIGH | 8th       | EXTREMUM_LOW,AT_HVN,VOL_SPIKE  | 3
DEF    | trending   | MED  | +2.3σ     | ABOVE_2SIGMA,VOL_SPIKE         | 2
```

### Interpretation Guide (embed in output docs)

| Flag Combination | Interpretation |
|-----------------|----------------|
| EXTREMUM + COMPRESSION | Range stock coiling at extreme — high probability setup regardless of style |
| EXTREMUM + VOLUME_SPIKE | Range stock at extreme with unusual activity — investigate catalyst |
| EXTREMUM + DECORRELATION | Macro driver detached — stock-specific story driving the move |
| ABOVE_2SIGMA + VOL_SPIKE | Trending stock overextended — mean reversion risk or breakout depending on context |
| IN_LVN | Price in air pocket — fast move likely, direction TBD |
| AT_HVN + COMPRESSION | Consolidating at major volume node — directional resolution pending |

---

## Implementation Notes

### Performance

- Parallelize Stages 1-3 across tickers using `multiprocessing.Pool` or `concurrent.futures.ProcessPoolExecutor`
- Cache baseline stats (Stage 2) — only recompute when new data extends beyond existing baseline window
- Stage 1 (Hurst via DFA) is the most compute-intensive step. Pre-filter obvious randoms first using a fast ADX screen before running DFA

### Libraries

```
numpy, pandas          — core data
statsmodels            — OLS regression
hurst                  — Hurst exponent (or implement DFA directly)
ta or pandas_ta        — ATR, ADX indicators
scipy                  — stats, percentile functions
```

### File Structure

```
stock_classifier/
├── main.py                  # Weekly batch entry point
├── config.py                # Thresholds, window lengths, parameters
├── stage1_classify.py       # Hurst + secondary confirmation
├── stage2_baseline.py       # Baseline stat computation
├── stage3_detect.py         # Flag detection
├── stage4_output.py         # Scorecard + ranking
├── utils/
│   ├── hurst.py             # DFA implementation
│   ├── volume_profile.py    # Daily OHLCV HVN/LVN approximation
│   └── indicators.py        # ATR, ADX, SMA helpers
└── output/
    ├── alerts_YYYYMMDD.csv
    └── alerts_YYYYMMDD.json
```

### Config Parameters (all tunable)

```python
HURST_RANGE_THRESHOLD = 0.45       # H below this = range-bound
HURST_TREND_THRESHOLD = 0.55       # H above this = trending
ADX_RANGE_MAX = 25                 # ADX below this confirms range
REGRESSION_R2_MIN = 0.70           # R² above this confirms trend
VOL_OF_VOL_THRESHOLD = 0.50        # boom-bust identifier

RECENT_WINDOW_DAYS = 10            # lookback for atypical detection
EXTREMUM_HIGH_PCT = 90             # percentile threshold for high extreme
EXTREMUM_LOW_PCT = 10              # percentile threshold for low extreme
COMPRESSION_RATIO = 0.60           # recent/baseline ATR below this = compression
EXPANSION_RATIO = 1.80             # recent/baseline ATR above this = expansion
DECORRELATION_STDDEV = 2.0         # stddev threshold for correlation break
VOLUME_SPIKE_RATIO = 2.0           # volume spike multiplier
VOLUME_DROUGHT_RATIO = 0.50        # volume drought multiplier
HVN_VOLUME_PERCENTILE = 70         # above this = HVN
LVN_VOLUME_PERCENTILE = 20         # below this = LVN
SMA_PROXIMITY_PCT = 0.015          # within 1.5% of SMA = "at SMA"
VOLUME_PROFILE_BUCKETS = 50        # price buckets for profile approximation
```

---

## Validation

Before running on full 5000-stock universe, validate on a small set of known-character names:

| Ticker | Expected Class | Notes |
|--------|---------------|-------|
| AAPL   | trending (recently) | Was range-bound pre-2023 — good regime-change test |
| KO     | range_bound   | Classic mean-reverter |
| NVDA   | trending      | Strong persistent trend |
| GME    | boom_bust or unclassifiable | Extreme vol-of-vol |
| XOM    | range_bound or trending | Macro-correlated, test SPY/GLD correlation flags |

If classification results don't match expectations on known names, debug Stage 1 thresholds before scaling.

---

## Future Upgrades

- **Intraday volume profile:** Replace daily approximation with tick/minute data for accurate HVN/LVN
- **Earnings character:** Flag stocks with high post-earnings drift vs. high gap-and-fill tendency — separate behavioral dimension
- **Regime-conditional baselines:** Maintain separate baselines for bull/bear regimes; flag when current regime differs from the one that built the baseline
- **Gap character:** Classify gap-and-go vs. gap-and-fill behavior as an additional dimension
- **Correlation basket:** Expand beyond SPY/GLD/TLT to sector ETFs for more precise correlation attribution
