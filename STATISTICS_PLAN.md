# Technical Indicators Storage Implementation Plan

## Overview
Design for storing 120+ technical analysis (TA) indicators calculated daily for all S&P 500 stocks using ta-lib. Optimized for screening (cross-category queries) with single wide table design.

## Design Decisions

### Table Design: Single Wide Table
- **Rationale**: Screening (B) and complex filters (D) are primary use cases requiring cross-category queries without joins
- **Storage**: ~126MB/year (500 stocks × 252 days × 120 indicators × 8 bytes)
- **Performance**: Single table enables efficient multi-indicator WHERE clauses

### Core Principles
1. **Allow NULLs for insufficient data** - First ~50 trading days will have NULLs for indicators requiring longer lookback (e.g., SMA-50)
2. **Batch calculation** - nightly pipeline after price updates
3. **Recalculate entire history** - when adding new indicators
4. **Parallel processing** - multiprocessing per ticker
5. **Validation with hard limits** - reject values significantly outside bounds (>1%), clamp minor floating-point errors (<1%)
6. **Use Decimal for storage** - consistency with prices (NUMERIC in database), float64 for calculation

## Phase 1: Database Schema

### New File: `sqlschema/11_technical_indicators.sql`

```sql
-- Technical indicators table (initial 20, expandable to 120+)
CREATE TABLE technical_indicators (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    date DATE NOT NULL,
    
    -- Trend (8 indicators) - NULL when insufficient data
    sma_5 NUMERIC(12, 4),
    sma_10 NUMERIC(12, 4),
    sma_20 NUMERIC(12, 4),
    sma_50 NUMERIC(12, 4),
    ema_12 NUMERIC(12, 4),
    ema_26 NUMERIC(12, 4),
    ema_50 NUMERIC(12, 4),
    vwap NUMERIC(12, 4),
    
    -- Momentum (5 indicators)
    rsi_14 NUMERIC(10, 6),
    rsi_21 NUMERIC(10, 6),
    macd_line NUMERIC(12, 4),
    macd_signal NUMERIC(12, 4),
    macd_histogram NUMERIC(12, 4),
    
    -- Volatility (4 indicators)
    bb_upper NUMERIC(12, 4),
    bb_middle NUMERIC(12, 4),
    bb_lower NUMERIC(12, 4),
    atr_14 NUMERIC(12, 4),
    
    -- Volume (3 indicators)
    -- BIGINT safe: 10B << 9.2 quintillion limit. Assumes post-split volume reporting.
    obv BIGINT,
    volume_sma_20 BIGINT,
    volume_ratio NUMERIC(10, 6),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

-- Basic indexes
CREATE INDEX idx_ta_date ON technical_indicators(date);
CREATE INDEX idx_ta_ticker_date_desc ON technical_indicators(ticker, date DESC);

-- BRIN indexes for screening queries (optimal for time-series range scans)
CREATE INDEX idx_ta_rsi_14 ON technical_indicators USING BRIN (rsi_14);
CREATE INDEX idx_ta_rsi_21 ON technical_indicators USING BRIN (rsi_21);
CREATE INDEX idx_ta_atr_14 ON technical_indicators USING BRIN (atr_14);
CREATE INDEX idx_ta_macd_line ON technical_indicators USING BRIN (macd_line);
CREATE INDEX idx_ta_volume_ratio ON technical_indicators USING BRIN (volume_ratio);

-- Metadata registry for dynamic queries
CREATE TABLE technical_indicator_metadata (
    indicator_name VARCHAR(50) PRIMARY KEY,
    column_name VARCHAR(50) NOT NULL,
    category VARCHAR(30) NOT NULL,  -- 'trend', 'momentum', 'volatility', 'volume'
    description TEXT,
    ta_lib_function VARCHAR(50),
    params JSONB,
    validation_min NUMERIC(10, 6),
    validation_max NUMERIC(10, 6),
    is_bounded BOOLEAN DEFAULT FALSE,  -- TRUE for bounded indicators (RSI 0-100)
    min_periods_required INTEGER,       -- Minimum data points needed
    display_name VARCHAR(100),          -- Human-readable name for UI
    unit VARCHAR(20),                   -- 'percent', 'dollars', 'ratio', 'count'
    sort_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate metadata for initial 20 indicators
INSERT INTO technical_indicator_metadata 
    (indicator_name, column_name, category, description, validation_min, validation_max, 
     is_bounded, min_periods_required, display_name, unit, sort_order)
VALUES
    ('sma_5', 'sma_5', 'trend', '5-day Simple Moving Average', NULL, NULL, 
     FALSE, 5, '5-Day SMA', 'dollars', 1),
    ('sma_10', 'sma_10', 'trend', '10-day Simple Moving Average', NULL, NULL, 
     FALSE, 10, '10-Day SMA', 'dollars', 2),
    ('sma_20', 'sma_20', 'trend', '20-day Simple Moving Average', NULL, NULL, 
     FALSE, 20, '20-Day SMA', 'dollars', 3),
    ('sma_50', 'sma_50', 'trend', '50-day Simple Moving Average', NULL, NULL, 
     FALSE, 50, '50-Day SMA', 'dollars', 4),
    ('ema_12', 'ema_12', 'trend', '12-day Exponential Moving Average', NULL, NULL, 
     FALSE, 12, '12-Day EMA', 'dollars', 5),
    ('ema_26', 'ema_26', 'trend', '26-day Exponential Moving Average', NULL, NULL, 
     FALSE, 26, '26-Day EMA', 'dollars', 6),
    ('ema_50', 'ema_50', 'trend', '50-day Exponential Moving Average', NULL, NULL, 
     FALSE, 50, '50-Day EMA', 'dollars', 7),
    ('vwap', 'vwap', 'trend', 'Volume Weighted Average Price', NULL, NULL, 
     FALSE, 1, 'VWAP', 'dollars', 8),
    ('rsi_14', 'rsi_14', 'momentum', '14-day Relative Strength Index', 0, 100, 
     TRUE, 14, '14-Day RSI', 'percent', 9),
    ('rsi_21', 'rsi_21', 'momentum', '21-day Relative Strength Index', 0, 100, 
     TRUE, 21, '21-Day RSI', 'percent', 10),
    ('macd_line', 'macd_line', 'momentum', 'MACD Line', NULL, NULL, 
     FALSE, 26, 'MACD Line', 'dollars', 11),
    ('macd_signal', 'macd_signal', 'momentum', 'MACD Signal Line', NULL, NULL, 
     FALSE, 35, 'MACD Signal', 'dollars', 12),
    ('macd_histogram', 'macd_histogram', 'momentum', 'MACD Histogram', NULL, NULL, 
     FALSE, 35, 'MACD Histogram', 'dollars', 13),
    ('bb_upper', 'bb_upper', 'volatility', 'Bollinger Band Upper', NULL, NULL, 
     FALSE, 20, 'BB Upper', 'dollars', 14),
    ('bb_middle', 'bb_middle', 'volatility', 'Bollinger Band Middle', NULL, NULL, 
     FALSE, 20, 'BB Middle', 'dollars', 15),
    ('bb_lower', 'bb_lower', 'volatility', 'Bollinger Band Lower', NULL, NULL, 
     FALSE, 20, 'BB Lower', 'dollars', 16),
    ('atr_14', 'atr_14', 'volatility', '14-day Average True Range', 0, NULL, 
     FALSE, 14, '14-Day ATR', 'dollars', 17),
    ('obv', 'obv', 'volume', 'On Balance Volume', NULL, NULL, 
     FALSE, 1, 'OBV', 'count', 18),
    ('volume_sma_20', 'volume_sma_20', 'volume', '20-day Volume SMA', 0, NULL, 
     FALSE, 20, '20-Day Volume SMA', 'count', 19),
    ('volume_ratio', 'volume_ratio', 'volume', 'Volume Ratio (today/20-day avg)', 0, NULL, 
     FALSE, 20, 'Volume Ratio', 'ratio', 20);
```

## Phase 2: Domain Model

### New File: `sawa/domain/technical_indicators.py`

```python
"""Technical indicators domain model."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class TechnicalIndicators:
    """Technical indicators for a ticker on a date.
    
    All 20 core indicators calculated daily from OHLCV data using ta-lib.
    Optional types used - indicators are NULL when insufficient historical data.
    """
    
    ticker: str
    date: date
    
    # Trend (8 indicators)
    sma_5: Decimal | None
    sma_10: Decimal | None
    sma_20: Decimal | None
    sma_50: Decimal | None
    ema_12: Decimal | None
    ema_26: Decimal | None
    ema_50: Decimal | None
    vwap: Decimal | None
    
    # Momentum (5 indicators)
    rsi_14: Decimal | None
    rsi_21: Decimal | None
    macd_line: Decimal | None
    macd_signal: Decimal | None
    macd_histogram: Decimal | None
    
    # Volatility (4 indicators)
    bb_upper: Decimal | None
    bb_middle: Decimal | None
    bb_lower: Decimal | None
    atr_14: Decimal | None
    
    # Volume (3 indicators)
    obv: int | None
    volume_sma_20: int | None
    volume_ratio: Decimal | None
    
    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", self.ticker.upper())
```

## Phase 3: Repository Layer

### New File: `sawa/repositories/technical_indicators.py`

Interface and PostgreSQL implementation for:
- Bulk insert with validation
- Screening queries (multi-indicator filters)
- Single ticker retrieval
- Date range queries

```python
async def screen_by_indicators(
    self,
    filters: dict[str, tuple[float | None, float | None]],  # {"rsi_14": (0, 30)}
    target_date: date,
    limit: int = 100,
) -> list[TechnicalIndicators]:
    """Screen stocks by multiple indicator values.
    
    Args:
        filters: Dict mapping indicator name to (min, max) tuple.
                 Use None for unbounded side.
        target_date: Date to screen
        limit: Maximum results
    
    Returns:
        List of TechnicalIndicators matching all filters
    """
    conditions = []
    params = []
    
    for indicator, (min_val, max_val) in filters.items():
        if min_val is not None and max_val is not None:
            conditions.append(f"{indicator} BETWEEN %s AND %s")
            params.extend([min_val, max_val])
        elif min_val is not None:
            conditions.append(f"{indicator} >= %s")
            params.append(min_val)
        elif max_val is not None:
            conditions.append(f"{indicator} <= %s")
            params.append(max_val)
    
    query = f"""
        SELECT * FROM technical_indicators
        WHERE date = %s AND {' AND '.join(conditions)}
        LIMIT %s
    """
    params = [target_date] + params + [limit]
    # Execute and return TechnicalIndicators objects
```

## Phase 4: Calculation Engine

### New File: `sawa/calculation/ta_engine.py`

```python
"""Technical indicator calculation engine using ta-lib."""

import logging
import math
import time
from multiprocessing import Pool

import numpy as np


def calculate_required_lookback(indicator_periods: list[int]) -> int:
    """Calculate required lookback days dynamically.
    
    Args:
        indicator_periods: List of indicator periods (e.g., [5, 10, 20, 50])
    
    Returns:
        Required calendar days (trading days × 1.4 for weekends/holidays)
    """
    max_period = max(indicator_periods)
    return int(max_period * 1.4)  # e.g., 50 days → 70 calendar days


def validate_indicator(
    name: str,
    value: float,
    min_val: float | None,
    max_val: float | None,
    logger: logging.Logger,
) -> float | None:
    """Validate indicator value with hard limits and soft clamping.
    
    - NaN values return None (insufficient data)
    - Values >1% outside bounds raise ValueError (calculation bug)
    - Values <1% outside bounds are clamped with warning (floating-point error)
    
    Args:
        name: Indicator name for logging
        value: Calculated value
        min_val: Minimum valid value (or None for unbounded)
        max_val: Maximum valid value (or None for unbounded)
        logger: Logger instance
    
    Returns:
        Validated value, or None for NaN
    
    Raises:
        ValueError: If value is significantly outside valid bounds
    """
    # Handle NaN (insufficient data)
    if math.isnan(value):
        return None
    
    # Check minimum bound
    if min_val is not None and value < min_val:
        deviation = abs(value - min_val)
        tolerance = abs(min_val) * 0.01 if min_val != 0 else 0.01
        
        if deviation > tolerance:
            logger.error(f"{name} value {value} far below min {min_val}")
            raise ValueError(f"Invalid {name}: {value} (min: {min_val})")
        
        logger.warning(f"Clamping {name} from {value} to {min_val}")
        return min_val
    
    # Check maximum bound
    if max_val is not None and value > max_val:
        deviation = abs(value - max_val)
        tolerance = abs(max_val) * 0.01 if max_val != 0 else 0.01
        
        if deviation > tolerance:
            logger.error(f"{name} value {value} far above max {max_val}")
            raise ValueError(f"Invalid {name}: {value} (max: {max_val})")
        
        logger.warning(f"Clamping {name} from {value} to {max_val}")
        return max_val
    
    return value


def estimate_backfill_time(
    tickers: list[str],
    sample_size: int = 10,
) -> tuple[int, float, float]:
    """Estimate total backfill time based on sample run.
    
    Args:
        tickers: Full list of tickers to backfill
        sample_size: Number of tickers to test (default: 10)
    
    Returns:
        Tuple of (total_tickers, per_ticker_seconds, estimated_total_seconds)
    """
    test_tickers = tickers[:sample_size]
    start = time.time()
    
    for ticker in test_tickers:
        calculate_indicators_for_ticker(ticker)
    
    elapsed = time.time() - start
    per_ticker = elapsed / len(test_tickers)
    estimated_total = per_ticker * len(tickers)
    
    return len(tickers), per_ticker, estimated_total
```

**Calculation flow:**
1. Fetch OHLCV for ticker (dynamic lookback based on max indicator period)
2. Calculate all 20 indicators using ta-lib (returns float64 numpy arrays)
3. Validate values: NaN → None, hard limit check, soft clamp
4. Convert validated floats to Decimal for storage
5. Return TechnicalIndicators dataclass

## Phase 5: CLI Integration

### Extend: `sawa/daily.py`

Add TA calculation to daily workflow:
```python
# After price update completes
if not args.skip_ta:
    calculate_technical_indicators(tickers)
```

New argument: `--skip-ta` to skip TA calculation

### New Command: `sawa/ta_backfill.py`

Full history recalculation:
```bash
sawa ta-backfill                    # Recalculate all tickers, all history
sawa ta-backfill --ticker AAPL      # Single ticker
sawa ta-backfill --workers 8        # Parallel workers
sawa ta-backfill --dry-run          # Preview what would be calculated
sawa ta-backfill --estimate         # Run performance estimate on 10 tickers
```

**Performance estimation output:**
```
Testing on 10 tickers...
Per-ticker calculation time: 0.52s
Estimated total time (500 tickers):
  - Sequential: 4.3 minutes
  - 4 workers: 1.1 minutes
  - 8 workers: 32 seconds
```

## Phase 6: Validation & Monitoring

### Validation Rules

| Indicator | Min | Max | Bounded | Tolerance |
|-----------|-----|-----|---------|-----------|
| RSI | 0 | 100 | Yes | 1% (clamp) |
| Volume Ratio | 0 | None | No | 1% (clamp) |
| ATR | 0 | None | No | 1% (clamp) |
| Volume SMA | 0 | None | No | 1% (clamp) |
| OBV | None | None | No | N/A (can be negative) |
| All others | None | None | No | N/A |

### Monitoring
- Calculation time per ticker
- Validation warning count (clamped values)
- Validation error count (rejected values)
- NULL count per indicator (insufficient data)
- Database insert batch size and time

## Implementation Order

1. **Schema** - Create `11_technical_indicators.sql`
2. **Model** - Create `TechnicalIndicators` dataclass with Optional types
3. **Engine** - Implement `ta_engine.py` with ta-lib, validation, NaN handling
4. **Repository** - Implement `technical_indicators.py` with screening queries
5. **CLI** - Add to `daily.py` and create `ta_backfill.py`
6. **Tests** - Unit tests for calculation, validation, NaN handling, repository

## Testing Recommendations

1. **Unit tests** for validation logic:
   - NaN handling (returns None)
   - Values within bounds (pass through)
   - Values slightly outside bounds (<1%): clamp with warning
   - Values significantly outside bounds (>1%): raise ValueError

2. **Performance tests**:
   - Estimate backfill time on 10-50 tickers
   - Verify parallelization efficiency vs overhead
   - Memory usage during batch processing

3. **Integration tests**:
   - Screening queries with complex filters
   - NULL handling in queries
   - Bulk insert performance

4. **Data validation tests**:
   - Post-backfill anomaly detection
   - Cross-check against external TA calculators

## Future Expansion

To add indicators 21-120:

1. **Schema**: Add columns to `technical_indicators` table
2. **Model**: Add fields to `TechnicalIndicators` dataclass
3. **Metadata**: Insert rows into `technical_indicator_metadata`
4. **Engine**: Add calculation functions, update lookback if needed
5. **Backfill**: Run `sawa ta-backfill` to populate historical data

## Dependencies

```
ta-lib>=0.4.0          # Technical analysis library (C library with Python bindings)
numpy>=1.20.0          # Required by ta-lib (float64 arrays)
psutil>=5.9.0          # Memory/CPU monitoring during backfill
tqdm>=4.65.0           # Progress bars for long-running operations
```

## Notes

- Polygon.io does NOT provide pre-calculated TA indicators
- All calculations done locally from stored OHLCV data
- ta-lib is industry-standard C library with Python bindings
- Single table design optimized for screening queries
- Decimal used for storage (consistency with prices), float64 for calculation
- NULLs allowed for insufficient data (first ~50 days depending on indicator)
- BRIN indexes chosen over B-tree for time-series range scans (smaller, faster)
