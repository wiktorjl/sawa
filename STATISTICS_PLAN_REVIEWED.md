# Technical Indicators Storage Implementation Plan - Review

## Critical Issues

### 1. NaN Handling Not Addressed
- **Problem**: Plan states "No NULLs" but impossible for first ~50 days of data
- **Reason**: SMA-50, EMA-50 require 50+ trading days. First ~70 calendar days (including weekends) will produce NaN from ta-lib
- **Impact**: Cannot insert data for first 2-3 months of any stock's history
- **Solution needed**: Strategy for insufficient data:
  - Option A: Allow NULLs for insufficient data (simplest)
  - Option B: Use default values (0 for indicators, price for SMAs)
  - Option C: Skip rows until all indicators calculable (loses 50 days of data)
  - Store `min_data_points_required` in metadata table

### 2. Schema File Number Conflict
- **Problem**: `08_technical_indicators.sql` conflicts with existing `08_sic_gics_mapping.sql`
- **Impact**: Overwrites existing schema file if not careful
- **Solution**: Use `11_technical_indicators.sql` (next available number per sqlschema/README.md)

### 3. Missing Screening Indexes
- **Problem**: Main use case is multi-indicator filtering (e.g., `WHERE rsi_14 < 30 AND bb_upper < close`), but only basic indexes exist
- **Current indexes**: `idx_ta_date`, `idx_ta_ticker_date_desc`
- **Missing**: No indexes on indicator columns needed for screening queries
- **Solution**: Add BRIN indexes for each frequently screened indicator:
  ```sql
  CREATE INDEX idx_ta_rsi_14 ON technical_indicators USING BRIN (rsi_14);
  CREATE INDEX idx_ta_bb_upper ON technical_indicators USING BRIN (bb_upper);
  CREATE INDEX idx_ta_atr_14 ON technical_indicators USING BRIN (atr_14);
  -- Add for other indicators commonly filtered
  ```
- **BRIN vs B-tree**: BRIN is ideal for time-series data (smaller, faster for range scans)

### 4. OBV Sign Handling Missing
- **Problem**: Validation table shows volume_ratio >= 0, but OBV (On Balance Volume) can be negative
- **Reason**: OBV is a cumulative indicator. When stock declines on high volume, OBV decreases and can become negative
- **Impact**: No validation rule for OBV, but it has known bounds
- **Solution**: Add OBV to metadata with proper validation:
  ```sql
  ('obv', 'obv', 'volume', 'On Balance Volume', NULL, NULL, 18)
  ```
  Or add `is_signed` flag to metadata table to indicate negative values allowed

### 5. 200-Day Lookback Limit Too Restrictive
- **Problem**: Lookback limited to 200 days, but SMA-200 requires ~280 trading days
- **Reason**: 200 trading days ≈ 280 calendar days (weekends, holidays)
- **Impact**: Cannot calculate SMA-200 which is a common technical indicator
- **Solution**: Remove arbitrary 200-day limit. Calculate required lookback dynamically:
  ```python
  max_period = max(indicator_periods)  # e.g., 200 for SMA-200
  lookback_days = int(max_period * 1.4)  # ~280 days
  ```

## Concerns

### 6. Validation Tolerance Hides Bugs
- **Problem**: Clamping values to ±0.1% tolerance might hide calculation errors
- **Example**: If RSI returns 105 (should be 0-100), this is a bug, not a tolerance issue
- **Current logic**:
  ```python
  if abs(value - min_val) / abs(min_val) < 0.001:
      return min_val  # Clamps 105 to 100 for RSI
  ```
- **Concern**: RSI = 105 indicates a calculation bug, not a floating-point error
- **Solution**: Separate concepts:
  - **Tolerance**: For floating-point precision errors (e.g., 100.001 → 100)
  - **Hard limits**: Reject calculations that exceed valid bounds significantly
  - **Log warnings**: Always log clamped values for audit trail

### 7. Backfill Scalability Unknown
- **Problem**: No time estimates for full backfill
- **Scale**: 500 stocks × 252 days = 126,000 indicator calculations
- **Unknown variables**:
  - Calculation time per ticker (depends on ta-lib speed)
  - Database insert time per batch
  - Parallelization efficiency
- **Example math**:
  - If ~0.5 sec/ticker: 500 × 0.5 = 250 sec ≈ 4.2 minutes (sequential)
  - If ~0.5 sec/ticker with 8 workers: 4.2 minutes / 8 ≈ 32 seconds
  - If ~2 sec/ticker: 17 minutes (sequential), 2 minutes (8 workers)
- **Solution needed**: Add performance testing phase:
  ```python
  # Test on 10 tickers first, then extrapolate
  test_tickers = tickers[:10]
  start = time.time()
  calculate_indicators(test_tickers)
  elapsed = time.time() - start
  estimated_total = elapsed * len(tickers) / len(test_tickers)
  ```

### 8. Decimal vs Float Overhead Not Justified
- **Problem**: Plan converts ta-lib floats to Decimal, but no justification provided
- **ta-lib behavior**: Returns float64 numpy arrays
- **Decimal overhead**: Converting 20 floats × 126k rows adds significant CPU time
- **Current pattern**: Codebase uses Decimal for prices (NUMERIC(12,4) in database)
- **Question**: Why Decimal for indicators? Float is acceptable for most TA indicators
- **Solution options**:
  - Use float for calculation, Decimal for storage (current plan)
  - Use float throughout if precision isn't critical (RSI 14.123456 vs 14.123)
  - Document why Decimal is needed (precision, consistency with prices)

## Minor Suggestions

### 9. Metadata Table Enhancements
Add columns to `technical_indicator_metadata` for better API integration:

```sql
ALTER TABLE technical_indicator_metadata
ADD COLUMN is_bounded BOOLEAN DEFAULT FALSE,
ADD COLUMN default_value NUMERIC(12, 4),
ADD COLUMN display_name VARCHAR(100),
ADD COLUMN unit VARCHAR(20);  -- 'percent', 'dollars', 'ratio', 'count'
```

**Purpose**:
- `is_bounded`: TRUE for RSI (0-100), FALSE for SMA (unbounded)
- `default_value`: Value to use when insufficient data (e.g., 0 for indicators, close price for SMAs)
- `display_name`: Human-readable name for UI ("14-day Relative Strength Index")
- `unit`: For API responses and formatting

### 10. Document BIGINT Assumption for Volume
Volume columns use BIGINT, which fits 8-byte signed integers.

**Assumption**: Daily volume < 9,223,372,036,854,775,807

**Math**:
- Average daily volume: ~50 million shares
- 20-day average: 50M × 20 = 1 billion
- 200-day average: 50M × 200 = 10 billion

**Verification**: 10B << 9.2 quintillion (BIGINT limit), safe.

**Edge case**: If a stock splits 100:1 and volume reports pre-split, could be problematic. Add comment in schema:
```sql
volume_sma_20 BIGINT,  -- Assumes post-split volume reporting
```

### 11. Add Partial Indexes for Common Screening Patterns
For frequently queried combinations, add partial indexes:

```sql
-- Oversold stocks (RSI < 30)
CREATE INDEX idx_ta_rsi_oversold
ON technical_indicators(ticker, date)
WHERE rsi_14 < 30;

-- High volatility (ATR > 2% of price)
CREATE INDEX idx_ta_high_volatility
ON technical_indicators(ticker, date)
WHERE atr_14 > 0;

-- Price above SMA-50 (bullish trend)
CREATE INDEX idx_ta_above_sma_50
ON technical_indicators(ticker, date)
WHERE close > sma_50;
```

**Note**: Requires adding `close` column to `technical_indicators` for price-relative queries.

## Recommended Implementation Changes

### Schema (sqlschema/11_technical_indicators.sql)

1. Allow NULLs for all indicators (add `NULL` to column definitions):
```sql
sma_5 NUMERIC(12, 4) NULL,
sma_10 NUMERIC(12, 4) NULL,
-- etc.
```

2. Add screening indexes (BRIN):
```sql
CREATE INDEX idx_ta_rsi_14 ON technical_indicators USING BRIN (rsi_14);
CREATE INDEX idx_ta_atr_14 ON technical_indicators USING BRIN (atr_14);
CREATE INDEX idx_ta_macd_line ON technical_indicators USING BRIN (macd_line);
```

3. Enhance metadata table:
```sql
ALTER TABLE technical_indicator_metadata
ADD COLUMN is_bounded BOOLEAN DEFAULT FALSE,
ADD COLUMN default_value NUMERIC(12, 4),
ADD COLUMN display_name VARCHAR(100),
ADD COLUMN unit VARCHAR(20);
```

### Domain Model (sawa/domain/technical_indicators.py)

Allow Optional types for indicators:
```python
from typing import Optional

@dataclass(frozen=True, slots=True)
class TechnicalIndicators:
    ticker: str
    date: date
    
    # Trend (8 indicators)
    sma_5: Decimal | None
    sma_10: Decimal | None
    # etc.
```

### Calculation Engine (sawa/calculation/ta_engine.py)

1. Remove 200-day limit:
```python
# Calculate required lookback dynamically
max_period = max([5, 10, 20, 50, 12, 26, 14])  # From indicators
lookback_days = int(max_period * 1.4)  # ~280 days
```

2. Add performance estimation:
```python
def estimate_backfill_time(tickers: list[str]) -> tuple[int, float]:
    """Estimate backfill time based on test run."""
    test_tickers = tickers[:10]
    start = time.time()
    for ticker in test_tickers:
        calculate_indicators(ticker)
    elapsed = time.time() - start
    per_ticker = elapsed / len(test_tickers)
    total = per_ticker * len(tickers)
    return len(tickers), total
```

3. Improve validation logic:
```python
def validate_indicator(
    name: str,
    value: float,
    min_val: float | None,
    max_val: float | None,
    logger: logging.Logger,
) -> float:
    """Validate and clamp indicator values."""
    
    # Hard bounds check (no tolerance)
    if min_val is not None and value < min_val:
        if abs(value - min_val) > min_val * 0.01:  # > 1% over limit
            logger.error(f"{name} value {value} far below min {min_val}")
            raise ValueError(f"Invalid {name}: {value}")
        # Minor deviation (< 1%): clamp with warning
        logger.warning(f"Clamping {name} from {value} to {min_val}")
        return min_val
    
    if max_val is not None and value > max_val:
        if abs(value - max_val) > max_val * 0.01:
            logger.error(f"{name} value {value} far above max {max_val}")
            raise ValueError(f"Invalid {name}: {value}")
        logger.warning(f"Clamping {name} from {value} to {max_val}")
        return max_val
    
    return value
```

### Repository Layer (sawa/repositories/technical_indicators.py)

Add screening query method:
```python
async def screen_by_indicators(
    self,
    filters: dict[str, tuple[float, float]],  # {"rsi_14": (0, 30), "atr_14": (0.5, None)}
    target_date: date,
    limit: int = 100,
) -> list[TechnicalIndicators]:
    """Screen stocks by multiple indicator values."""
    
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
    params.extend([target_date, limit])
    
    # Execute and return TechnicalIndicators objects
```

## Next Steps

1. **Address critical issues** before implementing
2. **Performance test** calculation engine on sample data (10-50 tickers)
3. **Define NaN handling strategy** (allow NULLs vs default values)
4. **Update schema file number** to `11_technical_indicators.sql`
5. **Add screening indexes** for common indicator queries
6. **Implement metadata enhancements** for better API support
7. **Document Decimal vs float decision** with justification

## Testing Recommendations

1. **Unit tests** for validation logic with edge cases (NaN, extreme values)
2. **Performance tests** for backfill time estimation
3. **Integration tests** for screening queries with complex filters
4. **Data validation tests** post-backfill (check for anomalies)
5. **Parallel processing tests** verify efficiency gains vs overhead

## Dependencies Update

Add performance monitoring library:
```
psutil>=5.9.0  # For memory/CPU monitoring during backfill
tqdm>=4.65.0   # Progress bars for long-running operations
```
