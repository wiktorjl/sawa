# Heads up: VIX moved

**Date:** 2026-05-13
**Migration:** `sqlschema/29_consolidate_vix.sql`
**Breaking?** Yes, if your app reads `^VIX` or `^VIX3M` from `stock_prices`.

## What changed

VIX used to live in two places:

1. `market_internals.vix_close` — FRED-sourced daily close.
2. `stock_prices` under tickers `^VIX` and `^VIX3M` — a legacy mirror so
   apps could query VIX like a stock.

Both are now one place: **`market_internals`**, with the column renamed
to `vix` (the row is already a daily snapshot — the `_close` suffix was
noise).

The `^VIX` and `^VIX3M` rows were deleted: 2 from `companies`, 2,663 from
`stock_prices`, 2,522 from `technical_indicators`. They were stale anyway
(last update 2026-02-26) because Polygon never actually served those
caret-prefix tickers.

## Migrating your queries

| Old                                                                | New                                              |
| ------------------------------------------------------------------ | ------------------------------------------------ |
| `SELECT close FROM stock_prices WHERE ticker = '^VIX'`             | `SELECT vix FROM market_internals`               |
| `SELECT close FROM stock_prices WHERE ticker = '^VIX3M'`           | `SELECT vix3m FROM market_internals`             |
| `SELECT vix_close FROM market_internals`                           | `SELECT vix FROM market_internals`               |
| `SELECT * FROM technical_indicators WHERE ticker = '^VIX'`         | **Not available.** See "VIX TA" below.           |

Order by `date DESC` and add a date filter — same as before.

## New: enriched view

For anything beyond raw VIX, use `v_market_internals_enriched`:

```sql
SELECT
    date,
    vix,
    vix3m,
    hy_spread,
    term_structure,        -- vix3m / vix  (>1 = healthy contango, <1 = stress)
    vix_sma_20,            -- 20-day rolling mean of vix
    vix_std_20,            -- 20-day rolling stddev of vix
    vix_pct_rank_252d,     -- where vix sits in trailing 252-day distribution
    hy_pct_rank_252d       -- same for hy_spread
FROM v_market_internals_enriched
ORDER BY date DESC
LIMIT 30;
```

Use cases:
- **Regime detection**: `vix > vix_sma_20 + 2 * vix_std_20` → stress regime.
- **Mean-reversion**: `vix_pct_rank_252d > 0.95` → top-5% panic, historically mean-reverts.
- **Term-structure stress**: `term_structure < 1` → backwardation, often precedes spikes.
- **Credit stress**: `hy_pct_rank_252d > 0.90` → high-yield spread elevated vs the past year.

## New: MCP tool

If you reach this data through the MCP server, there's a dedicated tool:

```
get_market_internals(start_date="2026-05-01", end_date="2026-05-12", limit=100)
```

Returns rows with `vix`, `vix3m`, `hy_spread`, plus all five derived
metrics above. The `get_economy_dashboard` tool now also surfaces `vix`,
`vix3m`, and `hy_spread` alongside rates/inflation/labor.

## On VIX technical indicators

We deliberately removed VIX from `technical_indicators`. Most standard TA
(SMA-50/150/200, MACD, ATR) is designed for trending price series and
doesn't translate to a mean-reverting, range-bound index like VIX.

The two that genuinely matter (RSI-14, Bollinger Bands) are easy to add
to `v_market_internals_enriched` if you need them — file a ticket and
mention what you'd use them for.

## Data freshness

Source is FRED. `market_internals` is updated daily by the `sawa daily`
cron run after market close (and weekly as a safety net). If
`FRED_API_KEY` is missing, the run will log an ERROR and post to NTFY
(`SawaNotifications1000`), so stale data won't go silent again.

## Questions

Bring them to me. The migration SQL is in `sqlschema/29_consolidate_vix.sql`
if you want to see the exact ALTER/CREATE/DELETE sequence.
