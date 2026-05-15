# Technical Indicator Expansion Plan

> Status: proposal, **Revision 2 (lean)**. No code changes yet. Intended
> as the single document reviewers read before approving Phase 1.

This plan covers ~70 new indicator/event/anchor fields organised in
seven groups (the original spec). It is broken into:

1. **What's already there** — corrects three assumptions in the spec.
2. **Storage tier philosophy** — *new in Revision 2*. Three tiers
   (stored / view-derived / runtime), with rules for which goes where.
3. **Storage architecture** — wide-table extension footprint.
4. **Per-indicator implementation specs** — every field, with the exact
   formula, computational pattern, NaN rules, dependencies, **and a
   storage-tier tag**. Grouped by **implementation pattern** (not by
   spec group) because that is how the work actually splits.
5. **Computational-pattern reference** — twelve reusable patterns; the
   per-indicator table refers to them by letter.
6. **Phased delivery and backfill strategy.**
7. **Risks, open questions, and decisions needed.**

> **Revision 2 summary (May 2026).** Reviewer pushback on column count
> and on the under-specified AVWAP anchor list led to substantial
> trimming. Final wide-table width: ~45 columns, not ~82. Volume
> profile cut. Anchored VWAP delivered as a runtime MCP tool, not a
> pre-materialized table. Most Group 3 event flags moved into a VIEW
> instead of stored columns. See Revision history (§9) for the
> point-by-point diff against Revision 1.

---

## 1. Current state vs. spec

Verified against the codebase before drafting:

| Spec assumption | Reality |
|------|---------|
| "sma_20/50/150/200 computed inline in scan SQL; not stored" | Already stored in `technical_indicators` (`sma_5/10/20/50/100/150/200`, `ema_12/26/50/100/200`). Scanners read `ti.sma_50` etc. from the table. The "inline" claim no longer holds. |
| "RSI / MACD / ATR / BB — UW endpoint; not stored per-ticker" | All already stored (`rsi_14/21`, `macd_line/signal/histogram`, `bb_upper/middle/lower`, `atr_14`). UW need not be hit at scan time today. |
| "OBV missing entirely" | `obv` column exists; only the **slope** is missing. |
| "volume_20d_avg — no" | Stored as `volume_sma_20`; `volume_ratio` (today / 20-day average) also stored. |
| "ADX-14 missing entirely" | Confirmed. |
| "rrg_relative_strength / rrg_rotation_signals already in DB" | **Not in DB.** Sawa has a runtime tool (`mcp_server/tools/multi_timeframe.py::calculate_relative_strength`) that computes RS on demand. No persisted RS table exists. |
| `earnings` table | Exists (`sqlschema/18`/`19`), keyed `(ticker, report_date)`, has `surprise_pct`. Populated only by manual `scripts/populate_earnings.py` (yfinance). |
| Premarket bars in `stock_prices_intraday` | **Not captured.** WS subscribes only to `AM.*` (regular-session aggregates) — see `sawa/api/websocket_client.py:114`. Premarket-volume fields require a separate ingest. |

**Implication.** The "biggest architectural win" the spec calls out
(stop calling UW per-ticker at scan time) is mostly already in place
for moving averages and standard momentum/volatility. Remaining gaps:
ADX, OBV-derived, dollar-volume, lookback aggregates (Group 2), event
flags (Group 3), all of intraday TA (Group 4), all of anchored/profile
(Group 5), and the entire RRG layer (Group 6).

---

## 1.5 Storage tier philosophy (new in Revision 2)

Every indicator lives in one of three tiers. A field's tier is chosen
by **what it costs to compute on read**, not by what group it belongs
to in the spec.

| Tier | Where | When to use | Cost model |
|------|-------|-------------|-----------|
| **Stored** | column on `technical_indicators` (or a dedicated table) | costly to compute at scan time (rolling windows, cross-series joins, intraday aggregation) | write once nightly; read free |
| **View** | computed in `v_technical_indicators` from stored columns + `stock_prices` | cheap derivation from already-stored fields (booleans, ratios, comparisons) | recomputed per query but predicates push down to indexed base columns; effectively free |
| **Runtime** | computed inside an MCP tool on demand | needs a parameter from the caller (anchor date, lookback length), or has fundamentally subjective semantics that should not be locked in a schema | computed when the tool is called, returned in the tool's response |

### 1.5.1 The decision tree

```
Does the value need a parameter from the caller?
   yes → Runtime (MCP tool)
   no ↓
Is the formula a 1-line function of fields already on technical_indicators
or stock_prices?
   yes → View
   no ↓
Does it need a multi-row window, cross-series join, or intraday aggregation?
   yes → Stored
   no  → View (the simple case won)
```

### 1.5.2 Why this matters

The first draft (Revision 1) defaulted everything to **Stored** because
"the scanner stays single-table." That mantra is conditional —
Postgres joins on `(ticker, date)` indexes are fast, and the cost of
write-amplifying every derived boolean across 7M+ rows is real. The
lean version stores only the values that are genuinely expensive to
compute on read; the rest live in `v_technical_indicators`, which
joins `technical_indicators` and `stock_prices` and exposes the
derived event flags.

A field tagged **View** can always be promoted to **Stored** later if
profiling shows the view's predicate path is too slow. Going the
other direction (cutting a stored column) is migration work. **Bias
toward View on first ship.**

### 1.5.3 Net column impact

| Source | New columns on `technical_indicators` |
|------|------|
| Revision 1 estimate | ~57 |
| Revision 2 (this version) | ~20 |
| `v_technical_indicators` view (no schema cost) | ~14 derived fields exposed |
| Cut entirely (deferred until concrete consumer asks) | ~8 |
| Runtime tool (no schema cost) | ~5 (AVWAP family) |

Final `technical_indicators` width: 25 → ~45 columns, not 82.

---

## 2. Storage architecture

### 2.1 Where each group lives

| Group | Storage | Rationale |
|------|---------|-----------|
| 1 — daily scalars | **Extend `technical_indicators`** (ALTER TABLE ADD COLUMN) | All require multi-day windows or external series; **all stored**. ~8 new columns. |
| 2 — lookback aggregates | **Extend `technical_indicators`** for the windowed aggregates; **VIEW** for the trivial comparisons | The 14/20/60/252-day rolling computations are expensive on read (Stored). Two-line booleans like `sma_150_above_sma_200`, `sma_200_rising` are free (View). ~9 stored, ~4 view. |
| 3 — event flags | Mostly **VIEW** (`v_technical_indicators`); only `gap_pct` is Stored | Most event flags are 1-line predicates over already-stored fields; the view absorbs them. `gap_pct` needs `LAG(close)` and is filtered often enough to justify materializing. **Resistance-helper fields cut entirely** (deferred until a concrete scanner needs them). ~1 stored, ~10 view, ~8 cut. |
| 4 — intraday-derived | **New `technical_indicators_intraday (ticker, date PK)`** | Unchanged from Revision 1: lifecycle differs (computed after WS shutdown), separate table keeps the daily TA pass pure-OHLCV. |
| 5 — anchored VWAP | **Runtime MCP tool** `mcp_server/tools/avwap.py` | Anchor selection has no objective right answer; pre-materializing forces Sawa to pick. The tool takes `(ticker, anchor_date)` from the caller, computes Σ(typical·v)/Σ(v) on the fly, returns. **No new table.** ~5ms per call. **Volume profile cut entirely.** |
| 6 — cross-source | **New `rrg_relative_strength (ticker, benchmark, date PK)`** + slim mirror columns on TI; UW options cache on hold | Three RS fields (`rs_ratio_21d`, `rs_ratio_63d`, `rs_line_at_60d_high`) mirrored to TI for fast scans. RRG quadrants and PEAD also stored. UW options cache (4b) separate, gated on UW key. |
| 7 — breakout log | **New `sa_breakouts (ticker, breakout_date, system PK)`** | Unchanged. Event log, mutated by the scanner (not the TA job). |

### 2.2 Wide-table sizing — Revision 2 footprint

Phases 1 + 4a take `technical_indicators` from 25 columns to **~45**
(Group 1: +8, Group 2 stored aggregates: +9, Group 3 stored: +1,
Group 6 RS mirror: +3, PEAD: +4 — minus `rvol_today` which aliases
the existing `volume_ratio`, see §6.1.4). Phase 4b (UW options) adds
+4 more *if* it ships. Phase 3 adds zero columns under the runtime
AVWAP design.

Comfortably within Postgres limits and small enough that the
write-amplification cost is reasonable. Two follow-ups remain
relevant even at this leaner size:

1. The backfill loader (`sawa/database/ta_load.py`) does row-by-row
   `INSERT ... ON CONFLICT`. With a 60-column table over ~5500 tickers ×
   1300 trading days that is ~7.2M rows; row-by-row is the bottleneck.
   Switch the *backfill* path to `psycopg.copy` while keeping per-row
   UPSERT for the daily incremental path.
2. `TechnicalIndicators.column_names()` and `to_tuple()` in
   `sawa/domain/technical_indicators.py` are **hand-maintained** lists —
   adding 35 fields to both manually is error-prone. Refactor to
   `dataclasses.fields(cls)` once, then every new column is one
   dataclass-field declaration.

### 2.3 Migration numbering

Next free `NN` is `30`. The plan needs migrations `30`–`36` (one per
table or column-set, kept idempotent so `sawa coldstart --no-drop`
re-applies cleanly).

---

## 3. Per-indicator implementation specs

Every field below references a **pattern letter (A–H)**. The patterns
themselves are described in §4.

Notation used throughout:
- `c, h, l, o, v` are the daily numpy arrays of close/high/low/open/volume.
- `n` is the number of trading days for the ticker.
- "Cost" is per-ticker per-backfill-run, assuming `n ≈ 1300` (5y).
- "NaN policy" describes what happens when the lookback window isn't
  satisfied. The default is `NULL` in the database. Validation against
  bounds happens via `validate_indicator()` in `ta_engine.py`.

### 3.1 Group 1 — new daily scalars (all Stored)

| Field | Tier | Type | Pattern | Formula | NaN policy | Cost | Notes |
|------|------|------|---------|---------|------------|------|-------|
| `ema_10` | Stored | NUMERIC(12,4) | A | `talib.EMA(c, 10)` | First 9 NULL | trivial | — |
| `ema_20` | Stored | NUMERIC(12,4) | A | `talib.EMA(c, 20)` | First 19 NULL | trivial | — |
| `adx_14` | Stored | NUMERIC(10,6) | A | `talib.ADX(h, l, c, 14)` | First 27 NULL (Wilder 14 + 14 ramp) | trivial | TA-Lib uses Wilder smoothing internally — matches the spec's "14, Wilder". |
| `bb_width_pct` | Stored | NUMERIC(10,6) | B | `(bb_upper - bb_lower) / bb_middle * 100` | NULL where any input is NULL | trivial | Free to compute, but co-located with `bb_width_pct_rank_60d` (which IS expensive on read) so we store both. |
| `obv_slope_20d_norm` | Stored | NUMERIC(12,6) | D | rolling OLS slope of `obv` over a 20-day window, **normalized by `volume_sma_20`** | First 19 NULL | one stride view + closed-form | Normalization explained at §4.D scale-warning. |
| `volume_sma_20` | already exists | — | — | — | — | — | Listed in spec but already stored. |
| `dollar_volume_20d_avg` | Stored | BIGINT | C | `talib.SMA(c * v, 20)` | First 19 NULL | trivial | BIGINT (max ~$10^15, fits in INT8). |
| `beta_60d` | Stored | NUMERIC(10,6) | F+D | rolling OLS slope of ticker daily returns vs SPY daily returns over 60 trading days | First 60 NULL; NULL if window is <80% full (recent IPO) | one OLS pass with sliding window | See §4.F. |
| `beta_252d` | Stored | NUMERIC(10,6) | F+D | same, window 252 | First 252 NULL; same partial-window rule | same | — |

### 3.2 Group 2 — lookback aggregates

Mixed tiers: the rolling-window computations are **Stored** (expensive
on read); the trivial comparisons are **View** (free); two speculative
ones (`recent_swing_low_20d`, `higher_lows_count_last_10`,
`days_below_sma50_last_20`) are **Cut** until a concrete consumer
asks.

| Field | Tier | Type | Pattern | Formula | NaN policy | Cost | Notes |
|------|------|------|---------|---------|------------|------|-------|
| `pct_from_52w_high` | Stored | NUMERIC(10,6) | C | `(c − rolling_max(h, 252, exclusive_of_today)) / rolling_max(h, 252, exclusive_of_today) * 100` | First 253 NULL | sliding view of `h[:-1]` | Excluding today lets a new 52w-high register positive. |
| `pct_from_52w_low` | Stored | NUMERIC(10,6) | C | symmetric | First 253 NULL; NULL if rolling-min is 0 | same | Divide-by-zero guard. |
| `range_20d_pct` | Stored | NUMERIC(10,6) | C | `(rolling_max(h, 20) − rolling_min(l, 20)) / rolling_min(l, 20) * 100` | First 20 NULL | two sliding-window reductions | Used to identify tight bases. |
| `adr_pct` | Stored | NUMERIC(10,6) | C | `(mean(h / l, 14) − 1) * 100` (IBD convention) | First 14 NULL; NULL if any `l == 0` | one elementwise op + SMA | IBD geometric form, not `(h-l)/c`. |
| `volume_trend_base_20d` | Stored | TEXT (CHECK) | C | `mean(v[-10:]) / mean(v[-20:-10])` → `contracting` <0.85, `expanding` >1.15, else `flat`; NULL if denom is 0 | NULL until 20 days | two SMA reads | Thresholds in `ta_aggregates.py`. |
| `bb_width_pct_rank_60d` | Stored | NUMERIC(6,4) | C | percentile rank of today's `bb_width_pct` within the prior 60 values, average-rank ties, in [0, 1] | First 80 NULL; partial-window rule below 30 | one stride view + `scipy.stats.rankdata` | Squeeze-detection input. |
| `consecutive_macd_hist_rising` | Stored | INT2 | E | streak length of `macd_hist[i] > macd_hist[i-1]`, ending today | 0 if today not rising; NULL until MACD warm-up | one `np.diff` + run length | Vectorizable. |
| `obv_trend_20d` | Stored | TEXT (CHECK) | D | from `obv_slope_20d_norm`: `up` if `> +0.5`, `down` if `< -0.5`, else `flat` | NULL where slope NULL | trivial | Fixed threshold because slope is already normalized. |
| `sma_200_rising` | **View** | BOOLEAN | G | `sma_200 > LAG(sma_200, 20) OVER (PARTITION BY ticker ORDER BY date)` | NULL where either side NULL | view-side window function | Pure SQL. |
| `sma_150_above_sma_200` | **View** | BOOLEAN | B | `sma_150 > sma_200` | NULL where either NULL | trivial | Pure SQL. |
| `recent_swing_low_20d` | **Cut** | — | C | `rolling_min(l, 20)` | — | — | Useful only as a stop reference. Restore as Stored if a scanner needs it; until then, the scanner can compute `MIN(low) OVER (... rows 19 preceding)` directly. |
| `days_below_sma50_last_20` | **Cut** | — | C′ | `Σ(c[i] < sma_50[i])` over last 20 | — | — | Speculative. Restore as Stored if a System needs it. |
| `higher_lows_count_last_10` | **Cut** | — | E | swing-low detection + count-rising | — | — | Subjective swing definition. Defer until a system needs it. |

### 3.3 Group 3 — event flags

Most are **View** (cheap derivations from already-stored fields).
Resistance-helper fields are **Cut** entirely — they introduce a
bespoke level-detection algorithm whose definition will inevitably
need to be re-tuned per scanner. Defer until a System has a written
spec saying which resistance definition it needs.

| Field | Tier | Type | Pattern | Formula | NaN policy | Cost | Notes |
|------|------|------|---------|---------|------------|------|-------|
| `gap_pct` | **Stored** | NUMERIC(10,6) | G | `(o − LAG(c)) / LAG(c) * 100` | NULL on day 1; NULL where prior close missing or 0 | trivial | Stored (not view) because it is filtered often and BRIN-indexed. Verify open + close come from the same adjustment regime in `sawa/database/load.py`. |
| `close_above_sma_50` | **View** | BOOLEAN | B | `close > sma_50` | NULL where SMA-50 NULL | trivial | View. |
| `close_above_sma_200` | **View** | BOOLEAN | B | `close > sma_200` | NULL where SMA-200 NULL | trivial | View. |
| `prior_close_below_sma_50` | **View** | BOOLEAN | G | `LAG(close) <= LAG(sma_50)` | NULL until day 51 | trivial | View, window function. |
| `close_crosses_above_sma_50_today` | **View** | BOOLEAN | H | `close_above_sma_50 AND prior_close_below_sma_50` | NULL where either input NULL | trivial | View, composition. |
| `close_crosses_above_pivot_20_60d` | **View** | BOOLEAN | C+H | `close > MAX(high) OVER (60 preceding rows excluding current) AND LAG(close) <= same` | NULL until 60 days | one window-max | View, single window function. |
| `close_pct_in_range` | **View** | NUMERIC(8,4) | B | `(close − low) / NULLIF(high − low, 0)` | NULL on doji | trivial | View. |
| `close_in_upper_half_of_range` | **View** | BOOLEAN | B | `close_pct_in_range >= 0.5` | NULL where input NULL | trivial | View. |
| `rvol_today` | **Alias** | — | — | reuse existing `volume_ratio` | — | — | Locked: scanner identifier `rvol_today` maps to column `volume_ratio` via `technical_indicator_metadata`. **No new column.** |
| `gap_clears_resistance` | **Cut** | — | I | requires resistance helper | — | — | Defer until a scanner has a written resistance definition. |
| `levels_cleared` | **Cut** | — | I | requires resistance helper | — | — | Same. |
| `distance_to_next_resistance` | **Cut** | — | I | requires resistance helper | — | — | Same. |
| `distance_to_next_resistance_r` | **Cut** | — | I+B | requires above | — | — | Same. |
| `next_resistance_source` | **Cut** | — | I | requires resistance helper | — | — | Same. |
| `reclaim_day_low` | **Cut** | — | — | pass-through of `stock_prices.low` | — | — | Just `JOIN stock_prices ... low` at scan time. |

### 3.4 Group 4 — intraday-derived

These need `stock_prices_intraday`. **All depend on the WS feed having
captured the day's session.**

| Field | Type | Pattern | Formula | NaN policy | Cost | Notes |
|------|------|---------|---------|------------|------|-------|
| `opening_range_low_30min` | NUMERIC(12,4) | J | min `low` of bars where `(timestamp AT TIME ZONE 'America/New_York')::time` is in `[09:30, 10:00)` | NULL if no bars in that window (rare: tech-issue late opens) | one filter + min | Convert UTC → America/New_York **inside the SQL query** (`AT TIME ZONE`), not in Python. Keeps the predicate in the query plan and avoids zoneinfo-version skew. |
| `opening_range_high_30min` | NUMERIC(12,4) | J | max `high` of bars in same window | NULL if no bars | same | — |
| `holds_opening_range_low` | BOOLEAN | J | `min(low for all session bars) >= opening_range_low_30min` | NULL if ORL NULL or session-bar count < N (e.g., late tech open or half-day where session is too short) | one filter + min + compare | "Held the open" semantic. |
| `vwap_intraday` | NUMERIC(12,4) | J | `Σ((h+l+c)/3 * v) / Σ(v)` over all session bars (regular session only) | NULL if `Σ(v) == 0`; individual bars with `v=0` are kept (typical-price-weighted sum still works) | one cumulative pass | Session-aware; do not include the prior session. Half-day closes (1pm ET) reduce the bar count but the formula is unchanged. |
| `holds_vwap_after_reclaim` | BOOLEAN | J+E | find the first bar where `c` crosses from `< vwap_running` to `>= vwap_running`; TRUE if **at most 1** subsequent bar dips below VWAP (tolerance for a single 5-min wick) | NULL if no reclaim event today | one running-VWAP pass + linear scan with a 1-bar tolerance counter | Brittleness fix: the original "all subsequent bars" rule fails on a single late-day wick. Tolerance of 1 bar (=5 min) matches what traders mean by "held VWAP after reclaim". Field name kept; semantics now realistic. |
| `premarket_volume` | BIGINT | J | sum of `v` for bars in `[04:00, 09:30) ET` | **Always NULL until premarket ingest is added** (WS-only feed today) — see §6.1 decision 5 | — | **Blocked on a separate ingest.** Phase 2 ships this as documented-NULL; the value lights up retroactively if/when the premarket ingest project lands. Same column shape, same loader, no schema change needed at that point. |
| `premarket_vol_vs_avg` | NUMERIC(10,6) | J | `premarket_volume / mean(premarket_volume over last 20 trading days)` | NULL while `premarket_volume` NULL | — | Same blocker. |

**Half-day note.** US market half-days (early closes, e.g. day after
Thanksgiving) shorten the regular session to 09:30–13:00 ET. The
opening-range and VWAP fields still work; only fields that assume a
full session length need to handle this. Use the NYSE trading
calendar (already available indirectly via the dates present in
`stock_prices`); do not hardcode 16:00 ET as the close.

### 3.5 Group 5 — anchored VWAP and volume profile

**Revision 2 collapses this group dramatically.** The original spec
called for two side tables (`ta_anchors`, `ta_volume_profile`), four
anchor-detection heuristics, six derived flag columns, and a histogram
algorithm with three tunable parameters. Revision 2 replaces all of
that with **one runtime MCP tool**, no new tables, no new columns.

#### 3.5.1 AVWAP — runtime MCP tool

Anchor selection is fundamentally subjective ("major swing low" needs a
threshold; "earnings gap" needs a gap-size threshold; "capitulation
low" needs both). Pre-materializing forces Sawa to lock those
thresholds in a schema that's hard to evolve. The cheaper, more
honest design: **let the caller pick the anchor**.

New tool: `mcp_server/tools/avwap.py::get_avwap(ticker, anchor_date)`.

```python
def get_avwap(ticker: str, anchor_date: date) -> dict:
    """
    Compute Anchored VWAP from anchor_date through the latest
    trading day, using daily bars.

    Returns: {
      "ticker": str,
      "anchor_date": date,
      "as_of_date": date,        # latest trading day in stock_prices
      "avwap": Decimal | None,   # NULL if Σv == 0 across the window
      "n_bars": int,             # number of trading days included
    }
    """
```

Implementation: one SELECT against `stock_prices`,
`SUM((high+low+close)/3 * volume) / NULLIF(SUM(volume), 0)` filtered
on `(ticker = $1 AND date >= $2)`. Cost: ~5ms per call.

**Suggested anchors for callers** (documented in the tool's docstring,
not enforced by Sawa):

| Anchor | How the caller derives the date |
|------|----------|
| Last earnings | `SELECT MAX(report_date) FROM earnings WHERE ticker = … AND report_date <= today` |
| YTD | `make_date(year(today), 1, 2)` adjusted to the first trading day on or after |
| 52-week high | `SELECT date FROM stock_prices WHERE ticker = … ORDER BY high DESC, date DESC LIMIT 1` from a 252-day window |
| 52-week low | symmetric |
| Custom event | caller's choice (FDA approval, M&A announcement, etc.) |

Optionally ship a convenience wrapper
`get_avwap_suggestions(ticker)` that returns the AVWAP from each of
the four canonical anchors above in one call. This is the only place
Sawa makes anchor choices, and the caller still gets the raw values
back — no opinion is baked into stored data.

**Deferred (Option 3 upgrade path).** If, after running for a quarter,
callers consistently want pre-materialized AVWAPs from those four
canonical anchors, promote them into a `ta_anchors (ticker, anchor_type,
anchor_date PK)` table at that point. The detection rules are then
single-line SQL (per the table above) with no subjective thresholds.
The runtime tool stays as the escape hatch for custom anchors.

#### 3.5.2 Volume profile — Cut

**Cut from this expansion.** Volume profile has three tunable
parameters (bin size, value-area %, HVN-LVN peak threshold) and no
canonical convention. Materializing it commits Sawa to one set of
choices forever. If a System 4 specification arrives with concrete
parameter values, build it as a runtime tool first
(`get_volume_profile(ticker, lookback_days, bin_size_atr_fraction)`),
then promote to a table only when query patterns demand it.

#### 3.5.3 Anchor cluster — Cut

`avwap_cluster_within_1pct` was a derived signal over the (cut) anchor
table. Without a canonical anchor set, the cluster signal is undefined.
Restore as a runtime tool layered on `get_avwap_suggestions` if needed.

### 3.6 Group 6 — cross-source

#### 3.6.1 Relative strength (`rrg_relative_strength`)

**Two distinct objects** (the first draft conflated them):

1. **RS-ratio of returns over N days** — the per-window number used
   for `rs_vs_spy_1m` (N=21) and `rs_vs_spy_3m` (N=63):
   ```
   rs_ratio_N = (close[t] / close[t-N]) / (benchmark_close[t] / benchmark_close[t-N])
   ```

2. **The RS line** — a daily series, used for "RS line at new high":
   ```
   rs_line[t] = close[t] / benchmark_close[t]   # raw price ratio, no normalization
   ```
   `rs_line_vs_spy_new_high` is `rs_line[t] == max(rs_line[t-60..t])`.

Persisted columns in `rrg_relative_strength (ticker, benchmark, date PK)`:
`rs_ratio_21d, rs_ratio_63d, rs_line, rs_slope_5d, rs_slope_20d`.
Slopes are OLS slopes of `rs_line` over 5 / 20 days (Pattern D, same
closed-form trick as `obv_slope_20d_norm`).

`rrg_rotation_signals` per `(sector_etf, benchmark, date)`: standard
RRG quadrants (`leading | weakening | lagging | improving`) computed
from the sector ETF's RS-ratio (x-axis) and RS-momentum (y-axis) vs
benchmark, both normalized to 100-centered scale per
[the Trahan/Bloomberg convention](https://en.wikipedia.org/wiki/Relative_rotation_graph).
Lock the 14-day momentum window in `rrg_calc.py`.

**Stored mirror columns on `technical_indicators`** (3 only — slimmed
from Revision 1's 6):

| Field | Tier | Type | Computation |
|------|------|------|------|
| `rs_ratio_21d` | Stored | NUMERIC(10,6) | from `rrg_relative_strength` |
| `rs_ratio_63d` | Stored | NUMERIC(10,6) | from `rrg_relative_strength` |
| `rs_line_at_60d_high` | Stored | BOOLEAN | `rs_line[t] == max(rs_line[t-60..t])` |

**View-derived (no schema cost):**

| Field | Tier | Computation |
|------|------|------|
| `rs_vs_spy_rising` | View | `rs_slope_20d > 0` (joined from `rrg_relative_strength`) |
| `sector_etf` | View | from `sic_gics_mapping.gics_sector` join |
| `sector_quadrant` | View | from `rrg_rotation_signals.quadrant` join on `sector_etf` |

The view does the joins once; scanners read `v_technical_indicators`.
This avoids storing 3 redundant columns on a 7M-row table.

#### 3.6.2 Earnings-derived (PEAD)

Pure joins from `earnings`:

| Field | Type | Computation |
|------|------|------|
| `last_earnings_date` | **Stored** | DATE | `MAX(report_date) WHERE report_date <= today AND ticker = …`; **NULL** if ticker has no earnings rows |
| `last_earnings_surprise_pct` | **Stored** | NUMERIC(10,4) | `surprise_pct` for that row; NULL if NULL upstream or no row |
| `days_since_earnings` | **View** | INT2 | `today − last_earnings_date`; NULL if `last_earnings_date` NULL — pure SQL date arithmetic |
| `pead_window` | **View** | BOOLEAN | `days_since_earnings BETWEEN 1 AND 60 AND last_earnings_surprise_pct >= 5`; FALSE (not NULL) when either input NULL |

Two stored, two view-derived. The view does the date math; the joins
through `earnings` happen once at write time when the two stored
fields are populated.

These are cheap; populate during the daily TA pass via a single per-ticker JOIN.

#### 3.6.3 Options (UW)

New table `options_iv_cache (ticker, date PK)` with `ivr`,
`has_options`, `atm_oi_total`, `atm_bid_ask_spread_pct`. Populated by a
new nightly job `sawa/options_cache.py` calling Unusual Whales per
ticker. Gate behind `UNUSUAL_WHALES_API_KEY`; reuse the existing
`alert_missing_api_key` notifier. Mirror the four columns into
`technical_indicators` for scan filters.

### 3.7 The resistance helper — Cut from this expansion

> **Revision 2:** Cut. All five Group 3 fields that depended on this
> helper (`gap_clears_resistance`, `levels_cleared`,
> `distance_to_next_resistance`, `distance_to_next_resistance_r`,
> `next_resistance_source`) are themselves cut. The helper had no
> remaining consumer.
>
> The original spec is preserved below for the eventual restore: when
> a System has a written spec naming the resistance definition it
> needs, this helper is the natural starting point.

---

Several Group 3 fields depend on a "set of resistance levels above
today's close." Define **once** in `sawa/calculation/resistance.py`:

```python
def resistance_levels(
    c: float,           # today's close
    h: np.ndarray,      # 60-day high series ending today
    sma_50: float,
    sma_150: float,
    sma_200: float,
) -> list[tuple[float, str]]:
    """Return [(price, source), ...] sorted ascending, all > c."""
    levels: list[tuple[float, str]] = []
    swing_high_60 = float(np.max(h))
    if swing_high_60 > c: levels.append((swing_high_60, "swing_high"))
    for sma, name in ((sma_50, "sma_50"), (sma_150, "sma_150"), (sma_200, "sma_200")):
        if sma is not None and sma > c:
            levels.append((float(sma), name))
    # Horizontal levels: round numbers per price band
    band = _round_band(c)  # 1, 5, 10, 50 depending on price
    next_round = math.ceil(c / band) * band
    if next_round > c: levels.append((next_round, "horizontal"))
    return sorted(levels)
```

Used by `gap_clears_resistance`, `levels_cleared`,
`distance_to_next_resistance`, `next_resistance_source`. Single source
of truth; one place to tune.

**Storage decision for `levels_cleared` — array vs. JSONB.**
The first draft stored prices only (`NUMERIC(12,4)[]`). The helper
returns `(price, source)` tuples, so the obvious next ask from a
scanner author is "which sources got cleared" — a coupled
`levels_cleared_sources TEXT[]` would have to be kept in sync by app
code (Postgres can't enforce). Two viable shapes:

- **(A) Two parallel typed arrays** — `levels_cleared NUMERIC(12,4)[]`,
  `levels_cleared_sources TEXT[]`. Pros: typed, smaller, GIN-indexable.
  Cons: app-enforced sync invariant; `levels_cleared[3] ↔ levels_cleared_sources[3]`
  is a foot-gun.
- **(B) Single JSONB** — `levels_cleared JSONB` storing
  `[{"price": 187.5, "source": "sma_50"}, ...]`. Pros: one column,
  tuple cohesion enforced by structure. Cons: larger on-disk; querying
  by source is `WHERE levels_cleared @> '[{"source":"sma_50"}]'`
  (workable but unfamiliar to SQL-only readers).

**Recommendation:** ship (B) JSONB. The "1 column / 1 source of truth"
ergonomics outweighs the storage and query-style cost at our scale.
Row size is dominated by 80+ NUMERIC columns either way. Update the
Group 3 row for `levels_cleared` to match before merging the migration.

### 3.8 Group 7 — breakout log

Schema:
```sql
CREATE TABLE sa_breakouts (
    ticker VARCHAR(10) REFERENCES companies(ticker) ON DELETE CASCADE,
    breakout_date DATE,
    system TEXT,                  -- S1..S5
    breakout_price NUMERIC(12,4),
    breakout_rvol  NUMERIC(8,4),
    key_levels JSONB,             -- heterogeneous; JSONB ok here
    status TEXT,                  -- active | failed | extended
    pullback_count INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT now(),
    PRIMARY KEY (ticker, breakout_date, system)
);
```

**Owned by the scanner.** Inserted on system trigger, mutated by
subsequent passes. The TA job does not touch it.

S6 candidates derive `pullback_depth_pct`,
`pullback_volume_vs_breakout_pct`, `pullback_number` at scan time from
this log + `stock_prices` + `technical_indicators`. They are scan-time
derivations, not stored fields.

---

## 4. Computational-pattern reference

Eight patterns cover all 70+ indicators. The per-indicator tables in §3
reference them by letter.

### A — Single TA-Lib call

```python
result = talib.X(close_prices, timeperiod=N)
```

Cost: O(n). Free. NaN handling: TA-Lib emits NaN for the warm-up
window; `_to_decimal()` already converts to None.

Indicators: `ema_10`, `ema_20`, `adx_14`, all existing SMAs/EMAs/RSIs.

### B — Arithmetic on TA-Lib outputs

Combine columns we already compute. No talib call.

```python
bb_width_pct = np.where(bb_middle != 0, (bb_upper - bb_lower) / bb_middle * 100, np.nan)
```

Cost: free.

Indicators: `bb_width_pct`, `rvol_today` (already exists as
`volume_ratio`), `close_above_sma_*`, `sma_150_above_sma_200`,
`close_in_upper_half_of_range`, `distance_to_next_resistance_r`.

### C — Rolling NumPy window via stride trick

```python
from numpy.lib.stride_tricks import sliding_window_view
windows = sliding_window_view(arr, window_shape=N)  # shape (n-N+1, N)
result  = np.empty(len(arr)); result[:N-1] = np.nan
result[N-1:] = windows.max(axis=1)   # or .min, .mean, etc.
```

Cost: O(n) memory + O(n·N) compute. For N=60 over n=1300, ~78k
operations — irrelevant. For multiple sliding-window calls, build the
view once and reuse it.

`days_below_sma50_last_20` is a special case (`C′`): `np.convolve`
over a boolean mask (`(c < sma_50).astype(np.int8)`) with a length-20
all-ones kernel gives the rolling count in one shot.

Indicators: `pct_from_52w_high/low`, `range_20d_pct`, `adr_pct`,
`recent_swing_low_20d`, `days_below_sma50_last_20`,
`volume_trend_base_20d`, `bb_width_pct_rank_60d`, `dollar_volume_20d_avg`,
`close_crosses_above_pivot_20_60d` (sliding max).

### D — Rolling OLS slope (closed form)

For a window of size N with x-values `[0..N-1]` (constant), the OLS
slope is:

```
slope = ( Σ y · (i − x̄) ) / ( Σ (i − x̄)² )
```

The denominator is constant (≈ N(N²−1)/12). The numerator is a
sliding-window dot product — cheap.

Cost: O(n). Faster than scipy.stats.linregress in a loop by ~50×.

**Scale warning.** Slope is in y-units per 1 x-step (x=days). When y
varies by orders of magnitude across the universe (OBV: 10⁵–10⁹) the
raw slope overflows fixed-precision NUMERIC and can't be filtered by a
single threshold across tickers. **Normalize y before applying D**: for
OBV use `obv / volume_sma_20`; for `rs_line` it's already a ratio so
no normalization needed. The resulting unitless slope is roughly
[-10, +10] for most names and fits cleanly in `NUMERIC(12,6)`.

Indicators: `obv_slope_20d_norm`, `obv_trend_20d` (derived from slope
sign), `rs_slope_5d`, `rs_slope_20d` (and Pattern F's beta — see below).

### E — Sequential / streak

Streak of "today greater than yesterday":

```python
diff_pos = (np.diff(macd_hist) > 0).astype(np.int8)
# Trailing run length ending at each index:
#   reset counter to 0 when diff_pos == 0, increment when 1
#   vectorize via cumulative-sum trick:
counter = np.zeros(len(diff_pos), dtype=np.int32)
counter[0] = diff_pos[0]
for i in range(1, len(diff_pos)):
    counter[i] = counter[i-1] + 1 if diff_pos[i] else 0
# pad to align with original index
streak = np.concatenate([[0], counter])
```

The loop is unavoidable for general streak detection (numba speeds it
up if it shows up in profiling). For n=1300 it's irrelevant.

Cost: O(n).

Indicators: `consecutive_macd_hist_rising`, `holds_vwap_after_reclaim`
(streak of "above VWAP after reclaim"), `higher_lows_count_last_10`
(swing-low detection then streak).

### F — Cross-series with cached benchmark

For beta and RS:

```python
# In _init_worker (sawa/ta_backfill.py):
SPY_RETURNS = _load_spy_daily_returns(db_url)

# In calculate_indicators_for_ticker:
returns = np.diff(c) / c[:-1]
# Align by date with SPY_RETURNS — both indexed by date
aligned = align_by_date(dates, returns, SPY_RETURNS)
beta_60d = rolling_beta(aligned, 60)
```

Loading SPY once per worker (not per ticker) cuts ~5500 redundant DB
fetches. Worker pool with `initializer=_init_worker` already exists in
`ta_backfill.py` — pass SPY in `initargs`. **Assert SPY has data in
`_init_worker`**; if SPY is missing from `stock_prices` (delisting
accident, fresh DB, etc.) every beta and RS goes silently NULL —
fail loudly instead.

Cost: O(n) per ticker after one-time fetch.

**Pattern composition.** F describes the *data-loading* concern only.
The *math* on top of F is Pattern D (closed-form rolling OLS slope) for
beta. Implementers should reach for D's closed-form, not
`scipy.stats.linregress` per row.

**Date alignment.** When a ticker IPO'd after SPY's history starts,
the beta window will be partially full at first. Convention: require
the window to be ≥80% full; otherwise NULL. Applies to all
cross-series fields.

Indicators: `beta_60d`, `beta_252d`, all `rs_vs_spy_*` (Pattern F + D),
`sector_*` (same pattern with a small dict of ETF series, mapped from
`sic_gics_mapping.gics_sector`).

### G — Shifted compare

`np.roll` or simple slicing:

```python
prior_below = np.full(n, False)
prior_below[1:] = c[:-1] < sma_50[:-1]
```

Cost: free.

Indicators: `prior_close_below_sma_50`, `gap_pct`, `sma_200_rising`
(rolled by 20), `reclaim_day_low`, `close_pct_in_range`.

### H — Logical composition

```python
crosses = close_above_sma_50 & prior_close_below_sma_50
```

Cost: free, after the inputs exist.

Indicators: `close_crosses_above_sma_50_today`,
`close_crosses_above_pivot_20_60d`, `gap_clears_resistance`.

### I — Resistance-level dependent

Per-row scan of the resistance-level helper output (§3.7). Not
vectorizable across rows in general (the level set changes each row),
but cheap per row (≤5 levels).

Cost: O(n · k) where k≤5.

Indicators: `levels_cleared`, `gap_clears_resistance`,
`distance_to_next_resistance`, `next_resistance_source`.

### J — Per-day intraday aggregation

Group `stock_prices_intraday` by `(ticker, date_in_ET)` and compute the
session metric in pandas or pure SQL. SQL is the right tool here — one
query per ticker, runs against a `(ticker, date)`-partitioned scan.
**Use `AT TIME ZONE 'America/New_York'` inside the query** — keeps the
predicate in the plan and isolates from Python zoneinfo version skew.

Indicators: all of Group 4.

### K — Anchored cumulative

Like a windowed reduction, but the window's left edge is fixed (the
anchor date) and the right edge is today. Trivially `O(n_anchor_to_now)`
per anchor, where `n` shrinks over the year as more anchors age out.
Vectorize per ticker by slicing the per-ticker arrays.

```python
slc = slice(anchor_idx, len(c))
typical = (h[slc] + l[slc] + c[slc]) / 3
avwap = (typical * v[slc]).sum() / v[slc].sum()    # NULL if denom == 0
```

Indicators: AVWAP for every row in `ta_anchors`.

### L — Histogram-based

Build a 1-D histogram of intraday `typical_price` weighted by `v`,
then derive POC / value area / HVN-LVN levels from the histogram
shape. `numpy.histogram` is fine; the cost is dominated by reading
the 30 sessions of intraday bars (~30 × 78 bars ≈ 2.3k rows per
ticker).

Indicators: every column of `ta_volume_profile`.

---

## 5. Phased delivery

| Phase | Scope | New TI columns | Migrations | Code | Backfill cost | Unlocks |
|------|------|------|------|------|------|------|
| 0 | Refactor `TechnicalIndicators` to derive `column_names`/`to_tuple` from `dataclasses.fields(cls)`; switch backfill loader to `psycopg.copy`; add `MIN_PERIODS` / `INDICATOR_BOUNDS` registration helper keyed by dataclass field name | 0 | 0 | small | ~5× speedup on backfill | foundation for the rest |
| 1 | Group 1 stored (8) + Group 2 stored (9) + `gap_pct` (1) | **+18** | 1 (`30_ta_extended_daily.sql`) | extends `ta_engine.py`, new `ta_aggregates.py`, screener `FILTERS` map | ~1.5× current TA backfill | Systems 1, 2, 3 stored data |
| 1.5 | View `v_technical_indicators` joining TI + `stock_prices` and exposing the ~10 derived event flags + 2 PEAD view fields | 0 | 1 (`31_v_technical_indicators.sql`) | view DDL only | none (DDL only) | scanners get a single read surface that combines stored + derived |
| 2 | Group 4 — intraday TA | 0 (separate table) | 1 (`32_technical_indicators_intraday.sql`) | new `intraday_ta_engine.py`, daily-pipeline step | bounded (only days with WS history) | session-aware filters |
| 3 | Group 5 — runtime AVWAP MCP tool. **No new tables, no new columns.** | 0 | 0 | new `mcp_server/tools/avwap.py` (~40 LOC) | none | callers can compute AVWAP from any anchor date on demand |
| 4a | Group 6 RRG persistence + 3 stored mirror columns | **+3** | 1 (`33_rrg.sql`) + ALTER TI | new `rrg_calc.py`, sector-ETF map; view `v_technical_indicators` extended with sector/quadrant joins | nightly; SPY-only RRG cheap | RRG filters & sector quadrants |
| 4b | PEAD: 2 stored + 2 view fields | **+2** | ALTER TI + view update | join logic in `daily.py` | nightly join | PEAD scans |
| 4c | Group 6 UW options cache | **+4** | 1 (`34_options_cache.sql`) + ALTER TI | new `options_cache.py` (UW client), env var `UNUSUAL_WHALES_API_KEY`, ntfy alert wiring | nightly; gated by UW rate limits — scope to optionable names | options-aware filter |
| 5 | Group 7 — breakout log | 0 (separate table) | 1 (`35_breakouts.sql`) | scanner-side writes, S6 derivations at scan time | none (event log) | System 6 |

**Running total of TI columns:** 25 baseline + 18 (P1) + 3 (P4a) +
2 (P4b) + 4 (P4c) = **52 if every phase ships**, **45 if P4c is
deferred**. Volume profile and anchor table contribute zero. View-only
fields (~14) live in `v_technical_indicators` with no schema cost.

**Why split Phase 4 into 4a / 4b / 4c.** RRG, PEAD, and UW options
have independent dependencies and failure modes:
- 4a depends only on data already in the DB.
- 4b depends only on `earnings` (which is sparsely populated today —
  see `docs/DATA_SOURCES.md` §2.7).
- 4c depends on a new external API (UW). Bundling them risks holding
  RRG hostage to UW rate-limit negotiation.

### 5.1 Backfill strategy

1. **Schema migration first.** All ALTER TABLE adds are
   `ADD COLUMN IF NOT EXISTS`, NULL-defaulted — instant on Postgres
   12+. No data is touched.
2. **Backfill in two passes:**
   - *Hot pass:* recompute the last 300 trading days for every ticker
     (covers all warm-up windows up to 252-day). This is the only data
     anyone scans. Run with 8 workers; estimated ≈ current backfill time.
   - *Cold pass:* recompute the rest of history at 2 workers, low
     priority, run over a weekend. The MCP tools never query rows >300
     days old for these new fields, so the cold pass is courtesy
     completeness.
3. **Validate**: random-sample 50 tickers, spot-check 5 fields each
   against an independent calculation in a Jupyter notebook. Add the
   sampled values as fixtures to `tests/calculation/test_ta_engine_extended.py`.

### 5.2 Daily incremental cost

The daily TA job runs after market close and currently processes
~5500 tickers in ~5 minutes (8 workers). Phase 1 roughly doubles
per-ticker compute (more numpy passes). Estimated post-Phase-1 daily
job: ~10 minutes — still well within the ~1h scheduler window.

### 5.3 `technical_indicator_metadata` population

Every column added to `technical_indicators` needs a row in
`technical_indicator_metadata` for the dynamic-screener MCP tools
(`get_technical_indicators`, `screen_technical_indicators`,
`list_technical_indicators`) to surface it. Without this step the
columns exist physically but are invisible to the API.

**Mechanism.** Each phase's migration `INSERT … ON CONFLICT DO UPDATE`
into `technical_indicator_metadata` for every new column it adds —
following the pattern already in `sqlschema/17_extended_sma.sql`. PR
review must enforce that every `ADD COLUMN` line has a matching
metadata `INSERT` in the same migration. Optional follow-up: a CI
test that reads `dataclasses.fields(TechnicalIndicators)` and asserts
each is present in `technical_indicator_metadata`.

### 5.4 Index plan for new columns

BRIN works well for monotonic / time-correlated columns. B-tree only
where the planner needs equality or sort. Spec for Phase 1 (lean):

| Column | Index type | Rationale |
|------|-------------|-----------|
| `adx_14` | BRIN | range scans for trend filters |
| `bb_width_pct`, `bb_width_pct_rank_60d` | BRIN | squeeze scans |
| `pct_from_52w_high`, `pct_from_52w_low` | BRIN | proximity-to-extreme filters |
| `gap_pct` | BRIN | "today's gappers" filter |
| `dollar_volume_20d_avg` | BRIN | liquidity universal filter |
| `adr_pct` | BRIN | volatility filter |
| `(date, ticker)` covering `(adx_14, bb_width_pct, gap_pct)` | B-tree composite | the canonical "today's scan" predicate path |

Resistance / `levels_cleared` indexes removed (those columns are cut).
View-derived booleans need no index — the view's predicate pushes down
to the indexed base columns. **Scheduled profiling pass** is part of
Phase 1 acceptance.

### 5.5 Validator updates

Every new **stored** field needs entries in:

- `MIN_PERIODS` (`sawa/calculation/ta_engine.py`) — minimum lookback.
- `INDICATOR_BOUNDS` — hard min/max for `validate_indicator()`.

Bounds for the lean Phase 1 set:

  - `adx_14`: (0, 100)
  - `bb_width_pct`: (0, None)
  - `bb_width_pct_rank_60d`: (0, 1)
  - `dollar_volume_20d_avg`: (0, None)
  - `pct_from_52w_high`: (-100, None) *(can go positive on breakout)*
  - `pct_from_52w_low`: (-100, None)
  - `adr_pct`: (0, None)
  - `range_20d_pct`: (0, None)
  - `obv_slope_20d_norm`: (-50, 50) *(soft sanity check)*
  - `beta_60d`, `beta_252d`: (-10, 10) *(soft sanity check)*
  - `rvol_today` is an alias — no new entry.

View-derived fields don't need bounds (the view returns whatever the
expression yields; bounds belong on the inputs). Without these,
`validate_indicator()` returns the value unchecked — fine until a
divide-by-zero infinity sneaks in. Add as part of the Phase 1 PR.

### 5.6 Tests

Three test layers, in order of breadth:

1. **Unit tests with TA-Lib reference values** —
   `tests/calculation/test_ta_engine_extended.py`. For each new
   indicator: a small fixture price series with hand-computed (or
   independently-computed in a notebook) expected values. Includes
   edge cases: doji bar, day 1, NaN propagation, divide-by-zero.
2. **Property tests** — for monotonicity / range invariants
   (`bb_width_pct >= 0`, `0 <= close_pct_in_range <= 1`,
   `pct_from_52w_low >= -100`).
3. **Sample validation** — random-sample 50 tickers post-backfill,
   spot-check 5 fields each against an independent calculation.
   Sampled values become regression fixtures for layer 1.

Layer 1 is the gate for the Phase 1 PR; layers 2-3 can land in a
follow-up ticket.

### 5.7 Cascade and tombstoning

Schema policy across new tables:

| Table | `ON DELETE CASCADE` from `companies(ticker)` | Tombstone semantics |
|------|-------------------|---------------------|
| `technical_indicators` (existing) | yes | none — historical TA preserved as long as `companies` row exists |
| `technical_indicators_intraday` | yes | same |
| `ta_anchors` | yes | rows deleted when ticker removed |
| `ta_volume_profile` | yes | same |
| `rrg_relative_strength` | yes | same |
| `options_iv_cache` | yes | same |
| `sa_breakouts` | yes | same |

When Sawa adds a "soft delete / `is_active`" column to `companies`
(out of scope here), revisit — the project may want to **stop writing
new rows** for inactive tickers but **keep historical rows** for
research. Today there is no `is_active` so the question doesn't yet
apply.

### 5.8 Downstream consumer note

Adding ~57 columns to `technical_indicators` is a wider response from
the MCP server's `get_latest_technical_indicators` and
`screen_technical_indicators` tools. **No breaking changes** — new
columns appear NULL until the post-migration backfill completes; the
existing 25 columns are untouched. Mirror the
`docs/notes/vix_storage_migration_2026-05-13.md` pattern: ship a
short `docs/notes/ta_expansion_$(date).md` note when Phase 1 lands so
downstream consumers (notebooks, dashboards) know what to expect.

### 5.9 Out of scope (explicit non-goals)

The following are **not** delivered by this expansion. Naming them
prevents scope creep mid-implementation.

**Cut by Revision 2 (deferred until a written consumer specification
arrives):**

- Resistance-helper-derived fields: `gap_clears_resistance`,
  `levels_cleared`, `distance_to_next_resistance(_r)`,
  `next_resistance_source`. Restore when a System has a written
  resistance definition.
- `recent_swing_low_20d`, `days_below_sma50_last_20`,
  `higher_lows_count_last_10`. Restore when a scanner needs them.
- `reclaim_day_low` — replace with a `JOIN stock_prices` at scan time.
- `ta_anchors` table and the four anchor-detection heuristics —
  replaced by the runtime AVWAP MCP tool.
- `ta_volume_profile` table and the histogram algorithm — replaced by
  "build it as a runtime tool when a System needs it."
- `avwap_cluster_within_1pct` — depended on the (cut) anchor table.
- All six "RS / sector" derived flags collapsed to the 3 stored
  mirrors above + 3 view fields.

**Out of scope by design (TA-Lib catalog gaps):**

- Ichimoku, Keltner Channels, Aroon, Williams %R, Stochastic, CCI.
  Single TA-Lib calls — add in a separate "indicator catalogue
  expansion" PR after Phase 1, not here.
- Multi-timeframe TA (weekly / monthly aggregation). The MCP tool
  `get_multi_timeframe_alignment` already does this on read; nothing
  to materialize.
- Pattern detection (head & shoulders, cup & handle, etc.). Sawa has
  `detect_chart_patterns` as a runtime tool — keep it that way.
- Options Greeks beyond the four UW columns in Phase 4c.
- Alternative-data signals (insider buying, short interest, etc.)
  already exist via Polygon endpoints; not part of this expansion.
- Historical intraday backfill (REST replay). Out of scope.

---

## 6. Risks, open questions, decisions needed

### 6.1 Hard blockers (need a decision before Phase 1 starts)

Revision 2 resolves several Revision 1 blockers by cutting their
source. Only the items still relevant to the lean scope:

1. **Beta benchmark.** SPY (in our DB already). Assert SPY is present
   in `_init_worker` and abort the backfill if not.
2. **`bb_width_pct_rank_60d` warm-up.** First 80 rows NULL. Confirm
   acceptable.
3. **`rvol_today` is an alias, not a new column.** Locked: scanner
   filter identifier `rvol_today` maps to existing column
   `volume_ratio` via `mcp_server/tools/screener.py` `FILTERS` and a
   metadata row. No new column added.
4. **Adjusted-vs-unadjusted price contract.** Confirm both `open` and
   `close` in `stock_prices` come from the same adjustment regime so
   `gap_pct` and OBV / dollar-volume aren't distorted on split days.
   Verify in `sawa/database/load.py` before Phase 1.
5. **View vs stored line.** Confirm the §1.5 storage-tier philosophy
   is acceptable. If the team prefers "stored everywhere for scanner
   simplicity," several Revision 1 columns return to the wide table
   (and the Revision 1 column-count comes back).

**Resolved by Revision 2 cuts** (no longer needed):

- ~~Resistance "horizontal" definition.~~ Resistance helper cut.
- ~~`levels_cleared` storage shape (JSONB vs array).~~ Cut.
- ~~`reclaim_day_low` cut decision.~~ Cut.

### 6.2 Soft blockers (need a decision before the relevant phase)

1. **Phase 2 — premarket bars.** WS doesn't deliver them. Document
   `premarket_*` as NULL in Phase 2 and revisit later under a separate
   "premarket ingest" sub-project. Unchanged from Revision 1.
2. **Phase 3 — promote runtime AVWAP to a table?** After running for
   a quarter, if callers consistently want pre-materialized AVWAPs
   from the four canonical anchors (last earnings, YTD, 52w high,
   52w low), build `ta_anchors` then. Until then, the runtime tool is
   the answer.
3. **Phase 4c — UW rate limits.** Per-ticker call × ~5500 tickers in
   the post-close window. Confirm UW per-key rate limit; scope to
   optionable names if needed.
4. **Phase 5 — breakout-log ownership.** Confirm which scanner owns
   writes. There is no scanner module in the current Sawa codebase —
   likely lives in a downstream consumer. If so, Sawa just provides
   the table.

### 6.3 Operational risks

- **TA-Lib is required.** Phase 1 expands the TA-Lib surface (`ADX`).
  Already a hard dependency; no new install impact.
- **Backfill runtime.** ~7M rows × 60 columns. Row-by-row UPSERT
  (current code) makes this hours; `psycopg.copy` brings it to
  minutes. Phase 0 isn't optional if Phases 1–4 ship in a reasonable
  window.
- **Wide-table scan plans.** With 60 columns and 10+ BRIN indexes the
  Postgres planner can pick suboptimal scans for compound predicates.
  Plan a profiling pass after Phase 1: `EXPLAIN ANALYZE` a
  representative scanner query and add B-tree composites only where
  needed.

---

## 7. Reviewer checklist

Before approving Phase 1, confirm:

- [ ] **Storage tier philosophy (§1.5)** is acceptable. Specifically:
      most Group 3 event flags live in `v_technical_indicators`, not
      as stored columns; AVWAP is a runtime tool, not a table; volume
      profile is cut.
- [ ] Stored Phase 1 column count is ~18, not ~37 (Revision 1
      number).
- [ ] All Group 1–3 fields fit one of patterns A–L (§4) — no field
      requires a new pattern.
- [ ] Decisions 1–5 in §6.1 are made.
- [ ] Phase 0 (loader refactor + dataclass cleanup + validator
      registration helper) is scheduled before Phase 1, not after.
- [ ] Phase 1.5 (the view) ships in the same release as Phase 1; the
      view is the public read surface for scanners.
- [ ] Backfill plan in §5.1 (hot pass first, cold pass over a weekend)
      is acceptable.
- [ ] §5.3 metadata-population step is enforced in PR review — every
      stored `ADD COLUMN` has a matching `INSERT INTO
      technical_indicator_metadata`. View columns also get metadata
      rows so dynamic screeners surface them.
- [ ] §5.4 index plan is implemented in the same migration as the
      column additions, not deferred.
- [ ] §5.5 `MIN_PERIODS` and `INDICATOR_BOUNDS` updates land with
      Phase 1 (new fields will silently skip validation otherwise).
- [ ] §5.6 layer-1 unit tests gate the Phase 1 PR.
- [ ] §5.8 downstream-consumer note is drafted before the migration
      runs in production.
- [ ] §5.9 out-of-scope list is referenced in PR description to head
      off "while we're here, can we also add…" requests.
- [ ] `docs/DATA_SOURCES.md` §3 (computed/derived) and
      `sqlschema/README.md` will be updated as part of each phase, not
      as an afterthought.

---

## 9. Revision history

- **First draft.** Initial plan with 7 patterns (A–J), 60-column
  wide-table estimate, single Phase 4.
- **Revision 1.** Independent review pass folded in.
  Changes:
  - Formula corrections: `obv_slope_20d` → `obv_slope_20d_norm`
    (scale fix); `pct_from_52w_high/low` excludes today; `adr_pct`
    switched to IBD `(mean(h/l)-1)*100`; `holds_vwap_after_reclaim`
    tolerates 1-bar wick; RS formula split into ratio vs RS-line.
  - Pattern reassignments: `close_crosses_above_pivot_20_60d` is
    C+H not H+I; `close_pct_in_range` is B not G;
    `reclaim_day_low` is a pass-through (recommended cut).
  - Added Patterns K (anchored cumulative) and L (histogram-based)
    that the first draft used but never named.
  - Edge cases specified for divide-by-zero on `pct_from_52w_low`,
    `volume_trend_base_20d`, `vwap_intraday`, AVWAP, doji bars.
  - Storage decisions: `levels_cleared` switched to JSONB;
    explicit cascade policy; explicit index plan.
  - New sections: §5.3 metadata population, §5.4 index plan, §5.5
    validator updates, §5.6 tests, §5.7 cascade, §5.8 downstream
    note, §5.9 explicit non-goals.
  - Phase 4 split into 4a (RRG) and 4b (UW options).
  - Phase 3 prerequisite on Phase 2 history accumulation made
    explicit.
  - Column-count estimate corrected from "~60" to "~82".
  - 7 hard blockers in §6.1 (was 4); added items 5–7 (storage
    shape, `reclaim_day_low` cut, adjusted-price contract).
- **Revision 2 (this version).** Lean rewrite in response to
  reviewer concern that the column count was too large and AVWAP
  anchor selection was under-specified. Changes:
  - **Storage tier philosophy (§1.5)** introduced. Three tiers:
    Stored / View / Runtime. Most Group 3 event flags moved to View.
  - **Wide-table column count: ~57 → ~20** new columns (final 25 → 45).
  - **AVWAP delivered as a runtime MCP tool** (§3.5.1), not a
    pre-materialized `ta_anchors` table. Caller picks the anchor.
    Four canonical anchors documented for callers (last earnings,
    YTD, 52w high, 52w low) but not enforced by Sawa.
  - **Volume profile cut entirely.** No table, no histogram. Restore
    when System 4 has a written spec with concrete parameters.
  - **Resistance-helper fields cut entirely** (§3.7) — five Group 3
    columns deleted plus the helper module itself.
  - **Group 2 trivial booleans (`sma_150_above_sma_200`,
    `sma_200_rising`) moved to View.**
  - **Group 6 RS mirror columns: 6 → 3** (`rs_ratio_21d`,
    `rs_ratio_63d`, `rs_line_at_60d_high`); the rest are view fields.
  - **Phase 4 split into 4a (RRG), 4b (PEAD), 4c (UW options)** —
    three independent dependency chains.
  - **New Phase 1.5** ships the view in the same release as Phase 1.
  - §5.9 non-goals now lists every Revision-2 cut explicitly.
  - §6.1 hard blockers reduced from 7 to 5; resolved-by-cut items
    moved to a struck-through list.
