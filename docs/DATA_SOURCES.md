# Data Sources

Canonical mapping of every external data source Sawa pulls from to the
PostgreSQL table(s) it populates, the loader module that handles it, and
the pipeline command(s) that trigger it.

If you are wondering *"where does the data in table X come from?"* — this is
the document to read.

Companion docs:
- [`README.md`](../README.md) — project intro
- [`MAINTENANCE.md`](MAINTENANCE.md) — system-level architecture
- [`OPERATIONS.md`](OPERATIONS.md) — day-to-day operational reference
- [`../sqlschema/README.md`](../sqlschema/README.md) — schema file catalogue

---

## 1. At a glance

```
┌────────────────────────┐
│ Polygon REST           │── prices, fundamentals, ratios, news, splits,
│ api.polygon.io         │   dividends, ticker details, treasury yields,
│ POLYGON_API_KEY        │   inflation, labor market
└────────────────────────┘
┌────────────────────────┐
│ Polygon S3 flatfiles   │── bulk historical daily OHLCV
│ files.polygon.io       │   (coldstart only — much faster than REST
│ POLYGON_S3_*           │   for multi-year backfills)
└────────────────────────┘
┌────────────────────────┐
│ Polygon WebSocket      │── live 5-min bars during market hours
│ delayed.polygon.io     │   (15-minute delayed on the basic tier)
│ POLYGON_API_KEY        │
└────────────────────────┘
┌────────────────────────┐
│ FRED                   │── market internals: VIX, VIX3M, HY spread
│ api.stlouisfed.org     │   (only data source for these series since
│ FRED_API_KEY           │   commit 2d4e350; Polygon VIX path retired)
└────────────────────────┘
┌────────────────────────┐
│ Wikipedia              │── S&P 500 constituent list
│ en.wikipedia.org       │   (HTML scrape; no auth)
└────────────────────────┘
┌────────────────────────┐
│ Bundled file           │── NASDAQ-5000 constituent list
│ data/nasdaq1000_       │   (~5000 tickers despite the filename;
│   symbols.txt          │   shipped inside the wheel)
└────────────────────────┘
┌────────────────────────┐
│ yfinance (optional)    │── earnings dates backfill
│ scripts/populate_      │   (standalone script, NOT in the auto
│   earnings.py          │   pipeline — Polygon's earnings endpoint
│                        │   currently returns no useful data)
└────────────────────────┘
```

The CLI always writes; the MCP server only reads. All loaders are
idempotent UPSERTs — re-running any pipeline command is safe.

---

## 2. By external source

### 2.1 Polygon REST — `https://api.polygon.io`

Auth: `POLYGON_API_KEY` (Bearer token).
Client: `sawa/api/client.py` (`PolygonClient`).
Rate limiting: `sawa/repositories/rate_limiter.py` (default 5 req/s).

| Endpoint | Populates | Pipeline command(s) | Loader |
|----------|-----------|---------------------|--------|
| `/v2/aggs/ticker/{t}/range/1/day/...` | `stock_prices` | `daily`, `add-symbol`, `adjust-splits` | `sawa/database/load.py` |
| `/v2/aggs/ticker/{t}/range/{m}/{ts}/...` | `stock_prices_intraday` (REST fallback) | `intraday` (rare; usually WS) | `sawa/database/intraday_load.py` |
| `/v3/reference/tickers/{t}` | `companies` | `coldstart`, `weekly`, `add-symbol` | `sawa/database/load.py` |
| `/stocks/financials/v1/ratios` | `financial_ratios` | `coldstart`, `quarterly` | `sawa/database/load.py` |
| `/stocks/financials/v1/balance-sheets` | `balance_sheets` | `coldstart`, `quarterly` | `sawa/database/load.py` |
| `/stocks/financials/v1/income-statements` | `income_statements` | `coldstart`, `quarterly` | `sawa/database/load.py` |
| `/stocks/financials/v1/cash-flow-statements` | `cash_flows` | `coldstart`, `quarterly` | `sawa/database/load.py` |
| `/v3/reference/splits` | `stock_splits` | `coldstart`, `weekly` | `sawa/corporate_actions.py` |
| `/v3/reference/dividends` | `dividends` | `coldstart`, `weekly` | `sawa/corporate_actions.py` |
| `/vX/reference/tickers/{t}/events` | `earnings` (opt-in) | `corporate-actions --include-earnings` only | `sawa/corporate_actions.py` |
| `/v2/reference/news` | `news_articles`, `news_article_tickers`, `news_sentiment` | `coldstart`, `daily`, `weekly` | `sawa/database/news.py` |
| `/fed/v1/treasury-yields` | `treasury_yields` | `coldstart`, `weekly` | `sawa/database/load.py` |
| `/fed/v1/inflation` | `inflation` | `coldstart`, `weekly` | `sawa/database/load.py` |
| `/fed/v1/inflation-expectations` | `inflation_expectations` | `coldstart`, `weekly` | `sawa/database/load.py` |
| `/fed/v1/labor-market` | `labor_market` | `coldstart`, `weekly` | `sawa/database/load.py` |

Notes:
- **News sentiment is provided by Polygon**, not computed locally. The
  per-ticker sentiment + reasoning lives in the article's `insights`
  field and is unpacked by `sawa/database/news.py`.
- **Earnings via Polygon is currently inert.** The `ticker-events`
  endpoint returns only `ticker_change` events, not earnings dates. The
  `weekly` pipeline does NOT request earnings (`include_earnings=False`).
  To populate the `earnings` table, run `scripts/populate_earnings.py`
  (yfinance) — see §2.7.
- The `/fed/v1/...` family is hosted by Polygon, not FRED. FRED handles
  only the market-internals series (§2.4).

### 2.2 Polygon S3 flatfiles — `s3://flatfiles/...`

Auth: `POLYGON_S3_ACCESS_KEY` + `POLYGON_S3_SECRET_KEY`.
Client: `sawa/api/s3.py` (`PolygonS3Client`).

| Object key | Populates | Pipeline command | Loader |
|-----------|-----------|------------------|--------|
| `us_stocks_sip/day_aggs_v1/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz` | `stock_prices` (bulk historical) | `coldstart`, `add-symbol --years N` | `sawa/database/load.py` |

S3 is used only for the historical bulk download path — coldstart fans
out one S3 GET per trading day across the requested year range, which is
dramatically faster than the per-ticker REST aggregates endpoint. After
coldstart, `sawa daily` uses REST for incremental updates.

### 2.3 Polygon WebSocket — `wss://delayed.polygon.io/stocks`

Auth: `POLYGON_API_KEY`.
Client: `sawa/api/websocket_client.py`.

| Channel | Populates | Pipeline command | Loader |
|---------|-----------|------------------|--------|
| `AM.*` (aggregate-minute → 5-min bars) | `stock_prices_intraday` | `intraday` | `sawa/database/intraday_load.py` |

15-minute delayed on Polygon's basic tier. Designed to be started at the
open and killed at the close — `scripts/market_scheduler.sh` does this
automatically.

### 2.4 FRED — `https://api.stlouisfed.org/fred/series/observations`

Auth: `FRED_API_KEY`.
Client: `sawa/api/fred.py` (`FredClient`).

| Series ID | Field | Populates | Pipeline command(s) |
|-----------|-------|-----------|---------------------|
| `VIXCLS` | `vix` | `market_internals` | `coldstart`, `daily`, `weekly` |
| `VXVCLS` | `vix3m` | `market_internals` | `coldstart`, `daily`, `weekly` |
| `BAMLH0A0HYM2` | `hy_spread` | `market_internals` | `coldstart`, `daily`, `weekly` |

Loader: `sawa/database/load.py::load_market_internals` (UPSERT keyed on
`date`).

VIX and VIX3M are sourced *exclusively* from FRED since commit `2d4e350`
("refactor: consolidate VIX to single source in market_internals",
2026-05-13). They are no longer mirrored into `stock_prices` or
`companies`. See [`VIX_MIGRATION.md`](VIX_MIGRATION.md) for the migration
note.

If `FRED_API_KEY` is unset, `daily`/`weekly`/`coldstart` log a warning,
send an ntfy alert if `NTFY_TOPIC` is set, skip the market-internals
step, and continue. They do not fail.

### 2.5 Wikipedia — S&P 500 constituents

URL: `https://en.wikipedia.org/wiki/List_of_S%26P_500_companies`.
Function: `sawa/utils/symbols.py::fetch_sp500_symbols` (HTML table scrape
via `pandas.read_html`).

| Source | Populates | Pipeline command(s) |
|--------|-----------|---------------------|
| Wikipedia HTML table (column "Symbol") | `index_constituents` (rows where `index_id` = `sp500`), and contributes to the union that becomes `companies` | `coldstart`, `index-update` |

No auth, no rate limit. If Wikipedia changes its table layout, the
scrape may break — `sawa index-update` is the recovery path.

### 2.6 Bundled file — NASDAQ-listed universe (fallback)

Path: `data/nasdaq1000_symbols.txt` (also force-included into the built
wheel; see `pyproject.toml`'s
`[tool.hatch.build.targets.wheel.force-include]`).
Function: `sawa/utils/symbols.py::fetch_nasdaq_listed_symbols`.

| Source | Populates | Pipeline command(s) |
|--------|-----------|---------------------|
| Polygon `/v3/reference/tickers?exchange=XNAS&active=true` (primary) | `index_constituents` (rows where `index_id` = `nasdaq_listed`); contributes to the union that becomes `companies` | `coldstart`, `index-update` |
| `data/nasdaq1000_symbols.txt` (~5000 tickers, fallback only) | same | only when the Polygon path is unreachable |

The filename says "1000" for historical reasons; the file actually
contains ~5000 NASDAQ-listed tickers. The bundled file is a 2021-era
snapshot kept as a recovery fallback — the live Polygon path is the
source of truth. To refresh `nasdaq_listed` membership, run
`sawa index-update`.

### 2.7 yfinance — optional earnings backfill

Library: `yfinance`.
Script: `scripts/populate_earnings.py` (NOT a `sawa <command>`).

| Source | Populates | Trigger |
|--------|-----------|---------|
| `yfinance.Ticker(t).earnings_dates` | `earnings` | manual: `python scripts/populate_earnings.py` |

This exists because Polygon's `ticker-events` endpoint currently does
not return earnings dates — the auto pipeline therefore leaves the
`earnings` table empty. Run this script periodically (e.g. once a
quarter after earnings season) to populate it.

---

## 3. Computed / derived tables

These tables are populated by code that runs against data already in the
database. They have no external source.

| Table(s) | Inputs | Compute module | Pipeline command(s) |
|----------|--------|----------------|---------------------|
| `technical_indicators` | `stock_prices` OHLCV | TA-Lib via `sawa/calculation/`, `sawa/ta_backfill.py` | `coldstart` (full backfill), `daily` (incremental), `add-symbol`, `ta-backfill` |
| `stock_character_classification`, `stock_character_baseline`, `stock_character_flags`, `stock_character_scorecard` | `stock_prices`, `technical_indicators`, `financial_ratios`, fundamentals | `sawa/calculation/stock_character*.py`, `sawa/stock_character_batch.py` | `weekly` (default), `character` | 
| `mv_52week_extremes` (materialized view) | `stock_prices` | SQL in `sqlschema/14_52week_extremes.sql` | refreshed by `daily`; manual `REFRESH MATERIALIZED VIEW` |
| All views in `sqlschema/06_views.sql` and `22_views_advanced.sql` | base tables | SQL views | computed on read |
| `trader_cards` | parsed analysis output from chart-analysis card.md files | manual / external tooling | not in the auto pipeline |

See [`STOCK_CHARACTER.md`](STOCK_CHARACTER.md) for the spec of the
character classification batch.

---

## 4. Source for each table — quick lookup

Reverse index of §2 and §3.

| Table | Source |
|-------|--------|
| `companies` | Polygon REST `/v3/reference/tickers/{t}` (overviews); ticker membership union of Wikipedia (S&P 500) + Polygon NASDAQ-listed fetch |
| `stock_prices` | Polygon S3 (bulk historical) + Polygon REST `/v2/aggs/.../day/...` (incremental) |
| `stock_prices_intraday` | Polygon WebSocket (live 5-min bars); REST `/v2/aggs/.../{m}/{ts}/...` for backfill |
| `financial_ratios` | Polygon REST `/stocks/financials/v1/ratios` |
| `balance_sheets` | Polygon REST `/stocks/financials/v1/balance-sheets` |
| `income_statements` | Polygon REST `/stocks/financials/v1/income-statements` |
| `cash_flows` | Polygon REST `/stocks/financials/v1/cash-flow-statements` |
| `treasury_yields` | Polygon REST `/fed/v1/treasury-yields` |
| `inflation` | Polygon REST `/fed/v1/inflation` |
| `inflation_expectations` | Polygon REST `/fed/v1/inflation-expectations` |
| `labor_market` | Polygon REST `/fed/v1/labor-market` |
| `market_internals` | FRED (`VIXCLS`, `VXVCLS`, `BAMLH0A0HYM2`) — sole source since commit `2d4e350` |
| `indices` | seed data in `sqlschema/12_indices.sql` |
| `index_constituents` | Wikipedia (`sp500`) + Polygon (`nasdaq_listed`, `us_active`) |
| `stock_splits` | Polygon REST `/v3/reference/splits` |
| `dividends` | Polygon REST `/v3/reference/dividends` |
| `earnings` | yfinance via `scripts/populate_earnings.py` (manual). Polygon `ticker-events` is wired up but currently returns no earnings data. |
| `news_articles`, `news_article_tickers`, `news_sentiment` | Polygon REST `/v2/reference/news` (sentiment provided by Polygon) |
| `technical_indicators` | computed locally from `stock_prices` via TA-Lib |
| `stock_character_*` | computed locally from `stock_prices` + `technical_indicators` + fundamentals |
| `mv_52week_extremes` | computed locally from `stock_prices` |
| `sic_gics_mapping` | seed data in `sqlschema/09_sic_gics_data.sql` |
| `trader_cards` | parsed externally from chart-analysis card.md files |

---

## 5. Required vs. optional credentials

| Variable | Required for | Effect if missing |
|----------|--------------|-------------------|
| `POLYGON_API_KEY` | Almost everything (REST + WS) | Pipeline alerts (ntfy if `NTFY_TOPIC` set), skips the affected step, continues |
| `POLYGON_S3_ACCESS_KEY` / `POLYGON_S3_SECRET_KEY` | `coldstart` bulk price download | Coldstart bulk price step fails — REST fallback is much slower for multi-year history |
| `FRED_API_KEY` | `market_internals` step | Step is skipped with an alert; pipeline still exits 0 |
| `DATABASE_URL` | Everything | Hard failure |
| `NTFY_TOPIC` | nothing (optional) | No push notifications; logging continues normally |
