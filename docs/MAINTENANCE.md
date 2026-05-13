# Sawa Maintenance Guide

This is the doc to read when you come back to the project after weeks/months
away and need to remember **how it actually works** before changing
anything. It is intentionally self-contained — you should not need to read
code to recover the mental model.

Companion docs:
- [`README.md`](../README.md) — short user-facing intro
- [`docs/OPERATIONS.md`](OPERATIONS.md) — day-to-day operational reference
- [`sqlschema/README.md`](../sqlschema/README.md) — schema file catalogue
- [`docs/STOCK_CHARACTER.md`](STOCK_CHARACTER.md) — build spec for the
  weekly classification batch

---

## 1. What Sawa is

Sawa has two halves that share a single PostgreSQL database:

```
┌──────────────────────────┐        ┌──────────────────────────────┐
│   sawa CLI (data ETL)    │        │  stock-data MCP server       │
│   - coldstart / daily /  │  ───>  │  - 60+ MCP tools for AI      │
│     weekly / quarterly   │   DB   │    clients (Claude, etc.)    │
│   - intraday WebSocket   │        │  - read-only queries + views │
└──────────────────────────┘        └──────────────────────────────┘
        │
        ├── Polygon REST   (prices, fundamentals, news, splits, dividends)
        ├── Polygon S3     (bulk price history)
        ├── Polygon WS     (live 5-min bars, 15-min delayed)
        └── FRED API       (market internals: VIX, VIX3M, HY spread)
```

The CLI is the writer. The MCP server is the reader. They share PostgreSQL.

## 2. The four pipeline commands

Each command lives in its own module under `sawa/` and is invoked by
`sawa <command>` (entry point: `sawa.cli:main` in `pyproject.toml`).

### `sawa coldstart` → `sawa/coldstart.py`

Full bootstrap. Does these things in order:

1. (optional, with `--drop`) `drop_all_tables`
2. Apply every `sqlschema/NN_*.sql` in numeric order
3. Build the universe: union of S&P 500 (scraped from Wikipedia) and the
   NASDAQ list in `data/nasdaq1000_symbols.txt` (~5000 tickers despite the
   name)
4. Download into `data/`:
   - `data/prices/<TICKER>.csv` via Polygon S3 (5y by default)
   - `data/overviews/overviews.csv` via Polygon REST
   - `data/fundamentals/{balance_sheets,income_statements,cash_flow}.csv`
   - `data/ratios/ratios.csv`
   - `data/economy/*.csv`
   - `data/economy/market_internals.csv` (FRED — needs `FRED_API_KEY`)
   - News articles (last 30 days)
5. Load each CSV into PostgreSQL via `sawa/database/load.py` (upserts)
6. Mirror VIX rows from `market_internals` into `stock_prices` for chart
   tooling (`mirror_vix_to_stock_prices`)

Flags worth remembering:
- `--no-drop` — re-apply schema without losing data (safe upgrade path
  when only the SQL changed)
- `--schema-only` — DDL only, no downloads
- `--load-only` / `--skip-downloads` — use existing CSVs in `data/`
- `--drop-only --confirm-drop` — purely destructive

### `sawa daily` → `sawa/daily.py`

Runs after market close. Steps:

1. Read latest stored date from `stock_prices`
2. Pull aggregates from Polygon REST for missing dates
3. Upsert into `stock_prices` (key: ticker, date)
4. Fetch news (`fetch_and_load_news`) — last `DEFAULT_NEWS_DAYS` of articles
5. Recompute technical indicators incrementally
   (`sawa/database/ta_load.py`)
6. Pull FRED market internals → `market_internals` table, mirror VIX into
   `stock_prices`

Skips: `--skip-news`, `--skip-ta`, `--skip-market-internals`,
`--news-only`. `--from-date YYYY-MM-DD` replays from a date forward.

Missing API keys (Polygon or FRED) trigger `alert_missing_api_key` — the
relevant step is skipped, a notification is sent if `NTFY_TOPIC` is set,
and the overall job still exits 0 for the other steps. (See
`b59f8a6 feat: alert on missing API keys, deprecate Polygon VIX path`.)

### `sawa weekly` → `sawa/weekly.py`

Slow-moving data:

1. Economy: treasury yields, CPI/PCE, inflation expectations, labor market
2. Company overviews — re-pulls ticker details (market cap, SIC, etc.)
3. News (same loader as daily)
4. Corporate actions: stock splits, dividends, earnings via
   `sawa/corporate_actions.py`
5. Stock character classification — see
   [`docs/STOCK_CHARACTER.md`](STOCK_CHARACTER.md). Runs via
   `sawa/stock_character_batch.py`, parallelised by `--character-workers`.

Skips: `--skip-economy`, `--skip-overviews`, `--skip-news`,
`--skip-corporate-actions`, `--skip-character`.

### `sawa quarterly` → `sawa/quarterly.py`

1. Balance sheets, income statements, cash flows (Polygon REST)
2. Financial ratios

Slow but rarely changes. Run by hand after a quarter ends, or as a cron
once a quarter.

### `sawa intraday` → `sawa/intraday.py` + `sawa/live.py`

Polygon WebSocket subscription for live 5-min bars (15-min delayed on the
basic tier). Writes to `stock_prices_intraday`. Designed to be started at
the open and killed at the close — `scripts/market_scheduler.sh` does this
automatically.

## 3. The unattended scheduler

`scripts/market_scheduler.sh` is a single bash file that handles
everything. Install it as a single cron entry that fires every 15 minutes
on weekdays:

```cron
*/15 * * * 1-5 /path/to/sawa/scripts/market_scheduler.sh >> ~/.sawa/scheduler/cron.log 2>&1
```

On each tick it:

1. Acquires a flock so two instances don't overlap
2. Checks `https://api.polygon.io/v1/marketstatus/now` (fallback: ET clock)
3. Market open → start `sawa intraday` if not already running
4. Market closed → stop `sawa intraday`, then:
   - If `>= 17:00 ET` and daily hasn't run today, run `sawa daily`
   - If Saturday and weekly hasn't run this ISO week, run `sawa weekly`
5. Sends start/stop/failure push notifications to `NTFY_TOPIC`

State lives in `~/.sawa/scheduler/`:
- `intraday.pid`, `intraday.log`, `intraday_{start,stop}_time`
- `daily_done_YYYY-MM-DD` (cleaned after 7 days)
- `weekly_done_YYYY-WNN` (cleaned after 60 days)
- `scheduler.log` (trimmed to 5000 lines when it exceeds 10000)

The `scripts/{daily,weekly,coldstart}.sh` wrappers are simpler — they just
activate the venv and run one `sawa` command. Use those if you want
separate cron entries instead of the unified scheduler.

## 4. The database

PostgreSQL only. Schema is fully captured by the files in `sqlschema/`,
applied in numeric prefix order. See
[`sqlschema/README.md`](../sqlschema/README.md) for the per-file catalogue.

**Adding a migration.** Pick the next free `NN_*.sql`, write idempotent
DDL (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`,
`DROP ... IF EXISTS`), and re-apply with `sawa coldstart --no-drop`. If
the migration adds a new table, also add it to `EXPECTED_TABLES` in
`sawa/database/schema.py` (otherwise the runner reports it missing).

**Loaders.** Each table has a dedicated loader function in
`sawa/database/load.py` (and `news.py`, `intraday_load.py`,
`ta_load.py`, `stock_character.py`). They are pure UPSERTs keyed on the
PK declared in the schema. To bulk-reload a CSV after a manual fix-up,
use the standalone utility:

```bash
python -m sawa.database.loader --mapping data/data_mappings.json
python -m sawa.database.loader --csv data/economy/inflation.csv \
    --table inflation --columns date,cpi,cpi_core --upsert
```

`data/data_mappings.json` maps logical table names to CSV path + columns;
it is **not** used by the daily/weekly/quarterly pipeline (those use
`load.py` directly). It exists for manual reloads.

## 5. The MCP server

Lives in `mcp_server/`. The entry point is `mcp_server.server`
(see `mcp_server/run.sh`):

```bash
python -m mcp_server.server
```

It is read-only against the same database. Tools are grouped under
`mcp_server/tools/` (companies, market_data, fundamentals, economy,
indices, momentum, movers, corporate_actions). The server registers each
tool in `server.py`; `validate_new_tools.py` runs a static sanity check.

Connecting an AI client: add the MCP entry as shown in the
[`README.md`](../README.md). The server reads `.env` from the project
root via `_project_root / ".env"` so MCP clients only need to pass
`DATABASE_URL` if they don't share that file.

## 6. Data on disk

```
data/
  nasdaq1000_symbols.txt        # NASDAQ universe (~5000 tickers, bundled into the wheel)
  data_mappings.json            # Optional CSV→table mapping for sawa.database.loader
  prices/<TICKER>.csv           # Daily OHLCV per ticker (coldstart output)
  fundamentals/                 # *_update.csv files are weekly deltas
  overviews/overviews.csv
  ratios/ratios.csv
  economy/
    treasury_yields.csv
    inflation.csv
    inflation_expectations.csv
    labor_market.csv
    market_internals.csv        # VIX, VIX3M, HY spread (FRED)
```

`data/` is the working directory for ETL output. The pipeline always
re-reads from PostgreSQL — these CSVs are intermediate caches, safe to
delete and regenerate. The exception is `data/nasdaq1000_symbols.txt`,
which is the **source of truth** for the NASDAQ universe and is also
shipped inside the installed wheel (see
`[tool.hatch.build.targets.wheel.force-include]` in `pyproject.toml`).

## 7. Environment variables

| Variable | Required | Used by |
|----------|----------|---------|
| `POLYGON_API_KEY` | yes | REST: prices, fundamentals, news, splits, dividends |
| `POLYGON_S3_ACCESS_KEY` / `POLYGON_S3_SECRET_KEY` | yes | Bulk history via S3 |
| `FRED_API_KEY` | yes | `market_internals` step in daily/weekly/coldstart |
| `DATABASE_URL` | yes | Everything |
| `NTFY_TOPIC` | no | Pipeline + scheduler push notifications |
| `MCP_LOG_LEVEL` / `MCP_MAX_ROWS` / `MCP_QUERY_TIMEOUT` | no | MCP server runtime |

## 8. Common maintenance tasks

### "I want to add a ticker"

```bash
sawa add-symbol PLTR COIN          # ad-hoc
sawa add-symbol --file new.txt --years 5
```

Then if the ticker should belong to `nasdaq5000`:

```bash
echo PLTR >> data/nasdaq1000_symbols.txt
python scripts/populate_nasdaq5000.py data/nasdaq1000_symbols.txt
```

### "Prices look stale after a split"

```bash
sawa adjust-splits                 # auto-detect recent splits
sawa adjust-splits --ticker XYZ    # force a specific ticker
```

This re-fetches split-adjusted history from Polygon.

### "I changed an SQL file"

```bash
sawa coldstart --no-drop           # re-applies all SQL idempotently
```

If the change is destructive (drop column, change PK), use a new
numbered migration file instead of editing an old one.

### "Technical indicators are wrong / missing"

```bash
sawa ta-backfill --workers 8       # rebuild for all tickers
sawa ta-backfill --ticker AAPL     # single ticker
sawa ta-show AAPL                  # spot-check
```

### "Where did the latest data run get to?"

```bash
sawa data-status                   # latest date per price table
tail -n 200 logs/daily_*.log       # most recent daily run
tail -n 200 ~/.sawa/scheduler/scheduler.log   # if using market_scheduler.sh
```

### "Cron didn't fire / the scheduler is silent"

1. Confirm cron is installed and the entry exists: `crontab -l`
2. Check `~/.sawa/scheduler/cron.log` for cron's stderr
3. Check `~/.sawa/scheduler/scheduler.lock` — stale lock from a crashed
   run? `rm` it
4. Check `~/.sawa/scheduler/scheduler.log` for the per-tick decision log

## 9. Where things live in code

| Concern | Module |
|---------|--------|
| CLI entry & arg parsing | `sawa/cli.py` |
| Pipeline commands | `sawa/{coldstart,daily,weekly,quarterly,intraday}.py` |
| Polygon clients | `sawa/api/{client,async_client,s3,websocket_client}.py` |
| FRED client | `sawa/api/fred.py` |
| Schema runner | `sawa/database/schema.py` |
| Loaders (pipeline) | `sawa/database/{load,news,intraday_load,ta_load,stock_character}.py` |
| Bulk loader (manual) | `sawa/database/loader.py` |
| Technical indicators | `sawa/calculation/`, `sawa/ta_backfill.py`, `sawa/ta_query.py` |
| Stock character batch | `sawa/stock_character_batch.py` (spec: `docs/STOCK_CHARACTER.md`) |
| Corporate actions | `sawa/corporate_actions.py`, `sawa/split_adjust.py` |
| Rate limiting | `sawa/repositories/rate_limiter.py` |
| Notifications | `sawa/utils/notify.py` (`NTFY_TOPIC`) |
| MCP server | `mcp_server/server.py` |
| MCP tools | `mcp_server/tools/*.py` |
| MCP read-only DB layer | `mcp_server/database.py`, `mcp_server/services/` |
| Schema files | `sqlschema/NN_*.sql` |
| Cron scheduler | `scripts/market_scheduler.sh` |
| Ad-hoc backfills | `scripts/backfill_market_internals.py`, `scripts/populate_*.py`, `scripts/populate_earnings.py` |

## 10. Tests

```bash
pytest                              # full suite
pytest tests/test_resources.py -q   # quick sanity check
pytest --cov=sawa
ruff check .
mypy sawa/
mypy mcp_server/
```

`tests/` mirrors the package layout. Database-touching tests use a
disposable schema — they will not touch your production DB if
`DATABASE_URL` points elsewhere, but in general run tests against an
empty / scratch database.
