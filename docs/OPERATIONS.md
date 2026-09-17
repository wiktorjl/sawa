# Operations Guide

Operational reference for the Sawa data pipeline. For a higher-level overview
see [MAINTENANCE.md](MAINTENANCE.md); for project intro see the top-level
[`README.md`](../README.md). For the per-table mapping of external data
sources (which API populates which table) see
[DATA_SOURCES.md](DATA_SOURCES.md).

## Prerequisites

### Environment Variables

Set these in your shell or `.env` file (copy `.env.example`):

```bash
POLYGON_API_KEY=...              # Polygon REST
POLYGON_S3_ACCESS_KEY=...        # Polygon S3 (bulk history)
POLYGON_S3_SECRET_KEY=...
FRED_API_KEY=...                 # FRED — market internals (VIX, VIX3M, HY spread)
DATABASE_URL=postgresql://user:pass@host:5432/dbname
NTFY_TOPIC=https://ntfy.sh/...   # optional; pipeline + scheduler push notifications
SAWA_HEARTBEAT_URL=https://...   # optional; dead-man's switches, see Monitoring
SAWA_WEEKLY_HEARTBEAT_URL=https://...
SAWA_TICK_HEARTBEAT_URL=https://...
SAWA_WATCHDOG_HEARTBEAT_URL=https://...
```

`scripts/market_scheduler.sh` exports only an allowlisted set of keys from
`.env` to the jobs it runs (`setup_env`). An unknown key is ignored — logged on
every tick and pushed as one warning — and a process-control name (`PATH`,
`PYTHONPATH`, `LD_PRELOAD`, ...) aborts the tick with an error push. Add new
keys to that allowlist (and to `.env.example`, which a test cross-checks)
before relying on them from a scheduled job.

Missing API keys behave differently by key:

- `POLYGON_API_KEY` (or `--api-key`) is required up front. If it is missing,
  `daily`/`weekly`/`quarterly`/`coldstart` log an error and exit non-zero
  before doing any work — Polygon underpins almost every step.
- `FRED_API_KEY` is optional. If it is missing, only the FRED market-internals
  step is skipped: the job logs an error, sends an ntfy alert (if `NTFY_TOPIC`
  is set) via `alert_missing_api_key`, and still exits 0.

### Database

PostgreSQL 12+ (14+ recommended). Create the database:

```bash
createdb sp500_data   # name is arbitrary; match DATABASE_URL
```

### Python Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cd mcp_server && pip install -e ".[dev]" && cd ..
```

TA-Lib needs the C library: `brew install ta-lib` (macOS) or
`apt install libta-lib-dev` (Ubuntu).

## Command Overview

| Command | Purpose | Frequency |
|---------|---------|-----------|
| `sawa coldstart` | Full database setup / rebuild | Once, then on schema rebuilds |
| `sawa daily` | Prices, news, TA, market internals | Daily after market close |
| `sawa weekly` | Economy, overviews, news, corporate actions, character | Weekly |
| `sawa quarterly` | Fundamentals + financial ratios | Quarterly |
| `sawa intraday` | WebSocket 5-min bars (15-min delayed) | During market hours |
| `sawa doctor` | Database sanity/completeness checks after jobs | After scheduled jobs |
| `sawa add-symbol` | Add new ticker(s) ad-hoc | As needed |
| `sawa adjust-splits` | Re-fetch adjusted prices after splits | After known split |
| `sawa ta-backfill` | Recompute technical indicators from history | After schema/code change |
| `sawa character` | Stock character classification (also runs in weekly) | As needed |
| `sawa data-status` | Show data freshness | Diagnostic |

All commands accept `--log-dir logs --verbose`.

## Coldstart Procedure

Use when:
- Setting up a new database
- Rebuilding after destructive schema changes
- Starting fresh after data corruption

```bash
sawa coldstart --years 5                   # Full bootstrap (5y of data)
sawa coldstart --years 5 --log-dir logs
sawa coldstart --schema-only               # DANGER: drops/recreates all tables; use throwaway DB
sawa coldstart --no-drop                   # Re-apply schema without dropping (safe upgrade)
sawa coldstart --load-only                 # Load already-downloaded CSVs only
sawa coldstart --skip-downloads            # Schema + load existing CSVs
sawa coldstart --drop-only --confirm-drop  # Destructive: drop everything
```

The universe is the union of S&P 500 (scraped from Wikipedia) and
NASDAQ-5000 (loaded from `data/nasdaq1000_symbols.txt`; despite the filename
the list contains ~5000 NASDAQ-listed tickers).

## Daily Update

Run after market close. Pulls prices, news, technical indicators, and market
internals (FRED). Safe to re-run — all upserts.

```bash
sawa daily
sawa daily --log-dir logs --verbose
sawa daily --dry-run                       # Preview only
sawa daily --from-date 2024-01-15          # Force replay from date
sawa daily --skip-news                     # Prices + TA only
sawa daily --skip-ta                       # Prices + news only
sawa daily --skip-market-internals         # Skip FRED step
sawa daily --news-only                     # Only update news
```

After a successful scheduled daily run, `sawa doctor --job daily` checks the
database before the scheduler marks the day complete. It verifies required
tables/views, active-company counts, latest `stock_prices` recency, latest-day
ticker coverage against recent populated price dates, OHLCV sanity, technical
indicator coverage, news/market internals freshness, and 52-week
materialized-view freshness.

## Weekly Update

```bash
sawa weekly
sawa weekly --skip-news --skip-overviews
sawa weekly --skip-corporate-actions
sawa weekly --skip-character               # Skip character classification batch
sawa weekly --character-workers 8
sawa weekly --dry-run
```

Updates:
- Economy: treasury yields, CPI/PCE, inflation expectations, labor market
- Company overviews
- News articles
- Corporate actions (splits, dividends)
- Stock character classification (Hurst-based regime classification)

After a successful scheduled weekly run, `sawa doctor --job weekly` checks the
database before the scheduler marks the ISO week complete. It validates core
price coverage plus economy table freshness, stock-character coverage, and
corporate-action table readability.

## Quarterly Update

```bash
sawa quarterly
sawa quarterly --skip-fundamentals
sawa quarterly --skip-ratios
```

Pulls balance sheets, income statements, cash flows, and financial ratios.

## Scheduling

### Recommended: `scripts/market_scheduler.sh`

A single cron entry handles intraday streaming during market hours, runs
`daily` ~1h after close, and `weekly` on the first closed-market evening of
each ISO week (normally Monday):

```cron
*/15 * * * 1-5 /path/to/sawa/scripts/market_scheduler.sh >> ~/.sawa/scheduler/cron.log 2>&1
```

State lives under `~/.sawa/scheduler/`. Sends ntfy notifications if
`NTFY_TOPIC` is set. The scheduler runs `sawa doctor --job daily` and
`sawa doctor --job weekly` after successful jobs; if doctor exits non-zero,
the job is not marked done and an error notification is sent. A failed daily
or weekly is retried on the next closed-evening tick at most
`MAX_JOB_ATTEMPTS` (3) times per date/week (`daily_attempts_<date>`,
`weekly_attempts_<week>` counters in the state directory); touch the
`*_done_*` flag to skip, or delete the counter to re-arm. A tick that dies
before it can run anything (bad `.env`, missing venv, unwritable state
directory) pushes one `Sawa Scheduler FAILED` alert per day from its EXIT
trap — via `sawa notify` when the venv works, else via a direct ntfy POST.

### Alternative: discrete cron entries

```cron
0 18 * * 1-5 /path/to/sawa/scripts/daily.sh
0  2 * * 6   /path/to/sawa/scripts/weekly.sh   # Saturday, matching market_scheduler.sh
```

Quarterly is small — run by hand or once a quarter.

## Database Doctor

Use `doctor` when you want a database-only health check without contacting
external APIs:

```bash
sawa doctor                                # broad database check
sawa doctor --job daily                    # checks relevant after daily
sawa doctor --job weekly                   # checks relevant after weekly
sawa doctor --min-coverage 0.95            # stricter coverage vs recent baseline
sawa doctor --max-staleness-days 3         # stricter stock_prices recency
```

Exit code is `0` only when there are no failed checks. Warnings are printed and
included in notifications, but do not make the command fail.

## Database Backups

`/home/seed/scripts/backup_postgres.sh` runs from cron every Sunday at 01:00
UTC and writes a `pg_dump -F t` archive to `/data/db-backups`, keeping the ten
most recent.

Postgres runs in a rootless podman container, so the dump executes inside the
container. This is not a style choice: the host `pg_dump` is 16.x while the
server is 18.x, and pg_dump refuses to dump a newer server. The script sets
`XDG_RUNTIME_DIR` itself because cron does not.

Each run writes to a `.part` file and renames it only after the archive passes
three checks: an absolute size floor, at least half the size of the previous
backup, and a readable tar table of contents containing `toc.dat`. A partial
dump therefore never takes the name of a good one, and retention never sees it.

Failures raise an ntfy alert through `sawa notify`. Independently,
`sawa doctor` fails the `backup.freshness` check when the newest archive is
more than 10 days old, so a stopped cron surfaces even when the script never
runs to report its own failure. Point `SAWA_BACKUP_DIR` elsewhere to check a
different location; hosts without the directory skip the check.

Run one on demand, and restore, like this:

```bash
/home/seed/scripts/backup_postgres.sh                  # takes ~1 minute

# Inspect an archive (through the container: host pg_restore is older)
podman exec -i stocksdb pg_restore -l < /data/db-backups/postgres_backup_YYYYMMDD_HHMMSS.tar

# Restore over the live database - destructive, stop the schedulers first
podman exec -i stocksdb pg_restore -U postgres -d postgres --clean --if-exists \
    < /data/db-backups/postgres_backup_YYYYMMDD_HHMMSS.tar
```

## Re-entrancy

All operations are safe to re-run.

| Data | Key | Behavior |
|------|-----|----------|
| Stock prices | (ticker, date) | Upsert |
| Intraday prices | (ticker, timestamp) | Upsert |
| Fundamentals | (ticker, period_end, timeframe) | Upsert |
| Economy | (date) | Upsert |
| Market internals | (date) | Upsert |
| Companies | (ticker) | Upsert |
| Ratios | (ticker, date) | Upsert |
| News articles | (id) | Upsert |
| Technical indicators | (ticker, date) | Upsert |

If an update fails partway:
1. Check the log file for the underlying error
2. Fix it (network, API limits, schema mismatch)
3. Re-run the same command — the upsert keys mean partial progress is fine

## Log Files

```
logs/
  coldstart_YYYYMMDD_HHMMSS.log
  daily_YYYYMMDD_HHMMSS.log
  weekly_YYYYMMDD_HHMMSS.log
  quarterly_YYYYMMDD_HHMMSS.log
  ta_backfill_YYYYMMDD_HHMMSS.log
  character_YYYYMMDD_HHMMSS.log
```

Console output is INFO; the file gets DEBUG.

## Troubleshooting

### "No existing data found. Run coldstart first."
Database empty. Run `sawa coldstart`.

### "No symbols in database"
`companies` table is empty. Run `sawa coldstart`, or `sawa coldstart
--skip-downloads` if you already have CSVs in `data/`.

### "FRED_API_KEY not set" / market internals skipped
The step is skipped with an ntfy alert. Set `FRED_API_KEY` to fix.
Get a free key at <https://fred.stlouisfed.org/docs/api/api_key.html>.

### API rate limits
The pipeline uses `SyncRateLimiter` (default 5 req/s for Polygon — see
`sawa/utils/constants.py`). On 429s, wait and retry.

### Database connection
Confirm: `DATABASE_URL` set, PostgreSQL running, network reachable, user
has DDL + DML on the target database.

### S3 download failures
Confirm S3 credentials, network, and that the date range falls within
Polygon's available history (typically 5+ years back).

### Adjusting after a stock split
Polygon's daily aggregates are split-adjusted at fetch time, but historical
data already in the DB will not be retroactively adjusted. Run
`sawa adjust-splits --ticker XYZ` or let `sawa adjust-splits` auto-detect
recent splits (the daily and weekly jobs do this themselves for splits they
record). Polygon serves a rolling five-year history, so rows older than that
cannot be re-fetched; the refresh re-bases them locally by the ratio Polygon
applied at the first date it did serve, provided the stored tail sits on the
same basis as that boundary. Rows it cannot reconcile are reported in
`pre_horizon_rebase_skipped` and are the job of
`scripts/quarantine_unadjustable_prices.py`.

Coldstart's flat-file bars are as-traded and are re-based at load time from
`stock_splits`; see `docs/DATA_SOURCES.md` §2.2. If `stock_splits` was
empty when a cache was loaded, populate it for the whole history and reload:

```bash
sawa corporate-actions --splits-only --start-date 2021-01-04   # history start
sawa coldstart --load-only
```

### Stale split basis: finding, quarantining, and restoring rows
A recorded split whose raw jump is still visible in `stock_prices` means that
ticker's history was never re-based. This query lists them (splits closer to
their raw step than to no step at all); fix each with
`sawa adjust-splits --ticker XYZ`:

```sql
WITH s AS (SELECT ticker, execution_date, split_to::numeric / split_from AS f
           FROM stock_splits),
     p AS (SELECT ticker, date, close,
                  lag(close) OVER (PARTITION BY ticker ORDER BY date) AS prev
           FROM stock_prices WHERE ticker IN (SELECT ticker FROM s))
SELECT s.ticker, s.execution_date, s.f, p.prev, p.close
FROM s JOIN p ON p.ticker = s.ticker
 AND p.date = (SELECT min(date) FROM stock_prices q
               WHERE q.ticker = s.ticker AND q.date >= s.execution_date)
WHERE p.prev > 0 AND (s.f >= 1.2 OR s.f <= 0.8333)
  AND abs(ln(p.close / p.prev) + ln(s.f)) < abs(ln(p.close / p.prev));
```

Two scripts handle rows older than the provider window:

- `scripts/quarantine_unadjustable_prices.py` moves pre-horizon rows that sit
  on a stale basis into `stock_prices_unadjustable_archive` (dry run by
  default; `--apply`, `--recompute-ta`).
- `scripts/restore_quarantined_prices.py` puts archived rows back on the
  split-adjusted basis using `stock_splits`, trying the as-traded reading and
  each "only the latest k splits" reading segment by segment, and restoring
  rows only where exactly one reading is continuous with the stored series
  (dry run by default; `--apply`, `--recompute-ta`, `--ticker XYZ`). Rows it
  leaves archived are listed with the reason. When the reason is that no
  split is recorded after the rows, the quarantined jump was a genuine move
  (TAL and ARDX collapsed 70%+ in July 2021); `--as-is XYZ` restores such rows
  unchanged and refuses any ticker that does have a later split.

## Data Directory Layout

```
data/
  nasdaq1000_symbols.txt        # NASDAQ universe list (~5000 tickers)
  data_mappings.json            # Optional CSV→table mapping for sawa.database.loader
  prices/AAPL.csv               # Per-symbol price files (coldstart output)
  fundamentals/
    balance_sheets.csv          income_statements.csv      cash_flow.csv
    *_update.csv                # Weekly delta files
  economy/
    treasury_yields.csv         inflation.csv
    inflation_expectations.csv  labor_market.csv
    market_internals.csv        # FRED-sourced VIX / VIX3M / HY spread
  overviews/overviews.csv
  ratios/ratios.csv
```

## Monitoring

Three layers, from cheapest to most independent. Only the first exists by
default; the other two need an operator to arm them.

1. **In-job pushes** (armed when `NTFY_TOPIC` is set): `market_scheduler.sh`
   sends intraday start/stop, daily/weekly summaries and failure alerts;
   `monitored_run` inside every `sawa` job pushes `Sawa: <job> FAILED`; the
   scheduler's EXIT trap pushes `Sawa Scheduler FAILED` (once per day) when a
   tick aborts before running any job. These only fire from a process that
   actually ran: cron stopped, host down, or the script unreadable produces
   nothing here.

2. **Watchdog** (independent cron line, shares no failure domain with the
   scheduler's `setup_env`):
   ```cron
   30 12 * * * cd /home/seed/code/sawa && SAWA_NOTIFY_SUCCESS=0 .venv/bin/sawa doctor --job watchdog --log-dir logs >> "$HOME/.sawa/scheduler/watchdog.log" 2>&1
   ```
   `sawa doctor --job watchdog` (08:30 EDT / 07:30 EST on a UTC host) checks,
   against the NYSE trading calendar, that `stock_prices`,
   `technical_indicators`, `market_internals` and `news_articles` reach the
   latest expected session, and reads `~/.sawa/scheduler/` directly: newest
   `Scheduler tick` line < 45 min old (unless a job is running),
   `daily_done_<session>` and `weekly_done_<ISO week>` present, and no
   `sawa intraday` process alive outside 09:00-16:30 ET. Any FAIL pushes
   `Sawa: doctor FAILED` naming the failing checks through `sawa`'s own
   notifier (`sawa` loads `.env` itself and is not subject to the scheduler's
   allowlist). Run it by hand any time: `sawa doctor --job watchdog`. Install
   the cron line only after the code that knows `--job watchdog` is deployed.

3. **Dead-man's switches** (detect that *nothing* ran, including the
   watchdog): create checks on healthchecks.io (or equivalent) and put their
   ping URLs in `.env` (all four names are allowlisted):

   | Variable | Pinged by | Suggested period / grace |
   |----------|-----------|--------------------------|
   | `SAWA_HEARTBEAT_URL` | scheduler after a successful daily (`/fail` on failure) | 1 day / 4 h |
   | `SAWA_WEEKLY_HEARTBEAT_URL` | scheduler after a successful weekly | 7 days / 1 day |
   | `SAWA_TICK_HEARTBEAT_URL` | scheduler right after every `Scheduler tick` line | 15 min / 60 min |
   | `SAWA_WATCHDOG_HEARTBEAT_URL` | `sawa doctor --job watchdog` (`/fail` when any check fails) | 1 day / 1 h |

   Pings are HTTPS-only GETs with redirects disabled; the URL never appears in
   argv or logs.

4. **Database size**: views in `sqlschema/06_views.sql` and
   `22_views_advanced.sql` are good targets for slow-query monitoring.

### What alerts when the scheduler itself does not run

Layers 2 and 3, plus the EXIT-trap push in layer 1 when the script at least
starts. The 2026-09-04 outage (an unknown key in `.env` made `setup_env` abort
on every 15-minute tick for 11 days) produced no push at all because only the
in-job pushes existed and the abort happened before any of them.
