# Periodic data collection review and repairs

Updated 2026-09-27. The original review covered `7be2780`; production failures
were reproduced against `a6193e0`. The working tree now contains the repairs
below. This is a collection/storage inventory and repair record, not a claim
that the next live market session has already been observed.

## Sources, storage and recurring jobs

PostgreSQL (`DATABASE_URL`, `public` schema) is authoritative. Default `data/`
CSVs are staging artifacts: replacing a download does not remove accumulated
database history. Loaders validate identity and values, paginate/retry requests,
and upsert using table-specific keys. Incomplete persistence must remain visible.

| Data | Source → storage | Recurring mechanism |
|---|---|---|
| Daily OHLCV | Polygon adjusted REST aggregates → `stock_prices`; raw S3 flatfiles bootstrap history | `daily`: 14-day replay extended for lagging tickers; split repairs re-fetch history. Bootstrap rebases raw bars through the split registry. |
| Intraday OHLCV/completeness | Polygon delayed WebSocket and REST minutes → `stock_prices_intraday` | Five-minute aggregation, buffered upserts and minute-level deduplication. Startup/reconnect recovery includes partial buckets. EOD cleanup removes replaced session bars and bars older than seven days. |
| Company profiles | Polygon ticker details → `companies`; overview CSV | `weekly` refreshes active profiles; maintenance onboards newly discovered companies/history. |
| News/sentiment | Polygon news → `news_articles`, `news_article_tickers`, `news_sentiment` | `daily`/`weekly`: paginated 30-day replay; article metadata/descriptions and supplied sentiment, not full article bodies. |
| Treasury/inflation/labor | Polygon `/fed/v1/*` → `treasury_yields`, `inflation`, `inflation_expectations`, `labor_market`; economy CSVs | `weekly` replays 365 days per table; `daily` independently refreshes Treasury yields with a 30-day overlap. |
| Volatility/credit | FRED `VIXCLS`, `VXVCLS`, `BAMLH0A0HYM2`; CBOE VIX/VIX3M quotes/history → `market_internals` | `daily`/`weekly` FRED overlap; daily CBOE fills publication lag and historical gaps. Missing fields preserve stored values. `put_call_ratio` has no configured collector. |
| Splits/dividends | Polygon corporate actions → `stock_splits`, `dividends` | `weekly` trailing-year replay; split events trigger adjusted-price/TA repair. Daily also repairs recent splits. Decimal share counts become exact integer ratios stored as BIGINT. |
| Statements/ratios | Polygon financial endpoints → `balance_sheets`, `income_statements`, `cash_flows`, `financial_ratios`; staged CSVs | Weekly `maintenance` invokes `quarterly`. Statements use per-ticker/feed watermarks with 120-day overlap; missing histories fetch fully. Monthly full-history reconciliation repairs older gaps. |
| Earnings | Yahoo/yfinance → `earnings` | Weekly maintenance refreshes report dates, EPS and surprises. Per-ticker transactions preserve known values when fields are absent; report timing respects New York sessions and early closes. |
| Universe/membership | Wikipedia, Polygon active listings, fixed MAG7; bundled Nasdaq bootstrap fallback → `indices`, `index_constituents` | Maintenance discovers symbols, onboards missing company/history data, then replaces complete snapshots. Failed sources/onboarding preserve affected memberships. No membership history. |
| Derived analytics | Stored prices/benchmarks → `technical_indicators`, `mv_52week_extremes`, four `stock_character_*` tables | Daily TA/extrema; weekly character. Extrema refresh after same-date corrections; character/benchmarks respect run date and stale source histories are excluded. |

SQL seeds maintain sector mappings/indicator metadata. `trader_cards` has no
in-repository writer. Repair archives retain removed source rows. Scheduler
state lives under `~/.sawa/scheduler/`; pipeline logs use the configured log
directory. Local MCP audit logs also feed weekly usage-insight JSON.

## Mechanism fixes

Recommended cron fires every 15 minutes, every day. Daily runs at/after
17:00 ET when closed; weekly runs on the first eligible evening of each ISO
week, normally Monday. Daily failure no longer suppresses weekly work.
Completion requires the job and scoped doctor check; retry caps remain.
Maintenance has an independent background lock, so lengthy earnings/fundamental
work cannot hold the market-control lock into the next opening. Launch gates
recheck time after long predecessors.

Intraday explicitly handles SIGINT/SIGTERM, including inherited ignored SIGINT.
The scheduler allows a 30-minute drain after observed close (15-minute feed delay
plus corrections), then verifies completion written after final persistence.
Forced termination or missing completion is a failure. Daily cleanup cannot
race a draining stream. Reconnect recovery merges minute lineage without
double-counting live bars; unresolved recovery is reported.

Health checks require both volatility series through the expected session after
daily processing, matching watchdog behavior. Treasury allows one completed
trading session of publication lag. Character coverage measures the latest run
against eligible active histories, not cumulative historical classifications.
Timestamped run logs expire after 90 days only if both filename and modification
time are old; audit JSON, symlinks and recently written logs are excluded.
`SAWA_LOG_RETENTION_DAYS=0` disables pruning.

## Production repair record

- CBOE's old host returned HTTP 307, leaving both volatility series at September 22.
  The client now uses `cdn-api.cboe.com` and replays history. September 23–25 VIX
  and VIX3M values were restored and read back successfully.
- All 38 skipped fractional split records were restored, including AXIA's exact
  `5000000000000:6314189440537` ratio. Additive migration 49 widens only the two
  split-count columns, preserving existing rows. All 29 affected tickers were
  repaired: 30,796 provider price rows, 154 ledger-verified older CLBK rows,
  and 33,066 rebuilt technical-indicator rows, with no ticker failures. Character
  replay at the existing September 21 run date classified 15, left 13 normally
  unclassifiable and correctly excluded one stale source, with zero errors.
- Verified ticker reuse was repaired for four symbols. In total, 1,404 obsolete
  price rows were preserved in `stock_prices_unadjustable_archive`, with CIKs,
  exact date ranges and evidence. Current-series prices/TA were rebuilt and
  extrema refreshed. Invalid character history was removed; ICON's latest
  classification was rebuilt from its current issuer alone.
- The new Treasury path replayed 19 observations; latest source date September 24
  is within the permitted publication lag.

| Ticker | Archived rows | Retained price/TA rows | Current-history boundary |
|---|---:|---:|---|
| DFNS | 133 | 159 | 2026-02-09 |
| BBBY | 494 | 241 | 2025-08-29 |
| ICON | 116 | 554 | 2024-07-12 |
| RPT | 661 | 454 | 2024-12-03 |

Primary evidence includes [BBBY's old-equity cancellation](https://www.sec.gov/Archives/edgar/data/886158/000119312523247428/d579010d8k.htm),
[Iconix's cash merger](https://www.sec.gov/Archives/edgar/data/857737/000114036121026938/brhc10027568_8k.htm),
and [old RPT's conversion into Kimco](https://www.sec.gov/Archives/edgar/data/1959472/000114036124000247/ny20017682x2_8k.htm).
Archive evidence also records the unrelated successors' filings/provider identities.
Persisted boundaries protect REST writes, raw CSV bootstrap, split repair and
restoration; explicitly excluded rows remain separately counted. Read-only
production checks verified that none of these four histories can be reintroduced.

Issuer changes require evidence, not a guessed multiplier: a read-only preflight
covered all 666 active/priced annual-split tickers without request failures.
CLBK/REAX successor conversions and SCO/UVXY metadata corrections have narrowly
pinned, primary-source-backed rules; SNFCA retains both security identifiers.
Historical basis changes must match the split ledger; ambiguous unavailable
ranges fail visibly. Large historical
jumps are a safety heuristic, not proof of corruption. Yahoo HTTP errors remain
visible even when its library returns no table; individual HTTP 200/no-table
responses remain a provider coverage limitation.

## Live freshness and health audit — September 27

Read-only checks at 18:47 UTC used September 25 as the latest completed market
session. **Daily market data is current, but the database is not fully current.**
The broad doctor returned **47 PASS, 2 WARN, 0 FAIL**; the watchdog returned
**30 PASS, 1 WARN, 0 FAIL**. These results do not certify every feed: quarterly
checks permit 210-day-old period dates, and earnings/membership freshness is not
checked. Direct table queries found the gaps below.

| Area | Observed state |
|---|---|
| Prices and TA | Latest September 25; 9,970 of 10,011 recently priced tickers have latest-session prices (99.6%). All 9,970 active latest-session prices have matching same-session TA, with no active TA-only rows. Latest-session OHLCV sanity check found no malformed rows. |
| Volatility and credit | VIX/VIX3M September 25; high-yield spread September 24. |
| Treasury and news | Treasury September 24, within the permitted publication lag; latest news September 26. |
| Earnings — stale | Last refreshed February 24; latest actual report February 23. No upcoming dates are stored. 498 of 503 stored S&P 500 members have earnings history, but none have actuals within 120 days. |
| Statements and ratios — stale | All three statement feeds stop at June 11 filings / May 10 periods; last insert June 15. Each covers 484 of 503 stored S&P 500 members. Ratios stop at June 12. No statement feed has filings within 90 days. |
| Index membership — stale | Every snapshot was last refreshed May 16, including the 503-member S&P 500 snapshot. Membership-based coverage above uses this stored, not independently verified current, universe. |
| Monthly economy | CPI/labor latest August; PCE/job openings July; modeled inflation expectations September. Dates are plausible for monthly cadence; latest provider releases were not independently verified. CPI YoY is missing for 15 newer CPI observations, with its last non-null value in April 2025. |
| Coverage warnings | 6,031 active companies lack SIC and 5,152 lack market cap. Latest character run September 21 classifies 2,929 of 7,534 eligible companies (38.9%). Treasury 6-month, 3-year, 7-year and 20-year columns are entirely empty; the current source CSV omits them. |
| Operations | Current daily/weekly completion markers and recent scheduler heartbeat; no orphan intraday process. September 27 database backup present (4.3 GB). |

A bounded integrity audit of August 28–September 25 covered 199,881 price rows
and 199,881 TA rows. It found no future/nontrading price dates, null or invalid
OHLCV, or orphan TA rows. All four reviewed issuer boundaries still match current
CIKs and have zero price/TA rows predating the allowed histories.
Both split-count columns are confirmed `BIGINT`. The current inflation CSV
contains only CPI, core CPI and date; the loader does not derive missing YoY
values, so a replay alone is not confirmed to resolve that field-level gap.

The new maintenance path is intended to refresh earnings, statements, ratios and
memberships, but it has not yet run across the full universe. Until that refresh
and a follow-up audit succeed, these feeds must not be described as up to date.
No ingestion, notifications or database writes were triggered by this audit.

## Verification and rollout

Regressions cover CBOE history, exact fractional persistence, identity archives,
source-date coverage, scheduler independence, delayed shutdown/recovery, universe
onboarding, filing overlaps, earnings failures and retention. The final full suite
passed 1,353 tests, with one live database smoke test skipped in the sandbox.
That smoke test then passed separately against the live database with both
session and transaction read-only enforced: all 1,354 collected cases have now
passed across these runs.
All 45 changed/new Python files passed lint; all six shell scripts passed syntax
checks, and dependency-lock/diff checks passed. Repository-wide lint still reports
one pre-existing import-order issue in unchanged `tests/tools/test_news.py:202`.
The four test warnings concern multiprocessing fork from a multithreaded test
process. WebSocket tests used a test-process-only 20 ms selector
polling workaround for this environment's async-thread wakeup problem;
production code is not monkeypatched.

A live read-only Yahoo probe returned 25 usable AAPL earnings records; that
provider probe did not refresh the stale stored earnings table.

No full-universe maintenance run or live market session was forced during repair.
The scheduler will use the new paths on its next eligible tick. Other deployments
must sync dependencies, apply migration 49 transactionally, and deploy code and
scheduler together. See [MAINTENANCE.md](MAINTENANCE.md),
[OPERATIONS.md](OPERATIONS.md), and [DATA_SOURCES.md](DATA_SOURCES.md).
