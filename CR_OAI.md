# Code Review Report

Date: 2026-02-06
Scope: sawa/cli.py, sawa/coldstart.py, sawa/update.py, sawa/api/client.py,
       sawa/utils/*.py, sawa/database/connection.py, sawa/repositories/*

## Executive Summary
The core architecture is coherent (domain models, repository interfaces, and
shared utilities are consistent). The main issues are a few correctness bugs,
some CLI safety/behavior mismatches, and a couple of edge cases that can lead
to missing or misleading data. No tests were executed for this review.

## Strengths
- Clear separation between ingestion (coldstart/update) and read-only access
  (repositories).
- Consistent use of domain models to normalize database and API responses.
- Shared utilities for CSV writing and ticker validation are well factored.

## Findings

### High
1) Polygon ticker events filters are ignored
   - Location: sawa/api/client.py (get_ticker_events)
   - Issue: event_types are built into params but never passed to get_single.
   - Impact: API filtering by event type never applies, returning unfiltered data.
   - Suggested fix: extend get_single to accept params or build URL with params.

2) REST aggregate timestamps interpreted in local timezone
   - Location: sawa/repositories/polygon_prices.py (_convert_rest_result)
   - Issue: datetime.fromtimestamp uses local timezone; Polygon timestamps are
     UTC-based. This can shift dates for non-UTC systems.
   - Impact: off-by-one-day errors in price dates for some users.
   - Suggested fix: use UTC conversion (e.g., datetime.fromtimestamp(..., tz=UTC)
     or sawa.utils.dates.timestamp_to_date).

### Medium
3) Coldstart date skip heuristic can drop partial data
   - Location: sawa/coldstart.py (_check_date_already_downloaded)
   - Issue: if any of the first few CSVs contain the date, the entire date is
     skipped. If a previous run was partial, missing tickers will never be
     downloaded.
   - Impact: silent data gaps for some tickers.
   - Suggested fix: track per-date completion or validate per-symbol presence
     before skipping.

4) Drop safety messaging references a non-existent flag
   - Location: sawa/coldstart.py (drop_only path), sawa/cli.py
   - Issue: logs say to use --confirm-drop in non-interactive mode, but the
     CLI does not define this flag. Also, schema-only always forces no-drop
     with no override.
   - Impact: confusing UX and no supported way to confirm destructive actions
     in CI/non-interactive runs.
   - Suggested fix: add --confirm-drop to CLI and honor it; clarify or adjust
     schema-only behavior/message.

### Low
5) Ticker normalization inconsistency in news queries
   - Location: sawa/repositories/database.py (_get_news_sync,
     _get_news_sentiment_summary_sync)
   - Issue: ticker parameter is not uppercased; other repository methods do.
   - Impact: callers passing lowercase tickers may get empty results.
   - Suggested fix: normalize ticker to upper case before queries.

6) Unknown technical indicator filters are silently ignored
   - Location: sawa/repositories/database.py (_screen_sync)
   - Issue: invalid indicator names are skipped without error, which can
     return broad results unexpectedly.
   - Impact: user input mistakes are hard to detect.
   - Suggested fix: validate and raise on unknown indicators or return a clear
     error message.

7) HTTP client lifetime not managed
   - Location: sawa/api/client.py (PolygonClient)
   - Issue: httpx.Client is never closed.
   - Impact: connection pool warnings or socket leaks in long-lived processes.
   - Suggested fix: add close() or context manager support.

## Recommended Next Steps
1) Fix the high-severity items (ticker-events params, UTC timestamp handling).
2) Align CLI behavior with safety messaging and add explicit non-interactive
   confirmation for destructive operations.
3) Tighten data integrity checks for coldstart price downloads.
4) Add validation and ticker normalization in repositories.
