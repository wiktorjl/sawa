# Code Review Report

**Date:** February 2, 2026  
**Scope:** sp500_tools/, tui/sp500_tui/

---

## Executive Summary

The codebase is well-structured with clear separation of concerns. The repository pattern is properly implemented, domain models are immutable and well-documented, and the CLI interface is comprehensive. However, there are several areas requiring attention:

- **35 mypy errors** in sp500_tools/
- **39 mypy errors** in tui/
- **9 ruff linting issues** in tui/
- Several architectural improvements needed

---

## 1. Type Safety Issues

### 1.1 Missing Type Stubs (sp500_tools)

Several third-party libraries lack type stubs:

| Library | File | Priority |
|---------|------|----------|
| psycopg2 | database/connection.py, loader.py, news.py | High |
| boto3/botocore | api/s3.py | Medium |
| requests | api/client.py | High |
| dateutil | utils/dates.py | Low |
| tomli | utils/xdg.py | Low |

**Fix:** Add types-* packages to dev dependencies in pyproject.toml:
```toml
[project.optional-dependencies]
dev = [
    "types-psycopg2",
    "types-requests",
    "types-python-dateutil",
    "boto3-stubs",
]
```

### 1.2 AsyncIterator Return Type Issue

**Location:** `sp500_tools/repositories/database.py:151`, `polygon_prices.py:287`

The `get_prices_stream` method has incompatible return type. The base class declares it as `async def ... -> AsyncIterator[...]` but implementations return a coroutine wrapping an async iterator.

```python
# Current (incorrect)
async def get_prices_stream(...) -> AsyncIterator[StockPrice]:
    ...
    yield price

# Base class should be:
def get_prices_stream(...) -> AsyncIterator[StockPrice]:
    ...
```

### 1.3 combine.py Type Errors

**Location:** `sp500_tools/processing/combine.py:94-119`

Multiple type errors due to treating `list[str]` as `dict`. The code appears to be using CSV rows incorrectly.

```python
# Line 99: Using list with string index
fieldnames[col] = 'ticker'  # Error: list doesn't support str index
```

### 1.4 TUI Type Issues

**Location:** `tui/sp500_tui/models/settings.py`

Using `object` type instead of proper dict typing:
```python
# Line 78, 145, etc.
SETTING_DEFINITIONS: dict[str, dict[str, Any]] = {...}
```

**Location:** `tui/sp500_tui/ai/client.py:182`

Using `callable` instead of `Callable`:
```python
# Current
stream_callback: callable | None = None

# Should be
from collections.abc import Callable
stream_callback: Callable[[str], None] | None = None
```

**Location:** `tui/sp500_tui/services/stock_service.py:193,213,233`

Literal type mismatch for timeframe parameter:
```python
# Current
timeframe: str = "quarterly"

# Should be
from typing import Literal
timeframe: Literal["quarterly", "annual"] = "quarterly"
```

---

## 2. Linting Issues (TUI)

### 2.1 Line Too Long

| File | Line | Length |
|------|------|--------|
| ai/prompts.py | 3 | 113 |
| ai/prompts.py | 12 | 195 |
| ai/prompts.py | 17 | 110 |
| models/glossary.py | 96 | 104 |
| models/glossary.py | 105 | 104 |

### 2.2 Unused Import

**Location:** `tui/sp500_tui/database.py:109`
```python
import os  # Unused
```

### 2.3 f-string Without Placeholders

**Location:** `tui/sp500_tui/app.py:283`
```python
# Current
f"Failed to create user (name may already exist)"

# Should be
"Failed to create user (name may already exist)"
```

### 2.4 Trailing Whitespace

- `tui/sp500_tui/database.py:195`
- `tui/sp500_tui/models/glossary.py:192`

---

## 3. Code Design Issues

### 3.1 Duplicated CSV Writing Pattern

**Locations:** 
- `sp500_tools/coldstart.py:152-163` (download_fundamentals)
- `sp500_tools/coldstart.py:199-209` (download_overviews)
- `sp500_tools/coldstart.py:233-242` (download_economy)
- `sp500_tools/coldstart.py:274-284` (download_ratios)
- `sp500_tools/update.py:117-127` (update_fundamentals)
- `sp500_tools/update.py:153-162` (update_economy)

The same pattern is repeated 6+ times:
```python
all_fields: set[str] = set()
for record in data:
    all_fields.update(record.keys())
fieldnames = sorted(all_fields)
with open(filepath, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(data)
```

**Recommendation:** Extract to a utility function in `sp500_tools/utils/csv_utils.py`:
```python
def write_csv_auto_fields(filepath: Path, data: list[dict], logger: Logger) -> None:
    """Write CSV with auto-detected fields from data."""
```

### 3.2 Inconsistent Error Handling

**Locations:**
- `sp500_tools/coldstart.py:149,196,245,271` - Use `except Exception as e`
- `sp500_tools/api/client.py:192` - Uses `except requests.exceptions.RequestException`

Some exception handlers are too broad (`Exception`) while others are specific. Error recovery is inconsistent.

### 3.3 Mixed Database Drivers

The codebase uses both `psycopg` (async, v3) and `psycopg2` (sync, v2):
- `sp500_tools/database/loader.py` uses psycopg2
- `sp500_tools/database/load.py` uses psycopg
- `sp500_tools/database/connection.py` uses psycopg2
- `sp500_tools/coldstart.py` uses psycopg

**Recommendation:** Standardize on psycopg (v3) for all database operations or document the intentional split.

### 3.4 Missing Input Validation

**Location:** `sp500_tools/api/client.py`

No validation of ticker symbols before API calls:
```python
def get_ticker_details(self, ticker: str) -> dict[str, Any] | None:
    # No validation of ticker format
```

**Recommendation:** Add ticker validation:
```python
from sp500_tools.utils.symbols import validate_ticker
if not validate_ticker(ticker):
    raise ValueError(f"Invalid ticker: {ticker}")
```

### 3.5 Hardcoded Magic Values

**Locations:**
- `sp500_tools/coldstart.py:590` - `days=30` hardcoded for news
- `sp500_tools/api/client.py:59,146` - timeout=30 hardcoded
- `sp500_tools/database/load.py:112` - batch_size=1000 hardcoded

**Recommendation:** Move to configuration or constants file.

---

## 4. Architecture Improvements

### 4.1 Missing Test Coverage

**Current state:**
- `tests/domain/` - Good coverage of models and exceptions
- `tests/repositories/` - Good mock coverage
- `tests/tui/services/` - Partial coverage

**Missing:**
- Integration tests for database loader operations
- Tests for `sp500_tools/api/client.py`
- Tests for `sp500_tools/api/s3.py`
- Tests for `sp500_tools/coldstart.py` and `sp500_tools/update.py`
- Tests for most TUI views and state management

### 4.2 No Retry Logic for S3 Operations

**Location:** `sp500_tools/api/s3.py`

The S3 client has no retry logic for transient failures:
```python
def download_day(self, target_date: date) -> str | None:
    # No retry on network errors
    self.client.download_fileobj(S3_BUCKET, key, tmp)
```

**Recommendation:** Add retry with exponential backoff similar to `api/client.py:get_single()`.

### 4.3 TUI State Class is Too Large

**Location:** `tui/sp500_tui/state.py`

The `AppState` class has 70+ attributes and 25+ methods. Consider splitting into:
- `NavigationState` - Current view, previous view
- `StockState` - Watchlists, selected stocks, filters
- `DetailState` - Stock detail, news, logos
- `SettingsState` - Settings category, editing state
- `UIState` - Messages, input mode, help overlay

### 4.4 No Rate Limiting for Polygon API in Coldstart

**Location:** `sp500_tools/coldstart.py`

When downloading data for 500+ symbols, there's no rate limiting:
```python
for i, symbol in enumerate(symbols, 1):
    data = client.get_fundamentals(...)  # No delay between calls
```

**Recommendation:** Use the existing `RateLimiter` class from repositories.

---

## 5. Style Inconsistencies & Technology Mixing

### 5.1 Database Driver Fragmentation (Critical)

The codebase uses **two incompatible PostgreSQL drivers** for the same purpose:

| File | Driver | Notes |
|------|--------|-------|
| `sp500_tools/database/schema.py` | psycopg (v3) | |
| `sp500_tools/database/load.py` | psycopg (v3) | |
| `sp500_tools/database/loader.py` | psycopg2 (v2) | Same functionality as load.py |
| `sp500_tools/database/connection.py` | psycopg2 (v2) | |
| `sp500_tools/database/news.py` | psycopg2 (v2) | |
| `sp500_tools/repositories/database.py` | psycopg2 (v2) | |
| `sp500_tools/coldstart.py` | psycopg (v3) | |
| `sp500_tools/update.py` | psycopg (v3) | |
| `tui/sp500_tui/database.py` | psycopg (v3) | |
| `mcp_server/database.py` | psycopg (v3) | |

**Recommendation:** Standardize on psycopg (v3) for all database operations.

### 5.2 HTTP Client Fragmentation

| Package | Library | Files |
|---------|---------|-------|
| sp500_tools | `requests` | api/client.py |
| tui | `httpx` | logo.py, ai/client.py |

**Recommendation:** Standardize on `httpx` (more modern, async support).

### 5.3 Configuration Function Duplication

`get_database_url()` is implemented in **4 different places** with different return types:

| Location | Returns |
|----------|---------|
| `sp500_tools/utils/config.py:44` | `str \| None` |
| `sp500_tools/repositories/config.py:96` | `str \| None` |
| `tui/sp500_tui/config.py:10` | `str` (raises) |
| `mcp_server/database.py:20` | `str` (raises) |

**Recommendation:** Single implementation in shared utils, imported by all packages.

### 5.4 Duplicated Data Models

Two parallel model hierarchies exist:

| Domain Model (sp500_tools/domain) | TUI Model (tui/models/queries) |
|-----------------------------------|-------------------------------|
| `StockPrice` | `StockPrice` |
| `CompanyInfo` | `Company` |
| `IncomeStatement` | `IncomeStatement` |
| `BalanceSheet` | `BalanceSheet` |
| `CashFlow` | `CashFlow` |
| `FinancialRatio` | `FinancialRatios` |
| `TreasuryYield` | `TreasuryYields` |

**Recommendation:** TUI should use domain models via converters (pattern already exists in `tui/services/converters.py`).

### 5.5 Dataclass Style Inconsistency

- **Domain models:** `@dataclass(frozen=True, slots=True)` - immutable, memory-efficient
- **All other dataclasses (27 files):** `@dataclass` - mutable, no slots

**Recommendation:** Apply `frozen=True, slots=True` to all value-type dataclasses.

### 5.6 Error Handling Inconsistency

33 uses of broad `except Exception` across the codebase, mixed with specific exception handling elsewhere. No consistent policy.

### 5.7 Logging Pattern Inconsistency

Two patterns used:
- **Module logger:** `logger = logging.getLogger(__name__)` (14 files)
- **Passed logger:** `def function(logger: logging.Logger)` (coldstart.py, update.py)

### 5.8 Legacy/Dead Code

- `tui/sp500_tui/database.py:140-291` - `init_schema_legacy()` marked deprecated
- `tui/sp500_tui/config.py:69-103` - `_LegacyConfigShim` marked "temporary"

### 5.9 Naming: Tool is "sawa" not "sp500"

The project is named **sawa** but uses `sp500` throughout:
- Package: `sp500-tools` should be `sawa`
- Module: `sp500_tools/` should be `sawa/`
- TUI package: `sp500-tui` should be `sawa-tui`
- TUI module: `sp500_tui/` should be `sawa_tui/`
- CLI command: `sp500` should be `sawa`

---

## 6. Security Considerations

### 6.1 API Key Handling

**Good:** API keys are read from environment variables, not hardcoded.

**Improvement needed:** The TUI stores API keys in the database (`settings.py`). Consider using OS keyring instead:
```python
import keyring
keyring.set_password("sp500-tui", "zai_api_key", api_key)
```

### 6.2 SQL Injection Prevention

**Good:** The codebase correctly uses parameterized queries with `psycopg2.sql` module.

---

## 7. Tasks to Resolve Issues

### Rename (Do First)

| ID | Task | Files | Effort |
|----|------|-------|--------|
| R1 | Rename sp500-tools package to sawa | pyproject.toml | 15 min |
| R2 | Rename sp500_tools/ directory to sawa/ | All files in sp500_tools/ | 30 min |
| R3 | Rename sp500-tui package to sawa-tui | tui/pyproject.toml | 15 min |
| R4 | Rename sp500_tui/ directory to sawa_tui/ | All files in tui/sp500_tui/ | 30 min |
| R5 | Rename CLI command from sp500 to sawa | pyproject.toml, cli.py | 15 min |
| R6 | Update all imports and references | All Python files | 1 hr |
| R7 | Update documentation | README.md, AGENTS.md | 30 min |

### High Priority

| ID | Task | Files | Effort |
|----|------|-------|--------|
| H1 | Add type stubs to dev dependencies | pyproject.toml | 15 min |
| H2 | Fix AsyncIterator return type in base repository | repositories/base.py, database.py, polygon_prices.py | 30 min |
| H3 | Fix combine.py type errors | processing/combine.py | 1 hr |
| H4 | Fix TUI callable vs Callable type | ai/client.py | 10 min |
| H5 | Fix Literal type for timeframe parameter | services/stock_service.py | 15 min |

### Medium Priority

| ID | Task | Files | Effort |
|----|------|-------|--------|
| M1 | Extract CSV writing utility function | utils/csv_utils.py, coldstart.py, update.py | 1 hr |
| M2 | Add retry logic to S3 client | api/s3.py | 30 min |
| M3 | Add rate limiting to coldstart downloads | coldstart.py | 45 min |
| M4 | Fix linting issues (line length, whitespace) | ai/prompts.py, database.py, glossary.py | 30 min |
| M5 | Remove unused import and f-string issue | database.py, app.py | 5 min |
| M6 | Standardize on psycopg (v3) for all DB operations | database/*.py, repositories/database.py | 3 hr |
| M7 | Standardize on httpx for HTTP requests | api/client.py | 1 hr |
| M8 | Consolidate get_database_url() to single location | utils/config.py, tui/config.py, mcp_server/database.py | 1 hr |

### Low Priority

| ID | Task | Files | Effort |
|----|------|-------|--------|
| L1 | Add input validation for ticker symbols | api/client.py, coldstart.py | 30 min |
| L2 | Extract magic values to constants | coldstart.py, api/client.py, database/load.py | 30 min |
| L3 | Add integration tests for database loader | tests/database/ | 3 hr |
| L4 | Add tests for API clients | tests/api/ | 4 hr |
| L5 | Split AppState into smaller classes | tui/state.py | 3 hr |
| L6 | Add tests for coldstart/update workflows | tests/workflows/ | 4 hr |
| L7 | Use domain models in TUI via converters | tui/models/queries.py, tui/services/ | 2 hr |
| L8 | Apply frozen=True, slots=True to value dataclasses | Multiple files | 1 hr |
| L9 | Remove legacy code | tui/database.py, tui/config.py | 30 min |
| L10 | Standardize logging approach | Multiple files | 1 hr |

---

## Summary

The codebase is functional and follows good practices in many areas (repository pattern, domain models, CLI structure). The main areas needing attention are:

1. **Naming** - Rename from sp500 to sawa throughout
2. **Type safety** - Fix mypy errors to catch bugs early
3. **Technology mixing** - Standardize on psycopg v3 and httpx
4. **Code duplication** - Extract common patterns, consolidate config
5. **Test coverage** - Add tests for API clients and workflows

Estimated total effort to resolve all issues: **~30 hours**

Recommended priority order:
1. R1-R7 (Rename to sawa) - 3 hours - Do first to avoid merge conflicts
2. H1-H5 (High priority type fixes) - 2.5 hours
3. M4-M5 (Quick linting fixes) - 35 min
4. M6-M8 (Standardize libraries) - 5 hours
5. M1-M3 (Code quality improvements) - 2.25 hours
6. L1-L10 (Architecture improvements) - As time permits
