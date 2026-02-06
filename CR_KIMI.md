# Code Review Report - Sawa

**Date:** February 6, 2026  
**Reviewer:** Kimi  
**Scope:** sawa/, mcp_server/, tests/

---

## Executive Summary

The Sawa codebase is a well-structured Python package for downloading and managing S&P 500 market data. It demonstrates good architectural patterns including repository pattern, domain-driven design, and clear separation between CLI tools and MCP server components. However, there are significant issues requiring attention:

- **40 mypy errors** in sawa/
- **51 mypy errors** in mcp_server/
- **15+ ruff linting issues** (E402 import ordering)
- **1 critical security vulnerability** (SQL injection)
- Multiple code quality and maintainability issues

**Overall Assessment:** Good foundation with critical issues that need immediate attention.

---

## 1. Critical Issues

### 1.1 SQL Injection Vulnerability

**Location:** `sawa/repositories/database.py:1181`

**Issue:** User-controlled `indicator` parameter is interpolated directly into SQL query:

```python
# VULNERABLE CODE (Line 1169, 1172, 1175)
indicator = key.replace("_", "").upper()  # User input
conditions.append(f"{indicator} BETWEEN %s AND %s")  # SQL injection risk
```

**Risk:** An attacker could inject arbitrary SQL through indicator names like `"price; DROP TABLE companies; --"`.

**Fix:** Validate indicator against whitelist before interpolation:

```python
VALID_INDICATORS = {"RSI_14", "RSI_21", "MACD_HISTOGRAM", "SMA_50", ...}
if indicator not in VALID_INDICATORS:
    raise ValueError(f"Invalid indicator: {indicator}")
```

**Priority:** CRITICAL - Fix immediately

---

## 2. Type Safety Issues

### 2.1 Missing Type Stubs

**Locations:**
- `sawa/utils/market_hours.py:3` - pytz
- `sawa/utils/symbols.py:7` - requests  
- `sawa/utils/xdg.py:23` - tomli

**Fix:** Add to dev dependencies:

```toml
[project.optional-dependencies]
dev = [
    "types-pytz",
    "types-requests",
    "types-toml",
]
```

### 2.2 WebSocket Client Type Errors

**Location:** `sawa/api/websocket_client.py:77-288`

**Issues:**
- Line 77: `self.ws` declared as `None` but assigned `ClientConnection`
- Lines 82, 85, 90, 114, 117: Attribute access on potentially `None`
- Line 288: `None` is not async iterable

**Fix:** Use proper Optional typing with null checks:

```python
async def connect(self) -> None:
    self.ws = await connect(self.ws_url)
    if self.ws is None:
        raise ConnectionError("Failed to establish WebSocket connection")
```

### 2.3 Chart Renderer Type Errors

**Location:** `mcp_server/charts/renderers/`

**Pattern:** All chart renderers have incompatible list types:

```python
# prices.py:56-75
closes: list[Any | None]  # Database returns nullable values
# But functions expect: list[float]
```

**Fix:** Filter out None values before calculations:

```python
closes = [row["close"] for row in data if row["close"] is not None]
```

### 2.4 Tuple Indexing Errors

**Locations:**
- `sawa/coldstart.py:515, 521, 610`
- `sawa/daily.py:298`
- `sawa/ta_backfill.py:231-268`

**Issue:** Attempting to index `tuple | None` from `cursor.fetchone()` without null checks.

**Fix:**

```python
row = cursor.fetchone()
if row is None:
    return None
value = row[0]  # Safe now
```

### 2.5 AsyncClient Params Type Error

**Location:** `sawa/api/async_client.py:95`

**Issue:** `params` dict uses `object` type, incompatible with httpx `QueryParams`.

**Fix:** Use `dict[str, str | int | float | bool | None]` for params.

---

## 3. Code Quality Issues

### 3.1 Import Ordering (E402)

**Location:** `mcp_server/server.py:27-58`

**Issue:** Module-level imports not at top due to `load_dotenv()` call before imports.

**Fix:** Move `load_dotenv()` after imports or use different pattern:

```python
# Before (incorrect)
from dotenv import load_dotenv
_env_file = Path(__file__).parent / ".env"
load_dotenv(_env_file)
from .charts.config import ChartDetail  # E402

# After (correct)
from dotenv import load_dotenv
from .charts.config import ChartDetail
# ... other imports ...
_env_file = Path(__file__).parent / ".env"
load_dotenv(_env_file)
```

### 3.2 Duplicated Code Patterns

**Pattern 1: CSV Writing (6+ occurrences)**

**Locations:**
- `sawa/coldstart.py:158-160, 285-291`
- `sawa/update.py:89-92`

```python
# Repeated pattern
all_fields: set[str] = set()
for record in data:
    all_fields.update(record.keys())
fieldnames = sorted(all_fields)
with open(filepath, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(data)
```

**Fix:** Extract to `sawa/utils/csv_utils.py`:

```python
def write_csv_auto_fields(filepath: Path, data: list[dict[str, Any]]) -> None:
    """Write CSV with auto-detected fields."""
    if not data:
        return
    fieldnames = sorted(set().union(*(d.keys() for d in data)))
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
```

**Pattern 2: Ticker Flattening (3+ occurrences)**

**Locations:**
- `sawa/coldstart.py:158-160`
- `sawa/coldstart.py:285-291`
- `sawa/update.py:89-92`

```python
if "tickers" in record and isinstance(record["tickers"], list):
    record["tickers"] = ",".join(record["tickers"])
```

**Pattern 3: Nested Field Flattening**

**Locations:**
- `sawa/coldstart.py:285-291`

Repeated logic for flattening address/branding fields.

### 3.3 Credential Retrieval Duplication

**Location:** `sawa/cli.py` (15+ functions)

**Pattern:** Every command repeats:

```python
api_key = os.environ.get("POLYGON_API_KEY")
if not api_key:
    logger.error("POLYGON_API_KEY not set")
    return 1
```

**Fix:** Create reusable credential helper:

```python
from functools import wraps
from typing import TypeVar, ParamSpec

P = ParamSpec("P")
T = TypeVar("T")

def require_env(var_name: str):
    def decorator(func: Callable[P, T]) -> Callable[P, T | int]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | int:
            if not os.environ.get(var_name):
                logger.error(f"{var_name} not set")
                return 1
            return func(*args, **kwargs)
        return wrapper
    return decorator

@require_env("POLYGON_API_KEY")
def cmd_coldstart(args: argparse.Namespace) -> int:
    ...
```

### 3.4 Async/Sync Wrapper Boilerplate

**Location:** `sawa/repositories/database.py` (20+ methods)

**Pattern:** Every async method uses identical wrapper:

```python
async def get_prices(self, ...) -> list[StockPrice]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, self._get_prices_sync, ...)
```

**Fix:** Use a decorator or metaclass to auto-generate async wrappers.

---

## 4. Error Handling Issues

### 4.1 Bare Exception Handling

**Locations:**
- `sawa/coldstart.py:70` - `_check_date_already_downloaded()`
- `sawa/coldstart.py:194, 209` - download loops with generic `except Exception`
- `sawa/update.py:95, 128` - economy/fundamental downloads
- `sawa/cli.py` - most command handlers

**Issue:** Catching generic `Exception` hides bugs and makes debugging difficult.

**Fix:** Catch specific exceptions:

```python
try:
    data = download_func()
except requests.exceptions.RequestException as e:
    logger.error(f"Network error: {e}")
    return None
except json.JSONDecodeError as e:
    logger.error(f"Invalid JSON: {e}")
    return None
```

### 4.2 Silent Error Swallowing

**Location:** `sawa/coldstart.py:70`

```python
except Exception:
    return False  # Silently ignores all errors
```

**Fix:** At minimum log the error:

```python
except Exception as e:
    logger.warning(f"Could not check downloaded date: {e}")
    return False
```

---

## 5. Security Issues

### 5.1 API Key Exposure Risk

**Location:** `sawa/api/client.py:89, 122, 149`

**Issue:** API key added to URL params - could be logged if URL logging is added.

**Fix:** Use headers instead of query params for API keys (if API supports it) or ensure API keys are redacted from logs.

### 5.2 Temporary File Handling

**Location:** `sawa/api/s3.py:82-85, 200-203`

**Issue:** Uses `delete=False` and bare `except OSError: pass` for cleanup.

**Current:**

```python
try:
    os.unlink(tmp_path)
except OSError:
    pass
```

**Fix:** Use context manager pattern:

```python
from contextlib import contextmanager

@contextmanager
def temp_file():
    fd, path = tempfile.mkstemp()
    try:
        os.close(fd)
        yield path
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
```

---

## 6. Performance Issues

### 6.1 No Connection Pooling

**Location:** `sawa/repositories/database.py:52-61`

**Issue:** Creates new connection for every query.

**Current:**

```python
def _get_connection(self) -> psycopg2.extensions.connection:
    return psycopg2.connect(**self.connection_params)
```

**Fix:** Use connection pool (psycopg2.pool or psycopg Pool):

```python
from psycopg2 import pool

class DatabaseRepository:
    def __init__(self):
        self._pool = psycopg2.pool.SimpleConnectionPool(1, 10, **self.connection_params)
    
    def _get_connection(self):
        return self._pool.getconn()
```

### 6.2 Inefficient Date Iteration

**Locations:**
- `sawa/coldstart.py:94-121`
- `sawa/update.py:38-58`

**Current:**

```python
current = start_date
while current <= end_date:
    # process current
    current += timedelta(days=1)  # Inefficient
```

**Fix:** Use pandas or date range generation:

```python
import pandas as pd
for current in pd.date_range(start_date, end_date):
    # process current
```

### 6.3 Full Pagination into Memory

**Location:** `sawa/api/client.py:102-154`

**Issue:** `get_paginated()` loads all pages into memory.

**Fix:** Use generator for large result sets:

```python
def get_paginated_stream(self, ...) -> Iterator[dict]:
    """Stream paginated results without loading all into memory."""
    while url:
        data = self.get_single(url, params)
        yield from data.get("results", [])
        url = data.get("next_url")
```

---

## 7. Architecture Issues

### 7.1 Mixed Database Drivers

**Observation:** Codebase uses psycopg (v3) in some places, psycopg2 (v2) in others.

**Recommendation:** Standardize on psycopg v3 for all new code. Create migration plan for psycopg2 code.

### 7.2 Duplicate get_database_url()

**Locations:**
- `sawa/utils/config.py:44`
- `sawa/repositories/config.py:96`  
- `mcp_server/database.py:20`

**Fix:** Single implementation in shared package, imported by all.

### 7.3 Hardcoded Magic Values

**Locations:**
- `sawa/repositories/database.py:299` - `fiscal_year=row.get("fiscal_year") or 2024`
- `sawa/coldstart.py:194, 209` - `time.sleep(0.5)`
- `sawa/api/client.py:59, 146` - `timeout=30`

**Fix:** Move to constants or configuration:

```python
# constants.py
DEFAULT_TIMEOUT = 30
DEFAULT_FISCAL_YEAR = date.today().year
RATE_LIMIT_DELAY = 0.5
```

---

## 8. Test Coverage Issues

### 8.1 Missing Unit Tests

**No tests for:**
- `sawa/api/client.py` - REST API client
- `sawa/api/s3.py` - S3 download client
- `sawa/api/async_client.py` - Async REST client
- `sawa/coldstart.py` - Main workflow
- `sawa/update.py` - Update workflow
- `sawa/daily.py` - Daily operations
- `sawa/intraday.py` - Intraday streaming

### 8.2 Integration Tests Needed

**Missing:**
- Database loader integration tests
- End-to-end coldstart workflow tests
- API client integration tests (mocked)

---

## 9. Specific File Issues

### 9.1 sawa/cli.py

| Issue | Line | Severity |
|-------|------|----------|
| Missing encoding in open() | 200 | Medium |
| Deprecated asyncio.get_event_loop() | 439, 476 | Medium |
| Imports inside functions | 146, 313, 427, 509 | Low |
| Generic Exception handling | Multiple | Medium |

### 9.2 sawa/coldstart.py

| Issue | Line | Severity |
|-------|------|----------|
| Generic Exception handler | 70 | Medium |
| Imports inside functions | 223, 238 | Low |
| Hardcoded sleep values | 194, 209 | Low |
| Tuple indexing without null check | 515, 521, 610 | High |

### 9.3 sawa/repositories/database.py

| Issue | Line | Severity |
|-------|------|----------|
| SQL Injection vulnerability | 1181 | CRITICAL |
| No connection pooling | 52-61 | Medium |
| Hardcoded fiscal year default | 299 | Low |
| NotImplementedError instead of proper impl | 941 | Low |

### 9.4 mcp_server/server.py

| Issue | Line | Severity |
|-------|------|----------|
| E402 import ordering | 27-58 | Low |
| Type incompatible assignments | 1286-1574 | Medium |

---

## 10. Recommendations by Priority

### Critical (Fix Immediately)

1. **SQL Injection** - `sawa/repositories/database.py:1181`
   - Add indicator whitelist validation
   - Estimated effort: 30 minutes

### High Priority (Fix This Week)

2. **Tuple null checks** - `coldstart.py`, `daily.py`, `ta_backfill.py`
   - Add null checks before indexing
   - Estimated effort: 1 hour

3. **Chart renderer null filtering** - `mcp_server/charts/renderers/`
   - Filter None values from price data
   - Estimated effort: 2 hours

4. **Import ordering** - `mcp_server/server.py`
   - Move load_dotenv() after imports
   - Estimated effort: 15 minutes

### Medium Priority (Fix Next Sprint)

5. **Extract CSV utility** - Create `sawa/utils/csv_utils.py`
   - Reduce duplication in coldstart/update
   - Estimated effort: 1 hour

6. **Add connection pooling** - `sawa/repositories/database.py`
   - Implement connection pool
   - Estimated effort: 2 hours

7. **Specific exception handling** - Multiple files
   - Replace bare `except Exception` with specific types
   - Estimated effort: 3 hours

8. **Credential helper** - `sawa/cli.py`
   - Extract common credential retrieval pattern
   - Estimated effort: 1 hour

### Low Priority (When Convenient)

9. **Add type stubs** - `pyproject.toml`
   - Install types-pytz, types-requests
   - Estimated effort: 15 minutes

10. **Move magic values to constants**
    - Extract hardcoded values
    - Estimated effort: 2 hours

11. **Standardize database drivers**
    - Migrate remaining psycopg2 to psycopg v3
    - Estimated effort: 4 hours

12. **Add comprehensive tests**
    - API client tests, integration tests
    - Estimated effort: 8 hours

---

## Summary Statistics

| Category | Count |
|----------|-------|
| Critical Issues | 1 |
| High Priority | 3 |
| Medium Priority | 4 |
| Low Priority | 4 |
| Total mypy errors (sawa) | 40 |
| Total mypy errors (mcp_server) | 51 |
| Total ruff issues | 15+ |

**Estimated total effort to resolve all issues:** 25-30 hours

**Recommended order:**
1. Fix critical SQL injection (30 min)
2. Fix high priority type safety issues (3 hours)
3. Clean up linting issues (1 hour)
4. Address medium priority code quality (7 hours)
5. Low priority improvements as time permits

---

## Positive Observations

1. **Good architectural patterns** - Repository pattern properly implemented
2. **Domain models** - Immutable dataclasses with `frozen=True, slots=True`
3. **SQL safety** - Most queries use parameterized statements correctly
4. **Documentation** - Good docstrings throughout
5. **Configuration** - Environment-based config with .env support
6. **Separation of concerns** - Clear split between sawa and mcp_server
