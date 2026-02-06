# Code Review: Sawa Project

## Overview
The project is well-structured with a clean architecture separating concerns (CLI, repositories, domain models, API client). Overall code quality is good with modern Python practices.

---

## Issues Found

### Critical

#### 1. SQL Injection Risk (`sawa/repositories/database.py:1169`)
```python
conditions.append(f"{indicator} BETWEEN %s AND %s")
```
Uses f-string for SQL column name. While `indicator` is validated against `VALID_INDICATORS`, use `sql.Identifier()` for safety per AGENTS.md guidelines.

#### 2. API Key in URL Params (`sawa/api/client.py:89, 122`)
```python
params["apiKey"] = self.api_key
```
API key is passed in query params. Consider using header authentication for better security.

---

### High

#### 3. Deprecated asyncio API (`sawa/cli.py:438`)
```python
indices = asyncio.get_event_loop().run_until_complete(repo.list_indices())
```
The `run_until_complete` is deprecated. Use `asyncio.run()` instead.

#### 4. Bare `except` Clauses (`sawa/cli.py:100, 135, 168, etc.`)
```python
except Exception as e:
```
Catches all exceptions including `SystemExit` and `KeyboardInterrupt`. Should catch specific exceptions.

---

### Medium

#### 5. Hardcoded Year (`sawa/repositories/database.py:299`)
```python
fiscal_year=row.get("fiscal_year") or 2024,
```
Hardcoded fallback year. Should use a constant or computed value.

#### 6. Inconsistent Import Order (`sawa/repositories/database.py:17-24`)
Imports follow pattern but `psycopg` should come after standard library (per AGENTS.md I rule).

#### 7. Missing Type Hints (`sawa/database/connection.py:97`)
```python
def get_symbols_from_db(conn) -> list[str]:
```
Parameter `conn` lacks type annotation.

#### 8. Mutable Default Arguments (`sawa/api/client.py:67`)
```python
params: dict[str, Any] | None = None,
```
This is actually correct (uses `None` as default), but `params or {}` on line 88 creates new dict each time.

---

### Low

#### 9. Duplicate Logger Setup (`sawa/utils/logging.py:47`)
`logging.basicConfig()` is called every time `setup_logging()` runs, which can cause duplicate log handlers.

#### 10. Magic Numbers (`sawa/repositories/database.py:1441`)
```python
limit = min(limit, 500)
```
`500` should be a named constant.

#### 11. Missing Module-Level Logger (`sawa/api/client.py`)
Should have `logger = logging.getLogger(__name__)` at module level per AGENTS.md.

---

## Positive Findings
- Good use of `frozen=True, slots=True` dataclasses for domain models
- Consistent use of modern type hints (`T | None` not `Optional[T]`)
- Repository pattern properly abstracted with base classes
- Docstrings follow Google style
- Environment variable handling via `python-dotenv`

---

## Recommendations
1. Run `ruff check --fix .` to auto-fix import order issues
2. Replace f-string SQL building with `psycopg.sql` module
3. Update `asyncio.get_event_loop()` to `asyncio.run()`
4. Add explicit exception types to CLI handlers
5. Add `__all__` exports in `__init__.py` files
