# Code Review: Sawa

Review date: 2026-04-24

## Review Method

This review combined one local pass with three focused sub-agent passes:

- Clean code and maintainability: duplication, broad functions, error handling, readability.
- Design and architecture: module boundaries, CLI/API layering, MCP boundaries, database abstraction.
- Consistency, testing, typing, and packaging: public interfaces, dependency metadata, docs drift, quality gates.

## Verification

Commands run from `/home/user/code/sawa`:

- `ruff check .`: failed because `ruff` is not on PATH.
- `.venv/bin/ruff check .`: failed with 39 violations.
- `.venv/bin/pytest -q`: passed, `362 passed in 1.05s`.
- `.venv/bin/mypy sawa/`: failed with 24 errors across 12 files.
- `.venv/bin/mypy mcp_server/`: failed with 19 errors across 7 files.

## Post-Review Fix Status

Fixed on 2026-04-24 in `8f75c9f` (`fix: align refresh ranges and screener filters`):

- Finding 1: Added `scipy` and `statsmodels` to main runtime dependencies.
- Finding 4: Replaced screener filter drift with a `FilterSpec` registry, generated filter
  selects/output aliases from that registry, added sort alias handling, and covered previously
  advertised filters with tests.

Fixed in the current working tree:

- Finding 2: Added wheel package data for `sqlschema/` and `nasdaq1000_symbols.txt`, plus runtime
  fallback resolution for source checkouts and installed packages.
- Finding 3: Implemented database inflation and labor-market repository methods against the current
  wide economy tables, and fixed treasury yield column mapping.
- Finding 12: Added `coldstart --confirm-drop` and threaded it through destructive drop paths.
- Finding 13: Cleaned the current ruff and mypy baseline.
- Finding 15: Updated CLI help and README freshness docs to show the default stock-character work
  and `--skip-character`.

Current verification after fixes:

- `.venv/bin/ruff check .`: passed.
- `.venv/bin/pytest -q`: passed, `377 passed in 0.90s`.
- `.venv/bin/mypy sawa/`: passed.
- `.venv/bin/mypy mcp_server/`: passed.
- `.venv/bin/pyproject-build --wheel --outdir /tmp/sawa-wheel-test`: passed; inspected wheel
  contents include `sawa/sqlschema/` and `sawa/nasdaq1000_symbols.txt`.

Additional fixes landed with the same commit:

- Daily updates now refresh `mv_52week_extremes` when it lags `stock_prices`.
- Weekly economy updates now calculate start dates per economy table instead of using treasury
  yields as the shared anchor for monthly series.
- Weekly market-internals updates now use their own last-loaded date.
- Added focused tests for daily 52-week refresh behavior, weekly economy ranges, and screener
  filter/query construction.

## High Severity

### 1. [Fixed] Clean installs can miss runtime dependencies

Refs: [pyproject.toml](pyproject.toml:12), [sawa/weekly.py](sawa/weekly.py:269), [sawa/calculation/stock_character.py](sawa/calculation/stock_character.py:17), [sawa/calculation/stock_character_scorecard.py](sawa/calculation/stock_character_scorecard.py:14)

`scipy` and `statsmodels` are imported by the stock-character pipeline, and `sawa weekly` runs that pipeline by default. Neither package is declared in the main runtime dependencies.

Impact: a clean install can fail during normal CLI use even though tests pass in the existing environment.

Suggested fix: add these as runtime dependencies, or make stock-character an optional extra and guard the weekly flow with a clear opt-in/error path.

Status: Fixed in `8f75c9f` by adding `scipy>=1.10.0` and `statsmodels>=0.14.0` to
`pyproject.toml`.

### 2. [Fixed] Wheels omit files required by runtime commands

Refs: [pyproject.toml](pyproject.toml:45), [sawa/cli.py](sawa/cli.py:816), [sawa/database/schema.py](sawa/database/schema.py:177), [sawa/utils/symbols.py](sawa/utils/symbols.py:124)

The wheel packages only `sawa`, but runtime code expects repo-root assets such as `sqlschema/` and `nasdaq1000_symbols.txt`.

Impact: installed wheels run outside the source checkout can fail for `coldstart`, schema setup, and NASDAQ symbol loading.

Suggested fix: move required assets under the package, include them as package data, resolve them with `importlib.resources`, and add an install-from-wheel smoke test from a temporary working directory.

Status: Fixed in the current working tree. `pyproject.toml` now force-includes `sqlschema/` and
`nasdaq1000_symbols.txt` under the `sawa` package in wheels, and runtime resource resolution falls
back to the source checkout for editable installs.

### 3. [Fixed] The economy repository exposes methods that always fail

Refs: [sawa/repositories/factory.py](sawa/repositories/factory.py:221), [sawa/repositories/database.py](sawa/repositories/database.py:693), [sawa/repositories/database.py](sawa/repositories/database.py:713)

`RepositoryFactory.get_economy_repository()` exposes a database economy repository, but `get_inflation()` and `get_labor_market()` raise `NotImplementedError` by design because the repository contract no longer matches the schema.

Impact: repository consumers get a valid-looking object that fails for normal economy calls. This also encourages MCP tools to bypass the repository layer with direct SQL.

Suggested fix: align the domain repository contract with the wide economy tables, or remove/split unsupported methods so the factory does not return unusable implementations.

Status: Fixed in the current working tree. The database economy repository now returns
`InflationData` and `LaborMarketData` entries by flattening the current wide tables, and treasury
yield mapping now uses the schema column names.

### 4. [Fixed] The MCP screener accepts filters it cannot query

Refs: [mcp_server/tools/screener.py](mcp_server/tools/screener.py:17), [mcp_server/tools/screener.py](mcp_server/tools/screener.py:270), [mcp_server/tools/screener.py](mcp_server/tools/screener.py:374), [mcp_server/tools/screener.py](mcp_server/tools/screener.py:416)

`VALID_FILTERS` advertises filters such as `sma_5`, `ema_12`, `bb_middle`, `obv`, `daily_range_pct`, and `high_52w_pct`, but the `base_data` CTE and `_get_filter_expression()` do not consistently select/map those fields.

Impact: accepted filters can produce runtime SQL errors, and future filter changes can drift silently.

Suggested fix: replace the separate set and mapper with a single `FilterSpec` registry that defines SQL expression, output alias, sort support, and test coverage for every exposed filter.

Status: Fixed in `8f75c9f`. `mcp_server/tools/screener.py` now uses a single filter registry for
filter validation, SQL expressions, output aliases, and sort aliases. Tests in
`tests/tools/test_screener.py` cover the previously missing filters and 52-week snapshot behavior.

## Medium Severity

### 5. MCP is no longer a thin wrapper around `sawa`

Refs: [mcp_server/server.py](mcp_server/server.py:40), [mcp_server/tools/market_data.py](mcp_server/tools/market_data.py:9), [sawa/repositories/database.py](sawa/repositories/database.py:453), [mcp_server/tools/companies.py](mcp_server/tools/companies.py:85)

The MCP server owns substantial direct SQL behavior and separate row mappings instead of consistently calling `sawa` services/repositories. For example, repository company mapping expects fields like `sector`, `employees`, and `website`, while MCP SQL uses `sic_description`, `total_employees`, and `homepage_url`.

Impact: schema behavior is duplicated outside the core package, so repository tests do not protect MCP behavior and mappings can disagree.

Suggested fix: move shared data access and row mapping into `sawa` services/repositories. Keep MCP focused on schemas, validation, chart rendering, and response formatting.

### 6. Raw `execute_query` is a broad user-facing database API

Refs: [mcp_server/server.py](mcp_server/server.py:594), [mcp_server/server.py](mcp_server/server.py:1993), [mcp_server/database.py](mcp_server/database.py:190)

The MCP server exposes arbitrary read-only SQL as a supported tool. The handler logs optional params but does not pass them to `execute_query`, and automatic row limiting only applies to queries starting with `SELECT`, not `WITH`.

Impact: this expands the public API to the raw database schema, makes behavior harder to version, and allows unbounded CTE queries subject only to statement timeout.

Suggested fix: disable this by default or gate it as an admin/debug tool. If retained, include params in the tool schema and execution call, enforce limits for CTE queries, and consider restricting access to approved read-only views.

### 7. MCP tool registration and dispatch are hand-maintained in one large file

Refs: [mcp_server/server.py](mcp_server/server.py:138), [mcp_server/server.py](mcp_server/server.py:1797)

`mcp_server/server.py` is 2,386 lines and keeps imports, tool metadata, argument defaults, dispatch, and optional chart rendering in separate blocks.

Impact: adding or changing a tool requires synchronized edits in multiple places, increasing drift risk.

Suggested fix: introduce a `ToolSpec` registry with schema, handler, defaults, validation, and renderer fields, then generate `list_tools()` and dispatch from the registry.

### 8. Provider WebSocket client writes directly to PostgreSQL

Refs: [sawa/api/websocket_client.py](sawa/api/websocket_client.py:9), [sawa/api/websocket_client.py](sawa/api/websocket_client.py:13), [sawa/api/websocket_client.py](sawa/api/websocket_client.py:33), [sawa/api/websocket_client.py](sawa/api/websocket_client.py:247)

`PolygonWebSocketClient` takes `database_url`, imports database loading code, and writes buffered bars itself.

Impact: the API layer is coupled to persistence, making the stream client harder to reuse, test, or adapt to another sink.

Suggested fix: have the WebSocket client emit bars to an injected callback/sink. Wire the PostgreSQL sink from the CLI/intraday orchestration layer.

### 9. Orchestration functions mix too many responsibilities

Refs: [sawa/daily.py](sawa/daily.py:211), [sawa/daily.py](sawa/daily.py:287), [sawa/coldstart.py](sawa/coldstart.py:450)

`run_daily()` and `run_coldstart()` compose API clients, raw DB connections, loaders, market-hours logic, technical analysis, FRED data, schema setup, downloads, and mode handling.

Impact: mode interactions are difficult to reason about, and narrow unit tests are hard to write without exercising unrelated behavior.

Suggested fix: split workflows into injected use-case services such as price update, news update, TA update, schema setup, and load-existing-data. Keep CLI functions as config parsing and orchestration only.

### 10. CSV loading/upsert logic is duplicated

Refs: [sawa/database/load.py](sawa/database/load.py:25), [sawa/database/load.py](sawa/database/load.py:98), [sawa/database/loader.py](sawa/database/loader.py:121)

There are two CSV-to-table paths with overlapping primary-key lookup, type conversion, SQL construction, batching, and error handling.

Impact: fixes to CSV parsing or upsert behavior must be made twice and can produce different database results depending on the entry point.

Suggested fix: consolidate value conversion, PK lookup, SQL construction, and batch insertion into one shared loader module.

### 11. Calculation errors are hidden as valid empty results

Refs: [sawa/calculation/stock_character_detect.py](sawa/calculation/stock_character_detect.py:525)

`detect_flags()` catches all exceptions, logs, and returns `[]`.

Impact: downstream scorecards cannot distinguish "no flags detected" from "flag detection failed."

Suggested fix: catch only expected data-shape/numeric exceptions, or return a structured failure status. Let unexpected exceptions propagate to the batch result.

### 12. [Fixed] `coldstart --drop-only` gives an invalid non-interactive remediation

Refs: [sawa/coldstart.py](sawa/coldstart.py:551), [sawa/cli.py](sawa/cli.py:831)

Non-interactive drop-only mode tells users to pass `--confirm-drop`, but the CLI parser only defines `--drop-only`; there is no `--confirm-drop` argument.

Impact: automated/CI use is blocked with an instruction that cannot be followed.

Suggested fix: add `--confirm-drop` and pass it into `run_coldstart`, or remove the message and document that destructive drop mode must be interactive.

Status: Fixed in the current working tree. `--confirm-drop` is now defined by the CLI and passed
through to `run_coldstart` for `--drop-only` and other destructive drop paths.

### 13. [Fixed] Advertised quality gates are currently red

Refs: [pyproject.toml](pyproject.toml:52), [pyproject.toml](pyproject.toml:55)

The repo documents `ruff check .`, `mypy sawa/`, and `mypy mcp_server/`, but all fail in the checked environment. Ruff reports 39 violations. Mypy reports 24 errors for `sawa/` and 19 for `mcp_server/`.

Impact: linting and typing cannot protect consistency until the baseline is clean.

Suggested fix: fix or explicitly baseline the current violations, declare missing typing deps, and add the intended quality commands to CI.

Status: Fixed in the current working tree. `.venv/bin/ruff check .`, `.venv/bin/mypy sawa/`, and
`.venv/bin/mypy mcp_server/` all pass.

## Low Severity

### 14. Public version metadata is inconsistent

Refs: [pyproject.toml](pyproject.toml:7), [sawa/__init__.py](sawa/__init__.py:23)

Package metadata says `0.3.0`, while the public module reports `__version__ = "0.2.0"`.

Suggested fix: derive `__version__` from `importlib.metadata.version("sawa")`, or add a test that enforces synchronization.

### 15. [Fixed] Weekly documentation omits default stock-character work

Refs: [README.md](README.md:114), [sawa/cli.py](sawa/cli.py:943), [sawa/weekly.py](sawa/weekly.py:269)

Docs and CLI help describe `sawa weekly` as economy/news/corporate-actions freshness, but the default flow also runs stock-character classification.

Impact: users can get an expensive extra step and extra dependency surface unexpectedly.

Suggested fix: document `--skip-character` prominently or make stock-character classification opt-in.

Status: Fixed in the current working tree. CLI help and README examples now describe weekly stock
character classification and show `weekly --skip-character`.

### 16. MCP packaging relies on undeclared or unconstrained dependencies

Refs: [mcp_server/pyproject.toml](mcp_server/pyproject.toml:12), [mcp_server/server.py](mcp_server/server.py:18)

The MCP package depends on unversioned `sawa` and directly imports `dotenv` without declaring `python-dotenv` in its own dependencies.

Suggested fix: constrain `sawa` to a compatible version/range and declare direct dependencies in the MCP package metadata.

## Residual Risk

No live database or Polygon/FRED credentials were used during this review, so DB-backed command behavior was assessed from code and static checks rather than end-to-end execution.
