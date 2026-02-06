# Code Review: Sawa - S&P 500 Data Downloader (opencode/xAI)
Date: Fri Feb 06 2026

## Repository Status
- Branch: master (ahead of origin/master by 19 commits)
- Untracked: CR_MINIMAX.md, docs/
- Dev env: .venv active, deps installed (ruff 0.14.14, mypy 1.19.1, pytest-cov 7.0.0)

## Linting (Ruff)
```
ruff check .: 30 errors (28 after --fix)
```
**Key issues**:
- E402: Module imports after `load_dotenv()` in `mcp_server/server.py` (27+ lines)
- E501: Long lines (>100 chars) in `mcp_server/tools/{screener.py,sectors.py}`, `sawa/api/websocket_client.py:198`, `sawa/coldstart.py:616`
- W291: Trailing whitespace in `sawa/{coldstart.py,database/intraday_load.py}`

Fix: Move imports to top; manual trim lines/whitespace.

## Type Checking (Mypy)
```
sawa/: 40 errors (16 files)
mcp_server/: 51 errors (7 files)
```
**Common**:
- `no-any-return`, union attrs (None.send/index), arg-type (int vs enum/str|None)
- Missing stubs: pytz, requests, tomli (`pip install types-*`)
- Files: api/websocket_client.py, ta_backfill.py, charts/renderers/*.py, server.py

## Tests & Coverage
```
pytest --cov=sawa: 108 passed, 15% coverage (htmlcov/)
- High: calculation/ta_engine.py (92%), domain/models.py (100%)
- Low/Zero: CLI/scripts (cli.py 0%), most repos (33%), mcp_server (0 tests)
```
mcp_server/: No tests collected.

## Codebase Analysis
```
# Structure (from explore agent)
- sawa/: Core (api, db, repos, utils, CLI)
- mcp_server/: MCP tools/charts/server.py
- tests/: Unit-focused (low cov)
- Strengths: Modular, secure env keys, psycopg.sql, logging, rate-limit
- Issues: Bugs (NPE/index), perf (scans), cov gaps, unused code
```

## Recommendations
1. **Fix lint/types**: `ruff --fix`; install stubs; resolve mypy stepwise (api/ta first)
2. **Tests**: Target 80% cov; add mcp_server/integration/CLI; `--cov-fail-under=80`
3. **Style**: Pre-commit (ruff/mypy); enforce AGENTS.md (unions/docstrings)
4. **Perf/Sec**: Full psycopg.sql; indexes for screeners; profile coldstart
5. **Next**: `git add . && git commit -m "fix: lint trailing spaces"` (ask user); CI workflows
6. **Push**: After fixes/tests, `git push`

Overall: Solid foundation (modern Py, secure), needs type/lint/tests polish for prod.
