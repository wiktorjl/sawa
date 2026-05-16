"""Shared helper for filtering MCP query results by index membership.

Replaces the per-index ``if index == "sp500": ... elif index == "nasdaq_listed":
...`` boilerplate previously duplicated across movers, screener,
corporate_actions, etc. With this helper any index code in the
``indices`` table is supported automatically — adding new indices
(nasdaq100, dow30, mag7, ...) no longer requires touching every tool.
"""

from typing import Any

from psycopg import sql


def build_index_filter(
    index: str,
    table_alias: str,
    params: dict[str, Any],
    *,
    param_name: str = "index_code",
) -> sql.Composable:
    """
    Build a SQL fragment that filters ``<table_alias>.ticker`` to members of
    a given index, or returns an empty fragment when no filtering is desired.

    The fragment looks up the index by code in the ``indices`` table at
    query time, so any code present in ``indices`` works without code
    changes here.

    Args:
        index: Index code (e.g., ``"sp500"``, ``"nasdaq100"``, ``"mag7"``) or
            the sentinel ``"all"`` / ``"both"`` for no filtering. Falsy
            values also produce no filter.
        table_alias: Table alias of the outer query whose ``.ticker``
            column is being filtered (e.g., ``"c"`` for companies,
            ``"d"`` for dividends, ``"s"`` for stock_splits).
        params: Query parameter dict to mutate with the bound value of
            ``index`` under ``param_name`` when filtering is applied.
        param_name: Key under which the index code is stored in ``params``.
            Override if the surrounding query already uses
            ``"index_code"`` for something else.

    Returns:
        A ``psycopg.sql.SQL`` fragment, either empty or of the form
        ``AND <alias>.ticker IN (SELECT ... WHERE i.code = %(<param_name>)s)``.
    """
    if not index or index in ("all", "both"):
        return sql.SQL("")
    params[param_name] = index
    return sql.SQL(
        f"AND {table_alias}.ticker IN ("
        f"SELECT ic.ticker FROM index_constituents ic "
        f"JOIN indices i ON ic.index_id = i.id "
        f"WHERE i.code = %({param_name})s)"
    )
