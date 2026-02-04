"""Screener service for parsing and executing stock screening queries."""

import logging
import re
from dataclasses import dataclass
from typing import Any

from sawa_web.database.connection import execute_query

logger = logging.getLogger(__name__)


class ScreenerError(Exception):
    """Error in screener query parsing or execution."""
    pass


# Map query variables to database columns
VARIABLE_MAPPING = {
    # Text filters
    "ticker": ("c.ticker", "text"),
    "name": ("c.name", "text"),
    "sector": ("c.sic_description", "text"),

    # Price and market data
    "price": ("sp.close", "numeric"),
    "cap": ("c.market_cap", "numeric"),
    "market_cap": ("c.market_cap", "numeric"),
    "volume": ("sp.volume", "numeric"),

    # Valuation ratios
    "pe": ("fr.price_to_earnings", "numeric"),
    "pb": ("fr.price_to_book", "numeric"),
    "ps": ("fr.price_to_sales", "numeric"),

    # Dividends
    "yield": ("fr.dividend_yield", "numeric"),
    "dy": ("fr.dividend_yield", "numeric"),
    "dividend_yield": ("fr.dividend_yield", "numeric"),

    # Leverage
    "debt_eq": ("fr.debt_to_equity", "numeric"),
    "debt_equity": ("fr.debt_to_equity", "numeric"),

    # Profitability
    "roe": ("fr.return_on_equity", "numeric"),
    "roa": ("fr.return_on_assets", "numeric"),
    "eps": ("fr.earnings_per_share", "numeric"),

    # Other
    "current": ("fr.current", "numeric"),
    "current_ratio": ("fr.current", "numeric"),
}

# Supported operators
OPERATORS = {
    "<": "<",
    ">": ">",
    "<=": "<=",
    ">=": ">=",
    "==": "=",
    "=": "=",
    "!=": "<>",
    "<>": "<>",
}


@dataclass
class QueryCondition:
    """A single condition in a screener query."""
    variable: str
    operator: str
    value: Any
    column: str
    value_type: str


def tokenize_query(query: str) -> list[str]:
    """Tokenize a query string into components."""
    # Normalize whitespace
    query = query.strip().lower()

    # Replace 'and' and 'or' with special markers
    query = re.sub(r'\band\b', ' AND ', query)
    query = re.sub(r'\bor\b', ' OR ', query)

    # Split on operators while keeping them
    # Match: <=, >=, ==, !=, <>, <, >, =
    pattern = r'(<=|>=|==|!=|<>|<|>|=|\s+AND\s+|\s+OR\s+)'
    tokens = re.split(pattern, query)

    # Clean up tokens
    tokens = [t.strip() for t in tokens if t.strip()]

    return tokens


def parse_query(query: str) -> tuple[list[QueryCondition], list[str]]:
    """
    Parse a screener query string into conditions and logical operators.

    Args:
        query: Query string like "pe < 15 and yield > 0.03"

    Returns:
        Tuple of (conditions list, logical operators list)

    Raises:
        ScreenerError: If query is invalid
    """
    if not query or not query.strip():
        raise ScreenerError("Query cannot be empty")

    tokens = tokenize_query(query)
    conditions = []
    logical_ops = []

    i = 0
    while i < len(tokens):
        # Expect: variable operator value [AND/OR]
        if i >= len(tokens):
            break

        # Get variable
        var = tokens[i].lower()
        if var in ("and", "or"):
            logical_ops.append(var.upper())
            i += 1
            continue

        if var not in VARIABLE_MAPPING:
            raise ScreenerError(f"Unknown variable: '{var}'. Valid variables: {', '.join(sorted(VARIABLE_MAPPING.keys()))}")

        column, value_type = VARIABLE_MAPPING[var]

        # Get operator
        if i + 1 >= len(tokens):
            raise ScreenerError(f"Missing operator after '{var}'")

        op = tokens[i + 1]
        if op.upper() in ("AND", "OR"):
            raise ScreenerError(f"Missing operator after '{var}'")

        if op not in OPERATORS:
            raise ScreenerError(f"Invalid operator: '{op}'. Valid operators: <, >, <=, >=, ==, !=")

        sql_op = OPERATORS[op]

        # Get value
        if i + 2 >= len(tokens):
            raise ScreenerError(f"Missing value after '{var} {op}'")

        value_str = tokens[i + 2]

        # Handle potential logical operator being parsed as value
        if value_str.upper() in ("AND", "OR"):
            raise ScreenerError(f"Missing value after '{var} {op}'")

        # Parse value based on type
        if value_type == "numeric":
            try:
                # Handle percentage values (e.g., "3%" -> 0.03)
                if value_str.endswith("%"):
                    value = float(value_str[:-1]) / 100
                else:
                    value = float(value_str)
            except ValueError:
                raise ScreenerError(f"Invalid numeric value: '{value_str}'")
        else:
            # Text value - remove quotes if present
            value = value_str.strip("'\"")

        conditions.append(QueryCondition(
            variable=var,
            operator=sql_op,
            value=value,
            column=column,
            value_type=value_type,
        ))

        i += 3

        # Check for logical operator
        if i < len(tokens):
            if tokens[i].upper() in ("AND", "OR"):
                logical_ops.append(tokens[i].upper())
                i += 1

    if not conditions:
        raise ScreenerError("No valid conditions found in query")

    return conditions, logical_ops


def build_sql_query(conditions: list[QueryCondition], logical_ops: list[str]) -> tuple[str, list[Any]]:
    """
    Build a SQL query from parsed conditions.

    Returns:
        Tuple of (SQL query string, parameter values)
    """
    base_query = """
        SELECT DISTINCT
            c.ticker,
            c.name,
            c.sic_description as sector,
            c.market_cap,
            sp.close as price,
            CASE WHEN sp.open > 0 THEN ((sp.close - sp.open) / sp.open * 100) ELSE 0 END as change_percent,
            fr.price_to_earnings as pe,
            fr.price_to_book as pb,
            fr.dividend_yield,
            fr.return_on_equity as roe,
            fr.debt_to_equity
        FROM companies c
        LEFT JOIN LATERAL (
            SELECT open, close, volume
            FROM stock_prices
            WHERE ticker = c.ticker
            ORDER BY date DESC
            LIMIT 1
        ) sp ON true
        LEFT JOIN LATERAL (
            SELECT price_to_earnings, price_to_book, price_to_sales, dividend_yield,
                   return_on_equity, return_on_assets, debt_to_equity, earnings_per_share,
                   beta, current
            FROM financial_ratios
            WHERE ticker = c.ticker
            ORDER BY date DESC
            LIMIT 1
        ) fr ON true
        WHERE 1=1
    """

    where_clauses = []
    params = []
    param_idx = 1

    for i, cond in enumerate(conditions):
        if cond.value_type == "text":
            # Use ILIKE for text matching
            if cond.operator in ("=", "<>"):
                clause = f"{cond.column} ILIKE ${param_idx}"
                params.append(f"%{cond.value}%")
            else:
                clause = f"{cond.column} {cond.operator} ${param_idx}"
                params.append(cond.value)
        else:
            clause = f"{cond.column} {cond.operator} ${param_idx}"
            params.append(cond.value)

        # Add NOT NULL check for numeric columns
        if cond.value_type == "numeric":
            clause = f"({cond.column} IS NOT NULL AND {clause})"

        where_clauses.append(clause)
        param_idx += 1

    # Combine conditions with logical operators
    if where_clauses:
        combined = where_clauses[0]
        for i, clause in enumerate(where_clauses[1:]):
            op = logical_ops[i] if i < len(logical_ops) else "AND"
            combined = f"({combined}) {op} ({clause})"

        base_query += f" AND ({combined})"

    base_query += " ORDER BY c.market_cap DESC NULLS LAST LIMIT 100"

    return base_query, params


async def execute_screener(query: str) -> list[dict]:
    """
    Execute a screener query and return matching stocks.

    Args:
        query: Query string like "pe < 15 and yield > 0.03"

    Returns:
        List of matching stock dicts

    Raises:
        ScreenerError: If query is invalid or execution fails
    """
    conditions, logical_ops = parse_query(query)
    sql, params = build_sql_query(conditions, logical_ops)

    try:
        results = await execute_query(sql, *params)
        return results or []
    except Exception as e:
        logger.error(f"Screener query failed: {e}")
        raise ScreenerError(f"Query execution failed: {e}")


def get_query_help() -> dict:
    """Get help information for the screener query syntax."""
    return {
        "variables": {
            "Price & Market": ["price", "cap/market_cap", "volume"],
            "Valuation": ["pe", "pb", "ps"],
            "Dividends": ["yield/dy/dividend_yield"],
            "Profitability": ["roe", "roa", "eps"],
            "Leverage": ["debt_eq/debt_equity", "current/current_ratio"],
            "Text Filters": ["ticker", "name", "sector"],
        },
        "operators": ["<", ">", "<=", ">=", "==", "!="],
        "logical": ["and", "or"],
        "examples": [
            ("pe < 15 and yield > 0.03", "Low P/E dividend payers"),
            ("roe > 0.15 and debt_eq < 1", "High ROE, low debt"),
            ("cap > 100000000000", "Mega-cap stocks (>$100B)"),
            ("sector = technology", "Technology sector"),
            ("pe < 20 and pb < 3 and yield > 0.02", "Value stocks"),
        ],
    }
