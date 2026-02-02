"""Screener logic engine."""

from sp500_tui.models.queries import ScreenerResult


class ScreenerEngine:
    """Handles filtering of the stock universe."""

    def __init__(self, universe: list[ScreenerResult]) -> None:
        self.universe = universe

    def filter(self, query: str) -> tuple[list[ScreenerResult], str]:
        """
        Filter the universe using a python expression.

        Args:
            query: Boolean expression string (e.g. "pe < 15 and yield > 0.03")

        Returns:
            Tuple of (filtered_results, error_message)
        """
        if not query.strip():
            return self.universe, ""

        results = []
        error = ""

        try:
            # Safe namespace for eval
            safe_names = {
                "startswith": str.startswith,
                "endswith": str.endswith,
                "len": len,
            }

            for item in self.universe:
                # Construct locals for this item
                # Using short variable names for ease of typing
                context = {
                    "ticker": item.ticker,
                    "name": item.name,
                    "sector": item.sector,
                    "price": item.price or 0,
                    "cap": item.market_cap or 0,
                    "pe": item.pe or 0,
                    "pb": item.pb or 0,
                    "ps": item.ps or 0,
                    "yield": item.dividend_yield or 0,
                    "dy": item.dividend_yield or 0,  # alias
                    "debt_eq": item.debt_to_equity or 0,
                    "roe": item.roe or 0,
                    "eps": item.eps or 0,
                    "vol": item.volume or 0,
                }

                # Combine with safe builtins
                eval_context = {**safe_names, **context}

                # Eval returns boolean
                if eval(query, {"__builtins__": {}}, eval_context):
                    results.append(item)

        except Exception as e:
            error = f"Error: {e}"
            return [], error

        return results, ""
