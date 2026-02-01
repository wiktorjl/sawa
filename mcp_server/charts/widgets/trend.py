"""Trend indicator widget."""

from ..themes import Theme, get_theme


class TrendIndicator:
    """
    Render trend indicators showing direction and magnitude of change.
    """

    def __init__(self, theme: Theme | None = None):
        """
        Initialize trend indicator.

        Args:
            theme: Theme to use for styling
        """
        self.theme = theme or get_theme()

    def render(
        self,
        value: float,
        show_value: bool = True,
        is_percent: bool = True,
        decimals: int = 1,
        threshold: float = 0.5,
    ) -> str:
        """
        Render a trend indicator.

        Args:
            value: Change value (positive = up, negative = down)
            show_value: Include the numeric value
            is_percent: If True, format as percentage
            decimals: Number of decimal places
            threshold: Threshold for "neutral" (values within +/- threshold show neutral)

        Returns:
            Colored trend indicator string
        """
        symbols = self.theme.symbols

        # Determine direction
        if value > threshold:
            symbol = symbols.positive
            color = self.theme.colors.positive
            sign = "+"
        elif value < -threshold:
            symbol = symbols.negative
            color = self.theme.colors.negative
            sign = ""
        else:
            symbol = symbols.neutral
            color = self.theme.colors.muted
            sign = ""

        # Build output
        parts = [symbol]

        if show_value:
            if is_percent:
                value_str = f"{sign}{value:.{decimals}f}%"
            else:
                value_str = f"{sign}{value:.{decimals}f}"
            parts.append(value_str)

        result = " ".join(parts)
        return self.theme.colorize(result, color)

    def render_change(
        self,
        old_value: float,
        new_value: float,
        show_absolute: bool = True,
        show_percent: bool = True,
        prefix: str = "",
    ) -> str:
        """
        Render change between two values.

        Args:
            old_value: Previous value
            new_value: Current value
            show_absolute: Show absolute change
            show_percent: Show percentage change
            prefix: Prefix for absolute value (e.g., "$")

        Returns:
            Formatted change string
        """
        abs_change = new_value - old_value

        if old_value != 0:
            pct_change = ((new_value - old_value) / abs(old_value)) * 100
        else:
            pct_change = 0 if new_value == 0 else 100

        # Determine color
        if abs_change > 0:
            color = self.theme.colors.positive
            sign = "+"
            symbol = self.theme.symbols.positive
        elif abs_change < 0:
            color = self.theme.colors.negative
            sign = ""
            symbol = self.theme.symbols.negative
        else:
            color = self.theme.colors.muted
            sign = ""
            symbol = self.theme.symbols.neutral

        parts = [symbol]

        if show_absolute:
            parts.append(f"{sign}{prefix}{abs_change:.2f}")

        if show_percent:
            parts.append(f"({sign}{pct_change:.1f}%)")

        result = " ".join(parts)
        return self.theme.colorize(result, color)

    def render_mini(self, value: float, threshold: float = 0.5) -> str:
        """
        Render minimal trend indicator (just symbol and color).

        Args:
            value: Change value
            threshold: Neutral threshold

        Returns:
            Single colored symbol
        """
        symbols = self.theme.symbols

        if value > threshold:
            return self.theme.positive_text(symbols.positive)
        elif value < -threshold:
            return self.theme.negative_text(symbols.negative)
        else:
            return self.theme.muted_text(symbols.neutral)

    def render_with_sparkline(
        self,
        values: list[float],
        sparkline_width: int = 20,
    ) -> str:
        """
        Render trend with inline sparkline.

        Args:
            values: List of values (oldest to newest)
            sparkline_width: Width of sparkline

        Returns:
            Combined sparkline and trend indicator
        """
        if not values or len(values) < 2:
            return ""

        from ..core.sparkline import Sparkline

        spark = Sparkline(self.theme)
        sparkline_str = spark.render(values, sparkline_width)

        # Calculate overall change
        first = values[0]
        last = values[-1]

        if first != 0:
            change_pct = ((last - first) / abs(first)) * 100
        else:
            change_pct = 0

        trend = self.render(change_pct)

        return f"{sparkline_str} {trend}"
