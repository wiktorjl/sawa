"""Sparkline generation for inline trend visualization."""

from collections.abc import Sequence

from ..themes import Theme, get_theme


class Sparkline:
    """
    Generate sparkline charts from numeric data.

    Uses Unicode block characters (or ASCII fallback) to create
    compact inline visualizations of trends.
    """

    def __init__(self, theme: Theme | None = None):
        """
        Initialize sparkline generator.

        Args:
            theme: Theme to use for symbols. If None, uses default theme.
        """
        self.theme = theme or get_theme()
        self._chars = self.theme.symbols.sparkline

    def render(
        self,
        values: Sequence[float | int | None],
        width: int | None = None,
        show_minmax: bool = False,
        colorize: bool = True,
    ) -> str:
        """
        Render a sparkline from values.

        Args:
            values: Sequence of numeric values (None values are skipped)
            width: Target width (if fewer than values, will sample)
            show_minmax: Include min/max labels
            colorize: Apply theme colors

        Returns:
            Sparkline string
        """
        # Filter out None values
        clean_values = [v for v in values if v is not None]

        if not clean_values:
            return ""

        # Sample if needed
        if width and len(clean_values) > width:
            clean_values = self._sample(clean_values, width)

        # Get min/max for normalization
        min_val = min(clean_values)
        max_val = max(clean_values)
        val_range = max_val - min_val

        # Build sparkline
        chars = []
        num_levels = len(self._chars)

        for val in clean_values:
            if val_range == 0:
                # All values are the same
                level = num_levels // 2
            else:
                # Normalize to 0-1, then scale to char levels
                normalized = (val - min_val) / val_range
                level = int(normalized * (num_levels - 1))
                level = max(0, min(num_levels - 1, level))

            chars.append(self._chars[level])

        sparkline = "".join(chars)

        # Colorize if requested
        if colorize:
            sparkline = self.theme.colorize(sparkline, self.theme.colors.sparkline)

        # Add min/max labels if requested
        if show_minmax:
            min_str = self._format_value(min_val)
            max_str = self._format_value(max_val)
            sparkline = f"{min_str} {sparkline} {max_str}"

        return sparkline

    def render_with_trend(
        self,
        values: Sequence[float | int | None],
        width: int | None = None,
    ) -> tuple[str, str]:
        """
        Render sparkline with trend indicator.

        Args:
            values: Sequence of numeric values
            width: Target width

        Returns:
            Tuple of (sparkline, trend_indicator)
        """
        clean_values = [v for v in values if v is not None]

        if len(clean_values) < 2:
            return self.render(values, width), ""

        # Calculate trend
        first = clean_values[0]
        last = clean_values[-1]

        if first == 0:
            change_pct = 0.0
        else:
            change_pct = ((last - first) / abs(first)) * 100

        # Determine trend symbol and color
        if change_pct > 0.5:
            symbol = self.theme.symbols.positive
            color = self.theme.colors.positive
            sign = "+"
        elif change_pct < -0.5:
            symbol = self.theme.symbols.negative
            color = self.theme.colors.negative
            sign = ""
        else:
            symbol = self.theme.symbols.neutral
            color = self.theme.colors.muted
            sign = ""

        trend = f"{symbol} {sign}{change_pct:.1f}%"
        trend = self.theme.colorize(trend, color)

        return self.render(values, width), trend

    def _sample(self, values: list[float | int], target_width: int) -> list[float | int]:
        """Sample values to fit target width."""
        if len(values) <= target_width:
            return values

        step = len(values) / target_width
        sampled = []
        for i in range(target_width):
            idx = int(i * step)
            sampled.append(values[idx])

        return sampled

    def _format_value(self, value: float | int) -> str:
        """Format a value for display."""
        if abs(value) >= 1_000_000_000:
            return f"{value / 1_000_000_000:.1f}B"
        elif abs(value) >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        elif abs(value) >= 1_000:
            return f"{value / 1_000:.1f}K"
        elif abs(value) >= 1:
            return f"{value:.1f}"
        else:
            return f"{value:.2f}"


def sparkline(
    values: Sequence[float | int | None],
    width: int | None = None,
    theme: Theme | None = None,
) -> str:
    """
    Convenience function to render a sparkline.

    Args:
        values: Sequence of numeric values
        width: Target width
        theme: Theme to use

    Returns:
        Sparkline string
    """
    return Sparkline(theme).render(values, width)
