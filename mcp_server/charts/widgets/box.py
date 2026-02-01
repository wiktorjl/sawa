"""Box drawing widget for framing content."""

from ..core.colors import pad_to_width, visible_len
from ..themes import Theme, get_theme


class Box:
    """
    Draw Unicode boxes around content.

    Supports various border styles and can contain multi-line content.
    """

    def __init__(self, theme: Theme | None = None, width: int | None = None):
        """
        Initialize box drawer.

        Args:
            theme: Theme to use for box characters and colors
            width: Fixed width for box (None = auto-fit to content)
        """
        self.theme = theme or get_theme()
        self.width = width

    def render(
        self,
        content: str | list[str],
        title: str | None = None,
        footer: str | None = None,
        padding: int = 1,
    ) -> str:
        """
        Render content inside a box.

        Args:
            content: String or list of strings to box
            title: Optional title for top border
            footer: Optional footer for bottom border
            padding: Internal padding

        Returns:
            Boxed content as string
        """
        box = self.theme.box

        # Convert content to lines
        if isinstance(content, str):
            lines = content.split("\n")
        else:
            lines = list(content)

        # Calculate width
        if self.width:
            inner_width = self.width - 2  # Account for borders
        else:
            max_content_width = max(visible_len(line) for line in lines) if lines else 0
            title_width = visible_len(title) + 4 if title else 0
            footer_width = visible_len(footer) + 4 if footer else 0
            inner_width = max(max_content_width, title_width, footer_width) + (padding * 2)

        result = []

        # Top border with optional title
        if title:
            title_space = inner_width - 4
            if visible_len(title) > title_space:
                title = title[: title_space - 3] + "..."
            title_padded = f" {title} "
            remaining = inner_width - visible_len(title_padded)
            left_border = remaining // 2
            right_border = remaining - left_border
            top = (
                box.top_left
                + box.horizontal * left_border
                + title_padded
                + box.horizontal * right_border
                + box.top_right
            )
        else:
            top = box.top_left + box.horizontal * inner_width + box.top_right

        result.append(self.theme.border_text(top))

        # Content lines
        for line in lines:
            padded = pad_to_width(line, inner_width - (padding * 2))
            content_line = (
                self.theme.border_text(box.vertical)
                + " " * padding
                + padded
                + " " * padding
                + self.theme.border_text(box.vertical)
            )
            result.append(content_line)

        # Bottom border with optional footer
        if footer:
            footer_space = inner_width - 4
            if visible_len(footer) > footer_space:
                footer = footer[: footer_space - 3] + "..."
            footer_padded = f" {footer} "
            remaining = inner_width - visible_len(footer_padded)
            left_border = remaining // 2
            right_border = remaining - left_border
            bottom = (
                box.bottom_left
                + box.horizontal * left_border
                + footer_padded
                + box.horizontal * right_border
                + box.bottom_right
            )
        else:
            bottom = box.bottom_left + box.horizontal * inner_width + box.bottom_right

        result.append(self.theme.border_text(bottom))

        return "\n".join(result)

    def render_header(self, title: str, width: int) -> str:
        """
        Render a header line with title.

        Args:
            title: Header title
            width: Total width

        Returns:
            Header line string
        """
        box = self.theme.box
        inner_width = width - 2

        title_padded = f" {title} "
        remaining = inner_width - visible_len(title_padded)
        left = remaining // 2
        right = remaining - left

        line = (
            box.top_left
            + box.horizontal * left
            + title_padded
            + box.horizontal * right
            + box.top_right
        )
        return self.theme.border_text(line)

    def render_separator(self, width: int, style: str = "heavy") -> str:
        """
        Render a horizontal separator line.

        Args:
            width: Total width
            style: 'heavy' for double lines, 'light' for single lines

        Returns:
            Separator line string
        """
        box = self.theme.box
        inner_width = width - 2

        if style == "heavy":
            line = box.t_right + box.horizontal * inner_width + box.t_left
        else:
            line = box.t_right + box.light_h * inner_width + box.t_left

        return self.theme.border_text(line)

    def render_bottom(self, width: int) -> str:
        """
        Render bottom border.

        Args:
            width: Total width

        Returns:
            Bottom border string
        """
        box = self.theme.box
        inner_width = width - 2
        line = box.bottom_left + box.horizontal * inner_width + box.bottom_right
        return self.theme.border_text(line)
