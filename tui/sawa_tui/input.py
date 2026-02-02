"""Keyboard input handling using blessed."""

from blessed import Terminal

# Key constants for easier reference
KEY_UP = "KEY_UP"
KEY_DOWN = "KEY_DOWN"
KEY_LEFT = "KEY_LEFT"
KEY_RIGHT = "KEY_RIGHT"
KEY_ENTER = "KEY_ENTER"
KEY_ESCAPE = "KEY_ESCAPE"
KEY_BACKSPACE = "KEY_BACKSPACE"
KEY_TAB = "KEY_TAB"
KEY_F1 = "KEY_F1"
KEY_F2 = "KEY_F2"
KEY_F3 = "KEY_F3"
KEY_F4 = "KEY_F4"
KEY_F5 = "KEY_F5"
KEY_F6 = "KEY_F6"


class InputHandler:
    """Handle keyboard input."""

    def __init__(self, terminal: Terminal) -> None:
        self.term = terminal

    def get_key(self, timeout: float = 0.1) -> str | None:
        """
        Get a keypress with timeout.

        Returns key name (e.g., 'KEY_UP', 'KEY_F1', 'q') or None if timeout.
        """
        with self.term.cbreak():
            key = self.term.inkey(timeout=timeout)

            if not key:
                return None

            # Check for special keys
            if key.is_sequence:
                return key.name

            # Regular character
            return key

    def get_key_blocking(self) -> str:
        """Get a keypress, blocking until one is received."""
        with self.term.cbreak():
            key = self.term.inkey()

            if key.is_sequence:
                return key.name

            return key


def normalize_key(key: str | None) -> str | None:
    """Normalize key names for consistent handling."""
    if key is None:
        return None

    # Map common key variations
    key_map = {
        "\n": KEY_ENTER,
        "\r": KEY_ENTER,
        "KEY_ENTER": KEY_ENTER,
        "\x1b": KEY_ESCAPE,
        "KEY_ESCAPE": KEY_ESCAPE,
        "\x7f": KEY_BACKSPACE,
        "\x08": KEY_BACKSPACE,
        "KEY_BACKSPACE": KEY_BACKSPACE,
        "KEY_DELETE": KEY_BACKSPACE,
        "\t": KEY_TAB,
        "KEY_TAB": KEY_TAB,
    }

    return key_map.get(key, key)
