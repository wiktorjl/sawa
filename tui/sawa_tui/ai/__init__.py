"""AI integration for glossary generation."""

from sawa_tui.ai.client import ZAIClient
from sawa_tui.ai.prompts import build_glossary_prompt

__all__ = ["ZAIClient", "build_glossary_prompt"]
