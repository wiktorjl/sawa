"""View rendering modules for the TUI.

This package contains separate modules for each major view:
- base: Header, footer, and main render_app function
- stocks: Stock list, detail, and news views
- fundamentals: Income Statement, Balance Sheet, Cash Flow
- economy: Treasury Yields, Inflation, Labor Market
- settings: User settings and configuration
- glossary: Financial term definitions
- users: User management and switching
"""

from sawa_tui.views.base import render_app, render_footer, render_header
from sawa_tui.views.economy import render_economy_view
from sawa_tui.views.fundamentals import render_fundamentals_view
from sawa_tui.views.glossary import render_glossary_view
from sawa_tui.views.settings import SETTINGS_ITEMS, render_settings_view
from sawa_tui.views.stocks import (
    render_news_fullscreen_view,
    render_stock_detail_view,
    render_stocks_view,
)
from sawa_tui.views.users import render_user_management_view, render_user_switcher_view

__all__ = [
    # Base
    "render_app",
    "render_header",
    "render_footer",
    # Stocks
    "render_stocks_view",
    "render_stock_detail_view",
    "render_news_fullscreen_view",
    # Fundamentals
    "render_fundamentals_view",
    # Economy
    "render_economy_view",
    # Settings
    "render_settings_view",
    "SETTINGS_ITEMS",
    # Glossary
    "render_glossary_view",
    # Users
    "render_user_management_view",
    "render_user_switcher_view",
]
