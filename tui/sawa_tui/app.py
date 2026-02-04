"""Main TUI application using Rich + blessed."""

import argparse
import logging
import os
import sys

from blessed import Terminal
from dotenv import load_dotenv
from rich.console import Console

from sawa_tui.ai.client import ZAIClient, ZAIError
from sawa_tui.ai.prompts import OVERVIEW_REGEN_OPTIONS, REGEN_OPTIONS
from sawa_tui.database import init_schema
from sawa_tui.input import (
    KEY_BACKSPACE,
    KEY_DOWN,
    KEY_ENTER,
    KEY_ESCAPE,
    KEY_F1,
    KEY_F2,
    KEY_F3,
    KEY_F4,
    KEY_F5,
    KEY_F6,
    KEY_LEFT,
    KEY_RIGHT,
    KEY_TAB,
    KEY_UP,
    InputHandler,
    normalize_key,
)
from sawa_tui.models.glossary import GlossaryManager
from sawa_tui.models.overview import OverviewManager
from sawa_tui.models.watchlist import WatchlistManager
from sawa_tui.state import AppState, EconomyTab, FundamentalsTab, SettingsCategory, View
from sawa_tui.views import SETTINGS_ITEMS, render_app

logger = logging.getLogger(__name__)


class SP500App:
    """S&P 500 Terminal Application."""

    def __init__(self) -> None:
        self.term = Terminal()
        self.console = Console(force_terminal=True)
        self.input = InputHandler(self.term)
        self.state = AppState()
        self.ai_client = ZAIClient()

    def run(self) -> None:
        """Run the application main loop."""
        # Initialize database
        try:
            init_schema()
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            print(f"Database error: {e}")
            return

        # Load initial data
        self.state.load_watchlists()

        # Enter fullscreen
        print(self.term.enter_fullscreen, end="")
        print(self.term.hide_cursor, end="")

        try:
            while self.state.running:
                # Update terminal size
                self.state.term_height = self.term.height
                self.state.term_width = self.term.width

                # Render only when needed
                if self.state.needs_redraw:
                    render_app(self.console, self.state)
                    self.state.needs_redraw = False

                # Get input
                key = self.input.get_key(timeout=0.1)
                key = normalize_key(key)

                if key:
                    self._handle_key(key)
                    self.state.needs_redraw = True

        except KeyboardInterrupt:
            pass
        finally:
            # Exit fullscreen
            print(self.term.normal_cursor, end="")
            print(self.term.exit_fullscreen, end="")

    def _handle_key(self, key: str) -> None:
        """Handle a keypress."""
        # Clear any status message on keypress
        if not self.state.input_mode:
            self.state.clear_message()

        # Input mode handling
        if self.state.input_mode:
            self._handle_input_mode(key)
            return

        # Help overlay - can be closed with Esc
        if self.state.show_help_overlay and key == KEY_ESCAPE:
            self.state.show_help_overlay = False
            return

        # Settings editing mode - handle before global keys
        if self.state.settings_editing or self.state.settings_popup_open:
            self._handle_settings_key(key)
            return

        # Global keys (work in any view)
        if key == "q":
            self.state.running = False
            return
        if key == "?":
            self.state.show_help_overlay = not self.state.show_help_overlay
            return
        if key == KEY_F1:
            self.state.navigate_to(View.STOCKS)
            self.state.load_watchlists()
            return
        if key == KEY_F2:
            self.state.navigate_to(View.FUNDAMENTALS)
            # Auto-load fundamentals if none selected
            if not self.state.fund_ticker:
                # Try current stock from watchlist first
                stock = self.state.current_stock()
                if stock and stock.ticker:
                    self.state.load_fundamentals(stock.ticker)
                elif self.state.watchlist_stocks:
                    self.state.load_fundamentals(self.state.watchlist_stocks[0].ticker)
                else:
                    # Fallback to AAPL
                    self.state.load_fundamentals("AAPL")
            return
        if key == KEY_F3:
            self.state.navigate_to(View.ECONOMY)
            self.state.load_economy()
            return
        if key == KEY_F4:
            self.state.navigate_to(View.SETTINGS)
            return
        if key == KEY_F5:
            self.state.navigate_to(View.GLOSSARY)
            if not self.state.glossary_terms:
                self.state.load_glossary_terms()
            return
        if key == KEY_F6:
            self.state.navigate_to(View.SCREENER)
            self.state.load_screener()
            return
        if key == "\x15":  # Ctrl+U - User switcher
            self.state.navigate_to(View.USER_SWITCHER)
            self.state.load_users_for_switcher()
            return
        if key == "\x10" and self.state.current_user and self.state.current_user.is_admin:
            # Ctrl+P - User management (admin only)
            self.state.navigate_to(View.USER_MANAGEMENT)
            self.state.load_users_for_management()
            return
        if key == "r" and self.state.current_view != View.SETTINGS:
            self._refresh()
            return

        # Universal search (where applicable)
        if key == "/" and self.state.current_view in (
            View.STOCKS,
            View.FUNDAMENTALS,
            View.GLOSSARY,
            View.SCREENER,
        ):
            if self.state.current_view == View.STOCKS:
                self.state.start_input("Filter stocks", "filter_stocks", self.state.stock_filter)
            elif self.state.current_view == View.FUNDAMENTALS:
                self.state.start_input("Ticker", "search_ticker")
            elif self.state.current_view == View.GLOSSARY:
                self.state.start_input(
                    "Search terms", "glossary_search", self.state.glossary_search
                )
            elif self.state.current_view == View.SCREENER:
                self.state.start_input(
                    "Query (e.g. pe < 15)", "run_screener", self.state.screener_query
                )
            return

        # View-specific keys
        if self.state.current_view == View.STOCKS:
            self._handle_stocks_key(key)
        elif self.state.current_view == View.STOCK_DETAIL:
            self._handle_detail_key(key)
        elif self.state.current_view == View.NEWS_FULLSCREEN:
            self._handle_news_key(key)
        elif self.state.current_view == View.FUNDAMENTALS:
            self._handle_fundamentals_key(key)
        elif self.state.current_view == View.ECONOMY:
            self._handle_economy_key(key)
        elif self.state.current_view == View.SETTINGS:
            self._handle_settings_key(key)
        elif self.state.current_view == View.GLOSSARY:
            self._handle_glossary_key(key)
        elif self.state.current_view == View.USER_MANAGEMENT:
            self._handle_user_management_key(key)
        elif self.state.current_view == View.USER_SWITCHER:
            self._handle_user_switcher_key(key)
        elif self.state.current_view == View.SCREENER:
            self._handle_screener_key(key)

    def _handle_input_mode(self, key: str) -> None:
        """Handle keys in input mode."""
        if key == KEY_ESCAPE:
            self.state.cancel_input()
        elif key == KEY_ENTER:
            self._submit_input()
        elif key == KEY_BACKSPACE:
            self.state.input_value = self.state.input_value[:-1]
        elif len(key) == 1 and key.isprintable():
            self.state.input_value += key

    def _submit_input(self) -> None:
        """Submit the current input."""
        value = self.state.input_value.strip()
        callback = self.state.input_callback
        self.state.cancel_input()
        self.state.needs_redraw = True

        if not value:
            return

        if callback == "new_watchlist":
            self.state.ensure_user()
            if self.state.current_user:
                wl = WatchlistManager.create(self.state.current_user.id, value)
                if wl:
                    self.state.set_message(f"Created watchlist: {value}")
                    self.state.load_watchlists()
                else:
                    self.state.set_message("Failed to create watchlist", error=True)
            else:
                self.state.set_message("No active user", error=True)

        elif callback == "search_ticker":
            ticker = value.upper()
            self.state.load_fundamentals(ticker)
            if not self.state.fund_company:
                self.state.set_message(f"Ticker not found: {ticker}", error=True)

        elif callback == "add_stock":
            ticker = value.upper()
            wl = self.state.current_watchlist()
            if not wl:
                self.state.set_message("No watchlist selected", error=True)
            else:
                success, error = WatchlistManager.add_symbol(wl.id, ticker)
                if success:
                    self.state.set_message(f"Added {ticker} to {wl.name}")
                    self.state.load_watchlist_stocks()
                else:
                    self.state.set_message(error, error=True)

        elif callback == "glossary_search":
            self.state.filter_glossary_terms(value)

        elif callback == "glossary_add_term":
            if GlossaryManager.add_term(value):
                self.state.set_message(f"Added term: {value}")
                self.state.load_glossary_terms()
            else:
                self.state.set_message("Term already exists or failed to add", error=True)

        elif callback == "glossary_custom_regen":
            # Custom regeneration with user-provided instructions
            term = self.state.current_glossary_term()
            if term:
                self._generate_glossary_definition(term.term, custom_instructions=value)

        elif callback == "overview_custom_regen":
            # Custom overview regeneration with user-provided instructions
            if self.state.detail_ticker:
                self._generate_company_overview(custom_instructions=value)

        elif callback == "filter_stocks":
            # Filter stocks in current watchlist
            self.state.filter_stocks(value)

        elif callback == "run_screener":
            self.state.run_screener(value)

        elif callback == "create_user":
            from sawa_tui.models.users import UserManager

            user = UserManager.create(value, is_admin=False)
            if user:
                self.state.set_message(f"Created user: {value}")
                self.state.load_users_for_management()
            else:
                self.state.set_message("Failed to create user (name may already exist)", error=True)

        elif callback == "rename_user":
            from sawa_tui.models.users import UserManager

            if self.state.user_mgmt_users:
                user = self.state.user_mgmt_users[self.state.user_mgmt_selected_idx]
                success, error = UserManager.rename(user.id, value)
                if success:
                    self.state.set_message(f"Renamed to: {value}")
                    self.state.load_users_for_management()
                    # Update current_user if we renamed ourselves
                    if self.state.current_user and self.state.current_user.id == user.id:
                        self.state.current_user = UserManager.get_by_id(user.id)
                else:
                    self.state.set_message(error, error=True)

    def _refresh(self) -> None:
        """Refresh current view data."""
        if self.state.current_view == View.STOCKS:
            self.state.load_watchlists()
        elif self.state.current_view == View.STOCK_DETAIL:
            self.state.load_stock_detail(self.state.detail_ticker)
        elif self.state.current_view == View.FUNDAMENTALS:
            if self.state.fund_ticker:
                self.state.load_fundamentals(self.state.fund_ticker)
        elif self.state.current_view == View.ECONOMY:
            self.state.load_economy()
        elif self.state.current_view == View.GLOSSARY:
            self.state.load_glossary_terms()

        self.state.set_message("Refreshed")

    # =========================================================================
    # STOCKS VIEW
    # =========================================================================

    def _handle_stocks_key(self, key: str) -> None:
        """Handle keys in stocks view."""
        if key == KEY_TAB:
            self.state.focus_sidebar = not self.state.focus_sidebar

        elif key == KEY_UP:
            if self.state.focus_sidebar:
                if self.state.selected_watchlist_idx > 0:
                    self.state.selected_watchlist_idx -= 1
                    self.state.adjust_watchlist_scroll()
                    self.state.load_watchlist_stocks()
            else:
                if self.state.selected_stock_idx > 0:
                    self.state.selected_stock_idx -= 1
                    self.state.adjust_stock_scroll()

        elif key == KEY_DOWN:
            if self.state.focus_sidebar:
                if self.state.selected_watchlist_idx < len(self.state.watchlists) - 1:
                    self.state.selected_watchlist_idx += 1
                    self.state.adjust_watchlist_scroll()
                    self.state.load_watchlist_stocks()
            else:
                if self.state.selected_stock_idx < len(self.state.watchlist_stocks) - 1:
                    self.state.selected_stock_idx += 1
                    self.state.adjust_stock_scroll()

        elif key == KEY_ENTER:
            if self.state.focus_sidebar:
                # Select watchlist and move to stock list
                self.state.focus_sidebar = False
            else:
                # View stock detail
                stock = self.state.current_stock()
                if stock and stock.ticker:
                    self.state.load_stock_detail(stock.ticker)
                    self.state.navigate_to(View.STOCK_DETAIL)

        elif key == KEY_LEFT:
            self.state.focus_sidebar = True

        elif key == KEY_RIGHT:
            self.state.focus_sidebar = False

        elif key == "n":
            # New watchlist
            self.state.start_input("New watchlist name", "new_watchlist")

        elif key == "d":
            # Delete watchlist
            wl = self.state.current_watchlist()
            if wl:
                if wl.is_default:
                    self.state.set_message("Cannot delete default watchlist", error=True)
                else:
                    self.state.ensure_user()
                    if self.state.current_user and WatchlistManager.delete(
                        self.state.current_user.id, wl.id
                    ):
                        self.state.set_message(f"Deleted: {wl.name}")
                        self.state.load_watchlists()
                    else:
                        self.state.set_message("Failed to delete", error=True)

        elif key == "a":
            # Add stock to watchlist
            self.state.start_input("Add ticker", "add_stock")

        elif key == "x":
            # Remove stock from watchlist
            wl = self.state.current_watchlist()
            stock = self.state.current_stock()
            if wl and stock and stock.ticker:
                if WatchlistManager.remove_symbol(wl.id, stock.ticker):
                    self.state.set_message(f"Removed {stock.ticker}")
                    self.state.load_watchlist_stocks()
                else:
                    self.state.set_message("Failed to remove", error=True)

    # =========================================================================
    # STOCK DETAIL VIEW
    # =========================================================================

    def _handle_detail_key(self, key: str) -> None:
        """Handle keys in stock detail view."""
        # Handle overview regen menu if active
        if self.state.detail_overview_show_regen_menu:
            self._handle_overview_regen_key(key)
            return

        if key == KEY_ESCAPE or key == "KEY_BACKSPACE":
            # Go back (also clear overview state)
            self.state.clear_overview_state()
            self.state.navigate_to(View.STOCKS)

        elif key == "a":
            # Add to watchlist
            wl = self.state.current_watchlist()
            if wl and self.state.detail_ticker:
                success, error = WatchlistManager.add_symbol(wl.id, self.state.detail_ticker)
                if success:
                    self.state.set_message(f"Added to {wl.name}")
                else:
                    self.state.set_message(error, error=True)

        elif key == "v":
            # Toggle news pane (hide overview if showing)
            if self.state.detail_overview_visible:
                self.state.detail_overview_visible = False
            self.state.toggle_news_pane()

        elif key == "V":
            # Enter fullscreen news view
            if self.state.detail_news:
                self.state.selected_news_idx = 0
                self.state.news_scroll_offset = 0
                self.state.navigate_to(View.NEWS_FULLSCREEN)

        elif key == "o":
            # Toggle/generate AI overview
            if self.state.detail_overview_visible and self.state.detail_overview:
                # Already visible with data - toggle off
                self.state.detail_overview_visible = False
            elif self.state.detail_overview_visible and not self.state.detail_overview:
                # Visible but no data - generate
                self._generate_company_overview()
            else:
                # Not visible - show and try cache first
                self.state.detail_overview_visible = True
                self.state.detail_show_news = False  # Hide news when showing overview
                if not self.state.load_company_overview(self.state.detail_ticker):
                    # No cache - generate
                    self._generate_company_overview()

        elif key == "O":
            # Show regeneration menu (if overview panel is visible)
            if self.state.detail_overview_visible:
                self.state.detail_overview_show_regen_menu = True

        elif key == KEY_UP:
            # Scroll overview up (if visible)
            if self.state.detail_overview_visible and self.state.detail_overview:
                if self.state.detail_overview_scroll > 0:
                    self.state.detail_overview_scroll -= 1

        elif key == KEY_DOWN:
            # Scroll overview down (if visible)
            if self.state.detail_overview_visible and self.state.detail_overview:
                self.state.detail_overview_scroll += 1

    def _handle_overview_regen_key(self, key: str) -> None:
        """Handle keys in overview regeneration menu."""
        if key == KEY_ESCAPE:
            self.state.detail_overview_show_regen_menu = False
            return

        # Get option from OVERVIEW_REGEN_OPTIONS
        if key in OVERVIEW_REGEN_OPTIONS:
            _, instructions = OVERVIEW_REGEN_OPTIONS[key]
            self.state.detail_overview_show_regen_menu = False
            self._generate_company_overview(custom_instructions=instructions)

        elif key == "c":
            # Custom instructions - start input mode
            self.state.detail_overview_show_regen_menu = False
            self.state.start_input("Custom instructions", "overview_custom_regen")

    def _generate_company_overview(self, custom_instructions: str = "") -> None:
        """Generate a company overview using AI."""
        if not self.ai_client.is_configured():
            self.state.detail_overview_error = (
                "ZAI_API_KEY not configured. Set it in Settings or environment."
            )
            return

        self.state.detail_overview_loading = True
        self.state.detail_overview_error = ""
        self.state.detail_overview_stream_content = ""
        self.state.detail_overview = None
        self.state.needs_redraw = True

        # Render loading state
        render_app(self.console, self.state)

        try:
            company = self.state.detail_company

            # Use streaming to show progress
            def stream_callback(chunk: str) -> None:
                self.state.detail_overview_stream_content += chunk
                self.state.needs_redraw = True
                render_app(self.console, self.state)

            overview = self.ai_client.generate_company_overview(
                ticker=self.state.detail_ticker,
                company_name=company.name if company else self.state.detail_ticker,
                sector=company.sector if company else None,
                custom_instructions=custom_instructions,
                stream_callback=stream_callback,
            )

            # Save to cache (as user override if user is logged in, shared otherwise)
            self.state.ensure_user()
            user_id = self.state.current_user.id if self.state.current_user else None
            OverviewManager.save(overview, user_id=user_id)

            # Update state
            self.state.detail_overview = overview
            self.state.detail_overview_loading = False
            self.state.detail_overview_stream_content = ""

        except ZAIError as e:
            self.state.detail_overview_loading = False
            self.state.detail_overview_error = str(e.message)
            self.state.detail_overview_stream_content = ""
            logger.error(f"Failed to generate company overview: {e}")

        except Exception as e:
            self.state.detail_overview_loading = False
            self.state.detail_overview_error = f"Unexpected error: {e}"
            self.state.detail_overview_stream_content = ""
            logger.error(f"Unexpected error generating overview: {e}")

    # =========================================================================
    # NEWS FULLSCREEN VIEW
    # =========================================================================

    def _handle_news_key(self, key: str) -> None:
        """Handle keys in fullscreen news view."""
        if key == KEY_ESCAPE:
            # Go back to stock detail
            self.state.navigate_to(View.STOCK_DETAIL)

        elif key == KEY_UP:
            if self.state.selected_news_idx > 0:
                self.state.selected_news_idx -= 1
                self.state.adjust_news_scroll()

        elif key == KEY_DOWN:
            if self.state.selected_news_idx < len(self.state.detail_news) - 1:
                self.state.selected_news_idx += 1
                self.state.adjust_news_scroll()

        elif key == KEY_ENTER:
            # Open article URL in browser
            article = self.state.current_news_article()
            if article and article.article_url:
                self._open_url(article.article_url)

    def _open_url(self, url: str) -> None:
        """Open URL in default browser without polluting console."""
        import os
        import subprocess
        import sys

        try:
            # Use subprocess directly with all output suppressed
            # This avoids webbrowser module which can leak output
            devnull = subprocess.DEVNULL

            if sys.platform == "darwin":
                subprocess.Popen(
                    ["open", url],
                    stdout=devnull,
                    stderr=devnull,
                    stdin=devnull,
                )
            elif sys.platform == "win32":
                subprocess.Popen(
                    ["start", "", url],
                    shell=True,
                    stdout=devnull,
                    stderr=devnull,
                    stdin=devnull,
                )
            else:
                # Linux - use setsid to fully detach from terminal
                subprocess.Popen(
                    ["setsid", "xdg-open", url],
                    stdout=devnull,
                    stderr=devnull,
                    stdin=devnull,
                    start_new_session=True,
                    env={**os.environ, "DISPLAY": os.environ.get("DISPLAY", ":0")},
                )
            self.state.set_message("Opened in browser")
        except Exception as e:
            self.state.set_message(f"Failed to open URL: {e}", error=True)

    # =========================================================================
    # FUNDAMENTALS VIEW
    # =========================================================================

    def _handle_fundamentals_key(self, key: str) -> None:
        """Handle keys in fundamentals view."""
        if key == "1":
            self.state.fund_tab = FundamentalsTab.INCOME
        elif key == "2":
            self.state.fund_tab = FundamentalsTab.BALANCE
        elif key == "3":
            self.state.fund_tab = FundamentalsTab.CASHFLOW
        elif key == "t":
            self.state.fund_quarterly = not self.state.fund_quarterly
            if self.state.fund_ticker:
                self.state.load_fundamentals(self.state.fund_ticker)
        elif key == "/":
            self.state.start_input("Ticker", "search_ticker")

    # =========================================================================
    # ECONOMY VIEW
    # =========================================================================

    def _handle_economy_key(self, key: str) -> None:
        """Handle keys in economy view."""
        if key == "1":
            self.state.econ_tab = EconomyTab.YIELDS
        elif key == "2":
            self.state.econ_tab = EconomyTab.INFLATION
        elif key == "3":
            self.state.econ_tab = EconomyTab.LABOR

    # =========================================================================
    # SETTINGS VIEW
    # =========================================================================

    def _handle_settings_key(self, key: str) -> None:
        """Handle keys in settings view."""
        from sawa_tui.models.settings import SettingsManager

        items = SETTINGS_ITEMS.get(self.state.settings_category, [])

        # Ensure user is loaded
        self.state.ensure_user()
        if not self.state.current_user:
            self.state.set_message("No active user", error=True)
            return

        # Handle popup menu
        if self.state.settings_popup_open:
            self._handle_settings_popup(key, items)
            return

        # Handle editing mode
        if self.state.settings_editing:
            self._handle_settings_edit(key, items)
            return

        # Category switching
        if key == "1":
            self.state.settings_category = SettingsCategory.DISPLAY
            self.state.settings_selected_idx = 0
            return
        if key == "2":
            self.state.settings_category = SettingsCategory.CHARTS
            self.state.settings_selected_idx = 0
            return
        if key == "3":
            self.state.settings_category = SettingsCategory.BEHAVIOR
            self.state.settings_selected_idx = 0
            return
        if key == "4":
            self.state.settings_category = SettingsCategory.API
            self.state.settings_selected_idx = 0
            return
        if key == "5":
            self.state.settings_category = SettingsCategory.USERS
            self.state.settings_selected_idx = 0
            return

        # Navigation
        if key == KEY_UP:
            if self.state.settings_selected_idx > 0:
                self.state.settings_selected_idx -= 1
            return

        if key == KEY_DOWN:
            if self.state.settings_selected_idx < len(items) - 1:
                self.state.settings_selected_idx += 1
            return

        # Enter/Space: toggle bool, cycle choice, or edit text
        if key == KEY_ENTER or key == " ":
            if not items:
                return
            item_key, label, value_type, choices = items[self.state.settings_selected_idx]
            current_value = SettingsManager.get(self.state.current_user.id, item_key)

            if value_type == "bool":
                # Toggle boolean
                new_value = "false" if current_value == "true" else "true"
                success, error = SettingsManager.set(
                    self.state.current_user.id, item_key, new_value
                )
                if success:
                    self.state.set_message(f"{label}: {'On' if new_value == 'true' else 'Off'}")
                else:
                    self.state.set_message(error, error=True)
            elif (value_type == "choice" or value_type == "int") and choices:
                # Open popup menu for choices
                self.state.settings_popup_open = True
                self.state.settings_popup_choices = [str(c) for c in choices]
                self.state.settings_popup_label = label
                # Set current selection
                try:
                    if value_type == "int" and current_value:
                        self.state.settings_popup_idx = choices.index(int(current_value))
                    elif current_value:
                        self.state.settings_popup_idx = choices.index(current_value)
                    else:
                        self.state.settings_popup_idx = 0
                except (ValueError, TypeError):
                    self.state.settings_popup_idx = 0
            elif value_type == "secret":
                # For secrets, start with empty value (user re-enters)
                self.state.settings_editing = True
                self.state.settings_edit_value = ""
            else:
                # Enter edit mode for free-form text/int
                self.state.settings_editing = True
                self.state.settings_edit_value = str(current_value) if current_value else ""
            return

        # Left/Right to cycle through choices
        if key == KEY_LEFT or key == KEY_RIGHT:
            if not items:
                return
            item_key, label, value_type, choices = items[self.state.settings_selected_idx]
            if not choices:
                return

            current_value = SettingsManager.get(self.state.current_user.id, item_key)

            if value_type == "choice":
                try:
                    idx = choices.index(current_value)
                except ValueError:
                    idx = 0
                if key == KEY_RIGHT:
                    idx = (idx + 1) % len(choices)
                else:
                    idx = (idx - 1) % len(choices)
                new_value = choices[idx]
                success, error = SettingsManager.set(
                    self.state.current_user.id, item_key, new_value
                )
                if success:
                    self.state.set_message(f"{label}: {new_value}")
                else:
                    self.state.set_message(error, error=True)
            elif value_type == "int":
                try:
                    idx = choices.index(int(current_value)) if current_value else 0
                except (ValueError, TypeError):
                    idx = 0
                if key == KEY_RIGHT:
                    idx = (idx + 1) % len(choices)
                else:
                    idx = (idx - 1) % len(choices)
                new_value = str(choices[idx])
                success, error = SettingsManager.set(
                    self.state.current_user.id, item_key, new_value
                )
                if success:
                    self.state.set_message(f"{label}: {new_value}")
                else:
                    self.state.set_message(error, error=True)
            return

    def _handle_settings_edit(self, key: str, items: list) -> None:
        """Handle keys while editing a setting value."""
        from sawa_tui.models.settings import SettingsManager

        if key == KEY_ESCAPE:
            self.state.settings_editing = False
            self.state.settings_edit_value = ""
            return

        if key == KEY_ENTER:
            # Save the value
            item_key, label, value_type, choices = items[self.state.settings_selected_idx]

            # Validate and save
            new_value_str = self.state.settings_edit_value
            success, error = SettingsManager.set(
                self.state.current_user.id, item_key, new_value_str
            )

            if success:
                # For API keys, also set environment variable for current session
                if value_type == "secret" and item_key == "zai_api_key":
                    os.environ["ZAI_API_KEY"] = new_value_str
                    self.state.set_message(f"Saved: {label}")
                else:
                    self.state.set_message(f"Saved: {label} = {new_value_str}")
            else:
                self.state.set_message(error, error=True)

            self.state.settings_editing = False
            self.state.settings_edit_value = ""
            return

        if key == KEY_BACKSPACE:
            self.state.settings_edit_value = self.state.settings_edit_value[:-1]
            return

        # Add character(s) - handle paste (multiple chars) and single char input
        if key and all(c.isprintable() for c in key):
            self.state.settings_edit_value += key

    def _handle_settings_popup(self, key: str, items: list) -> None:
        """Handle keys in settings popup menu."""
        from sawa_tui.models.settings import SettingsManager

        if key == KEY_ESCAPE:
            # Close popup without saving
            self.state.settings_popup_open = False
            return

        if key == KEY_UP:
            if self.state.settings_popup_idx > 0:
                self.state.settings_popup_idx -= 1
            return

        if key == KEY_DOWN:
            if self.state.settings_popup_idx < len(self.state.settings_popup_choices) - 1:
                self.state.settings_popup_idx += 1
            return

        if key == KEY_ENTER or key == " ":
            # Select the highlighted choice
            item_key, label, value_type, choices = items[self.state.settings_selected_idx]

            selected_value_str = self.state.settings_popup_choices[self.state.settings_popup_idx]

            success, error = SettingsManager.set(
                self.state.current_user.id, item_key, selected_value_str
            )
            if success:
                self.state.set_message(f"{label}: {selected_value_str}")
            else:
                self.state.set_message(error, error=True)

            # Close popup
            self.state.settings_popup_open = False
            return

    # =========================================================================
    # GLOSSARY VIEW
    # =========================================================================

    def _handle_glossary_key(self, key: str) -> None:
        """Handle keys in glossary view."""
        # Handle regeneration menu
        if self.state.glossary_show_regen_menu:
            self._handle_regen_menu_key(key)
            return

        # Don't allow actions during loading
        if self.state.glossary_loading:
            return

        if key == KEY_ESCAPE:
            # Clear search or error
            if self.state.glossary_search:
                self.state.filter_glossary_terms("")
            elif self.state.glossary_error:
                self.state.glossary_error = ""
            return

        if key == "/":
            # Start search
            self.state.start_input("Search terms", "glossary_search", self.state.glossary_search)
            return

        if key == KEY_TAB:
            # Toggle focus between sidebar and definition
            self.state.glossary_focus_sidebar = not self.state.glossary_focus_sidebar
            return

        if key == KEY_LEFT:
            self.state.glossary_focus_sidebar = True
            return

        if key == KEY_RIGHT:
            self.state.glossary_focus_sidebar = False
            return

        if key == KEY_UP:
            if self.state.glossary_focus_sidebar:
                if self.state.selected_term_idx > 0:
                    self.state.selected_term_idx -= 1
                    self.state.adjust_glossary_scroll()
                    # Load cached definition if available
                    term = self.state.current_glossary_term()
                    if term:
                        self.state.load_glossary_definition(term.term)
            return

        if key == KEY_DOWN:
            if self.state.glossary_focus_sidebar:
                if self.state.selected_term_idx < len(self.state.glossary_filtered) - 1:
                    self.state.selected_term_idx += 1
                    self.state.adjust_glossary_scroll()
                    # Load cached definition if available
                    term = self.state.current_glossary_term()
                    if term:
                        self.state.load_glossary_definition(term.term)
            return

        if key == KEY_ENTER:
            # Generate definition for selected term
            term = self.state.current_glossary_term()
            if term:
                # Check if already cached
                if self.state.load_glossary_definition(term.term):
                    self.state.glossary_focus_sidebar = False
                else:
                    # Generate new definition
                    self._generate_glossary_definition(term.term)
            elif self.state.glossary_error:
                # Retry on error
                term = self.state.current_glossary_term()
                if term:
                    self._generate_glossary_definition(term.term)
            return

        if key == "n":
            # Add new term
            self.state.start_input("New term", "glossary_add_term")
            return

        if key == "d":
            # Delete user-added term
            term = self.state.current_glossary_term()
            if term and term.source == "user":
                if GlossaryManager.delete_term(term.term):
                    self.state.set_message(f"Deleted: {term.term}")
                    self.state.load_glossary_terms()
                else:
                    self.state.set_message("Cannot delete curated terms", error=True)
            else:
                self.state.set_message("Can only delete user-added terms", error=True)
            return

        if key == "g":
            # Show regeneration menu
            term = self.state.current_glossary_term()
            if term:
                self.state.glossary_show_regen_menu = True
            return

        # Quick jump to related terms (1-5)
        if key in "12345":
            definition = self.state.glossary_definition
            if definition and definition.related_terms:
                idx = int(key) - 1
                if idx < len(definition.related_terms):
                    related_term = definition.related_terms[idx]
                    # Ensure term is in list
                    GlossaryManager.ensure_term_in_list(related_term)
                    self.state.load_glossary_terms()
                    # Find and select the term
                    for i, t in enumerate(self.state.glossary_filtered):
                        if t.term == related_term:
                            self.state.selected_term_idx = i
                            self.state.adjust_glossary_scroll()
                            break
                    # Load or generate definition
                    if not self.state.load_glossary_definition(related_term):
                        self._generate_glossary_definition(related_term)
            return

    def _handle_regen_menu_key(self, key: str) -> None:
        """Handle keys in the regeneration menu."""
        if key == KEY_ESCAPE:
            self.state.glossary_show_regen_menu = False
            return

        term = self.state.current_glossary_term()
        if not term:
            self.state.glossary_show_regen_menu = False
            return

        if key in "1234":
            # Predefined regeneration option
            option = REGEN_OPTIONS.get(key)
            if option:
                _, instructions = option
                self.state.glossary_show_regen_menu = False
                # Delete cached definition first
                self.state.ensure_user()
                user_id = self.state.current_user.id if self.state.current_user else None
                GlossaryManager.delete_cached_definition(term.term, user_id=user_id)
                self._generate_glossary_definition(term.term, custom_instructions=instructions)
            return

        if key == "c":
            # Custom instructions
            self.state.glossary_show_regen_menu = False
            self.state.start_input("Custom instructions", "glossary_custom_regen")
            return

    def _generate_glossary_definition(self, term: str, custom_instructions: str = "") -> None:
        """Generate a glossary definition using AI."""
        if not self.ai_client.is_configured():
            self.state.glossary_error = "ZAI_API_KEY not configured. Set it in your environment."
            return

        self.state.glossary_loading = True
        self.state.glossary_error = ""
        self.state.glossary_stream_content = ""
        self.state.glossary_definition = None
        self.state.needs_redraw = True

        # Render loading state
        render_app(self.console, self.state)

        try:
            # Use streaming to show progress
            def stream_callback(chunk: str) -> None:
                self.state.glossary_stream_content += chunk
                self.state.needs_redraw = True
                render_app(self.console, self.state)

            entry = self.ai_client.generate_glossary_entry(
                term,
                custom_instructions=custom_instructions,
                stream_callback=stream_callback,
            )

            # Save to cache (as user override if user is logged in, shared otherwise)
            self.state.ensure_user()
            user_id = self.state.current_user.id if self.state.current_user else None
            GlossaryManager.save_definition(entry, user_id=user_id)

            # Update state
            self.state.glossary_definition = entry
            self.state.glossary_loading = False
            self.state.glossary_stream_content = ""
            self.state.glossary_focus_sidebar = False

            # Reload terms to update has_definition flag
            self.state.load_glossary_terms()

        except ZAIError as e:
            self.state.glossary_loading = False
            self.state.glossary_error = str(e.message)
            self.state.glossary_stream_content = ""
            logger.error(f"Failed to generate glossary definition: {e}")

        except Exception as e:
            self.state.glossary_loading = False
            self.state.glossary_error = f"Unexpected error: {e}"
            self.state.glossary_stream_content = ""
            logger.error(f"Unexpected error generating glossary: {e}")

    # =========================================================================
    # USER MANAGEMENT VIEWS
    # =========================================================================

    def _handle_user_management_key(self, key: str) -> None:
        """Handle keys in user management view."""
        from sawa_tui.models.users import UserManager

        # Confirm delete prompt
        if self.state.user_mgmt_confirm_delete:
            if key == "y":
                user = self.state.user_mgmt_users[self.state.user_mgmt_selected_idx]
                success, error = UserManager.delete(user.id)
                if success:
                    self.state.set_message(f"Deleted user: {user.name}")
                    self.state.load_users_for_management()
                else:
                    self.state.set_message(error, error=True)
                self.state.user_mgmt_confirm_delete = False
            elif key == "n" or key == KEY_ESCAPE:
                self.state.user_mgmt_confirm_delete = False
            return

        # Escape: go back
        if key == KEY_ESCAPE:
            self.state.navigate_to(View.SETTINGS)
            return

        # Navigation
        if key == KEY_UP:
            if self.state.user_mgmt_selected_idx > 0:
                self.state.user_mgmt_selected_idx -= 1
            return

        if key == KEY_DOWN:
            if self.state.user_mgmt_selected_idx < len(self.state.user_mgmt_users) - 1:
                self.state.user_mgmt_selected_idx += 1
            return

        # Enter: Switch to selected user
        if key == KEY_ENTER:
            if self.state.user_mgmt_users:
                user = self.state.user_mgmt_users[self.state.user_mgmt_selected_idx]
                success = UserManager.set_active(user.id)
                if success:
                    self.state.current_user = user
                    self.state.set_message(f"Switched to user: {user.name}")
                    # Reload data for new user
                    self.state.load_watchlists()
                else:
                    self.state.set_message("Failed to switch user", error=True)
            return

        # n: New user
        if key == "n":
            self.state.start_input("New user name", "create_user")
            return

        # d: Delete user
        if key == "d":
            if self.state.user_mgmt_users:
                self.state.user_mgmt_confirm_delete = True
            return

        # t: Toggle admin
        if key == "t":
            if self.state.user_mgmt_users:
                user = self.state.user_mgmt_users[self.state.user_mgmt_selected_idx]
                success, error = UserManager.toggle_admin(user.id)
                if success:
                    self.state.load_users_for_management()
                    self.state.set_message(f"Toggled admin for: {user.name}")
                else:
                    self.state.set_message(error, error=True)
            return

        # r: Rename user
        if key == "r":
            if self.state.user_mgmt_users:
                user = self.state.user_mgmt_users[self.state.user_mgmt_selected_idx]
                self.state.start_input(f"Rename '{user.name}'", "rename_user", user.name)
            return

    def _handle_user_switcher_key(self, key: str) -> None:
        """Handle keys in user switcher view."""
        from sawa_tui.models.users import UserManager

        # Escape: close switcher
        if key == KEY_ESCAPE:
            self.state.navigate_to(self.state.previous_view or View.STOCKS)
            return

        # Navigation
        if key == KEY_UP:
            if self.state.user_switcher_selected_idx > 0:
                self.state.user_switcher_selected_idx -= 1
            return

        if key == KEY_DOWN:
            if self.state.user_switcher_selected_idx < len(self.state.user_switcher_users) - 1:
                self.state.user_switcher_selected_idx += 1
            return

        # Enter: Switch to selected user
        if key == KEY_ENTER:
            if self.state.user_switcher_users:
                user = self.state.user_switcher_users[self.state.user_switcher_selected_idx]
                success = UserManager.set_active(user.id)
                if success:
                    self.state.current_user = user
                    self.state.set_message(f"Switched to user: {user.name}")
                    # Reload data for new user
                    self.state.load_watchlists()
                    self.state.navigate_to(View.STOCKS)
                else:
                    self.state.set_message("Failed to switch user", error=True)
            return

    # =========================================================================
    # SCREENER VIEW
    # =========================================================================

    def _handle_screener_key(self, key: str) -> None:
        """Handle keys in screener view."""
        if key == KEY_UP:
            if self.state.screener_selected_idx > 0:
                self.state.screener_selected_idx -= 1
                self.state.adjust_screener_scroll()
            return

        if key == KEY_DOWN:
            if self.state.screener_selected_idx < len(self.state.screener_results) - 1:
                self.state.screener_selected_idx += 1
                self.state.adjust_screener_scroll()
            return

        if key == KEY_ENTER:
            # Go to detail view
            item = self.state.current_screener_result()
            if item:
                self.state.load_stock_detail(item.ticker)
                self.state.navigate_to(View.STOCK_DETAIL)
            return


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    from pathlib import Path

    level = logging.DEBUG if verbose else logging.WARNING

    # Log to ~/.local/state/sp500-tui/app.log
    log_dir = Path.home() / ".local" / "state" / "sp500-tui"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file)],
    )


def main() -> None:
    """Main entry point for the TUI application."""
    parser = argparse.ArgumentParser(
        description="S&P 500 Terminal - Bloomberg-style TUI for stock data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  sp500-tui                     # Start the TUI
  sp500-tui --database-url ...  # Use specific database

Environment Variables:
  DATABASE_URL    PostgreSQL connection URL
  PGHOST          PostgreSQL host
  PGPORT          PostgreSQL port (default: 5432)
  PGDATABASE      PostgreSQL database name
  PGUSER          PostgreSQL username
  PGPASSWORD      PostgreSQL password
        """,
    )

    parser.add_argument(
        "--database-url",
        help="PostgreSQL database URL (overrides DATABASE_URL env var)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Load .env file
    load_dotenv()

    # Set database URL if provided
    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url

    # Set up logging
    setup_logging(args.verbose)

    # Verify database connection is configured
    db_url = os.environ.get("DATABASE_URL")
    pg_host = os.environ.get("PGHOST")

    if not db_url and not pg_host:
        print("Error: Database configuration not found.")
        print("Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD environment variables.")
        print("You can also use: sp500-tui --database-url postgresql://...")
        sys.exit(1)

    # Run the app
    app = SP500App()
    app.run()


if __name__ == "__main__":
    main()
