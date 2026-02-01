"""Application state management."""

import logging
import os
from dataclasses import dataclass, field
from enum import Enum, auto

from rich.text import Text

from sp500_tui.ai.client import GlossaryEntry
from sp500_tui.logo import get_placeholder, load_logo_async
from sp500_tui.models.glossary import GlossaryManager, GlossaryTerm
from sp500_tui.models.queries import (
    BalanceSheet,
    CashFlow,
    Company,
    FinancialRatios,
    IncomeStatement,
    Inflation,
    LaborMarket,
    NewsArticle,
    StockPrice,
    StockQueries,
    TreasuryYields,
)
from sp500_tui.models.watchlist import Watchlist, WatchlistManager, WatchlistStock

logger = logging.getLogger(__name__)


class View(Enum):
    """Available views."""

    STOCKS = auto()
    STOCK_DETAIL = auto()
    NEWS_FULLSCREEN = auto()
    FUNDAMENTALS = auto()
    ECONOMY = auto()
    SETTINGS = auto()
    GLOSSARY = auto()


class FundamentalsTab(Enum):
    """Fundamentals sub-tabs."""

    INCOME = auto()
    BALANCE = auto()
    CASHFLOW = auto()


class EconomyTab(Enum):
    """Economy sub-tabs."""

    YIELDS = auto()
    INFLATION = auto()
    LABOR = auto()


class SettingsCategory(Enum):
    """Settings categories."""

    DISPLAY = auto()
    CHARTS = auto()
    BEHAVIOR = auto()
    API = auto()


@dataclass
class AppState:
    """Complete application state."""

    # Navigation
    current_view: View = View.STOCKS
    previous_view: View | None = None

    # Stocks view
    watchlists: list[Watchlist] = field(default_factory=list)
    selected_watchlist_idx: int = 0
    watchlist_scroll_offset: int = 0  # Scroll offset for watchlist sidebar
    watchlist_stocks: list[WatchlistStock] = field(default_factory=list)
    selected_stock_idx: int = 0
    stock_scroll_offset: int = 0  # Scroll offset for stock table
    focus_sidebar: bool = False  # True = sidebar, False = stock list

    # Terminal dimensions (set by app)
    term_height: int = 24
    term_width: int = 80

    # Stock detail view
    detail_ticker: str = ""
    detail_company: Company | None = None
    detail_prices: list[StockPrice] = field(default_factory=list)
    detail_ratios: FinancialRatios | None = None
    detail_52w_high: float | None = None
    detail_52w_low: float | None = None
    detail_avg_volume: int | None = None
    # News pane state
    detail_news: list[NewsArticle] = field(default_factory=list)
    detail_news_sentiment: dict[str, int] = field(default_factory=dict)
    detail_show_news: bool = True  # Show news pane by default
    selected_news_idx: int = 0  # Selected news item index
    news_scroll_offset: int = 0  # Scroll offset for news list
    # Logo state
    detail_logo_ascii: Text | None = None
    detail_logo_loading: bool = False

    # Fundamentals view
    fund_ticker: str = ""
    fund_company: Company | None = None
    fund_tab: FundamentalsTab = FundamentalsTab.INCOME
    fund_quarterly: bool = True
    fund_income: list[IncomeStatement] = field(default_factory=list)
    fund_balance: list[BalanceSheet] = field(default_factory=list)
    fund_cashflow: list[CashFlow] = field(default_factory=list)

    # Economy view
    econ_tab: EconomyTab = EconomyTab.YIELDS
    econ_yields: list[TreasuryYields] = field(default_factory=list)
    econ_inflation: list[Inflation] = field(default_factory=list)
    econ_labor: list[LaborMarket] = field(default_factory=list)

    # Glossary view
    glossary_terms: list[GlossaryTerm] = field(default_factory=list)
    glossary_filtered: list[GlossaryTerm] = field(default_factory=list)
    glossary_search: str = ""
    selected_term_idx: int = 0
    glossary_scroll_offset: int = 0
    glossary_definition: GlossaryEntry | None = None
    glossary_loading: bool = False
    glossary_stream_content: str = ""  # Partial content during streaming
    glossary_error: str = ""  # Error message if generation failed
    glossary_show_regen_menu: bool = False  # Show regeneration options menu
    glossary_focus_sidebar: bool = True  # True = term list, False = definition

    # Settings view
    settings_category: SettingsCategory = SettingsCategory.DISPLAY
    settings_selected_idx: int = 0  # Selected item in current category
    settings_editing: bool = False  # Are we editing a value?
    settings_edit_value: str = ""  # Current edit value
    settings_popup_open: bool = False  # Is choice popup menu open?
    settings_popup_choices: list[str] = field(default_factory=list)  # Available choices
    settings_popup_idx: int = 0  # Selected index in popup
    settings_popup_label: str = ""  # Label for the setting being edited

    # UI state
    message: str = ""  # Status message to display
    message_error: bool = False  # Is message an error?
    input_mode: bool = False  # Are we in text input mode?
    input_prompt: str = ""
    input_value: str = ""
    input_callback: str = ""  # Action to take on input submit

    # App control
    running: bool = True
    needs_redraw: bool = True  # Flag to trigger redraw

    def load_watchlists(self) -> None:
        """Load all watchlists."""
        self.watchlists = WatchlistManager.get_all()
        if self.selected_watchlist_idx >= len(self.watchlists):
            self.selected_watchlist_idx = 0
        self.load_watchlist_stocks()

    def load_watchlist_stocks(self) -> None:
        """Load stocks for current watchlist."""
        if self.watchlists:
            wl = self.watchlists[self.selected_watchlist_idx]
            self.watchlist_stocks = WatchlistManager.get_stocks(wl.id)
        else:
            self.watchlist_stocks = []
        if self.selected_stock_idx >= len(self.watchlist_stocks):
            self.selected_stock_idx = 0
        self.stock_scroll_offset = 0  # Reset scroll on watchlist change

    def get_visible_stock_rows(self) -> int:
        """Calculate how many stock rows fit in the visible area."""
        # Header=3, Footer=3, Panel borders=2, Table header=1, padding=2
        # Available: term_height - 3 - 3 - 2 - 1 - 2 = term_height - 11
        return max(1, self.term_height - 11)

    def get_visible_watchlist_rows(self) -> int:
        """Calculate how many watchlist rows fit in the visible area."""
        # Same as stocks but sidebar has help text at bottom (~3 lines)
        return max(1, self.term_height - 14)

    def adjust_stock_scroll(self) -> None:
        """Adjust scroll offset to keep selected stock visible."""
        visible = self.get_visible_stock_rows()
        # Scroll down if selection below visible area
        if self.selected_stock_idx >= self.stock_scroll_offset + visible:
            self.stock_scroll_offset = self.selected_stock_idx - visible + 1
        # Scroll up if selection above visible area
        if self.selected_stock_idx < self.stock_scroll_offset:
            self.stock_scroll_offset = self.selected_stock_idx

    def adjust_watchlist_scroll(self) -> None:
        """Adjust scroll offset to keep selected watchlist visible."""
        visible = self.get_visible_watchlist_rows()
        if self.selected_watchlist_idx >= self.watchlist_scroll_offset + visible:
            self.watchlist_scroll_offset = self.selected_watchlist_idx - visible + 1
        if self.selected_watchlist_idx < self.watchlist_scroll_offset:
            self.watchlist_scroll_offset = self.selected_watchlist_idx

    def current_watchlist(self) -> Watchlist | None:
        """Get currently selected watchlist."""
        if self.watchlists:
            return self.watchlists[self.selected_watchlist_idx]
        return None

    def current_stock(self) -> WatchlistStock | None:
        """Get currently selected stock."""
        if self.watchlist_stocks:
            return self.watchlist_stocks[self.selected_stock_idx]
        return None

    def load_stock_detail(self, ticker: str) -> None:
        """Load detailed data for a stock."""
        self.detail_ticker = ticker
        self.detail_company = StockQueries.get_company(ticker)
        self.detail_prices = StockQueries.get_stock_prices(ticker, days=90)
        self.detail_ratios = StockQueries.get_latest_ratios(ticker)
        self.detail_52w_high, self.detail_52w_low = StockQueries.get_52_week_range(ticker)
        # Calculate average volume from price data
        if self.detail_prices:
            volumes = [p.volume for p in self.detail_prices if p.volume]
            self.detail_avg_volume = int(sum(volumes) / len(volumes)) if volumes else None
        else:
            self.detail_avg_volume = None
        # Load news and sentiment (past 30 days)
        self.detail_news = StockQueries.get_news(ticker, limit=20)
        self.detail_news_sentiment = StockQueries.get_news_sentiment_summary(ticker, days=30)

        # If no news found, try to fetch from API
        if not self.detail_news:
            self._fetch_news_from_api(ticker)

        # Load company logo
        self._load_logo()

    def _fetch_news_from_api(self, ticker: str) -> None:
        """Fetch news from Polygon API if not in database."""
        api_key = os.environ.get("POLYGON_API_KEY")
        if not api_key:
            logger.debug("POLYGON_API_KEY not set, skipping news fetch")
            return

        try:
            from sp500_tools.api.client import PolygonClient
            from sp500_tools.database.news import fetch_and_load_news

            from sp500_tui.database import get_connection

            client = PolygonClient(api_key)

            with get_connection() as conn:
                fetch_and_load_news(conn, client, ticker=ticker, days=30, limit=100, logger=logger)

            # Reload news from database
            self.detail_news = StockQueries.get_news(ticker, limit=20)
            self.detail_news_sentiment = StockQueries.get_news_sentiment_summary(ticker, days=30)
            logger.info(f"Fetched {len(self.detail_news)} news articles for {ticker}")

        except Exception as e:
            logger.warning(f"Failed to fetch news from API: {e}")

    def _load_logo(self) -> None:
        """Load company logo asynchronously."""
        from sp500_tui.config import get_tui_config

        config = get_tui_config()
        if not config.logo_enabled:
            self.detail_logo_ascii = None
            self.detail_logo_loading = False
            return

        company = self.detail_company
        if not company or not company.logo_url:
            self.detail_logo_ascii = get_placeholder(config.logo_width, config.logo_height)
            self.detail_logo_loading = False
            return

        self.detail_logo_loading = True
        self.detail_logo_ascii = None

        def on_logo_loaded(result: Text | None) -> None:
            self.detail_logo_loading = False
            self.detail_logo_ascii = result or get_placeholder(
                config.logo_width, config.logo_height
            )
            self.needs_redraw = True

        load_logo_async(company.logo_url, config.logo_width, config.logo_height, on_logo_loaded)

    def toggle_news_pane(self) -> None:
        """Toggle visibility of the news pane."""
        self.detail_show_news = not self.detail_show_news

    def current_news_article(self) -> NewsArticle | None:
        """Get currently selected news article."""
        if self.detail_news and self.selected_news_idx < len(self.detail_news):
            return self.detail_news[self.selected_news_idx]
        return None

    def get_visible_news_rows(self) -> int:
        """Calculate how many news rows fit in the visible area."""
        # Full screen: Header=3, Footer=3, Panel borders=2, sentiment header=3
        return max(1, self.term_height - 11)

    def adjust_news_scroll(self) -> None:
        """Adjust scroll offset to keep selected news visible."""
        visible = self.get_visible_news_rows()
        if self.selected_news_idx >= self.news_scroll_offset + visible:
            self.news_scroll_offset = self.selected_news_idx - visible + 1
        if self.selected_news_idx < self.news_scroll_offset:
            self.news_scroll_offset = self.selected_news_idx

    def load_fundamentals(self, ticker: str) -> None:
        """Load fundamentals for a ticker."""
        self.fund_ticker = ticker
        self.fund_company = StockQueries.get_company(ticker)
        timeframe = "quarterly" if self.fund_quarterly else "annual"
        self.fund_income = StockQueries.get_income_statements(ticker, timeframe=timeframe, limit=12)
        self.fund_balance = StockQueries.get_balance_sheets(ticker, timeframe=timeframe, limit=12)
        self.fund_cashflow = StockQueries.get_cash_flows(ticker, timeframe=timeframe, limit=12)

    def load_economy(self) -> None:
        """Load economy data."""
        self.econ_yields = StockQueries.get_treasury_yields(limit=20)
        self.econ_inflation = StockQueries.get_inflation(limit=20)
        self.econ_labor = StockQueries.get_labor_market(limit=20)

    def load_glossary_terms(self) -> None:
        """Load all glossary terms."""
        self.glossary_terms = GlossaryManager.get_all_terms()
        self.glossary_filtered = self.glossary_terms.copy()
        if self.selected_term_idx >= len(self.glossary_filtered):
            self.selected_term_idx = 0
        self.glossary_scroll_offset = 0

    def filter_glossary_terms(self, search: str = "") -> None:
        """Filter glossary terms by search string."""
        self.glossary_search = search
        if search:
            self.glossary_filtered = GlossaryManager.get_all_terms(search)
        else:
            self.glossary_filtered = self.glossary_terms.copy()
        self.selected_term_idx = 0
        self.glossary_scroll_offset = 0

    def current_glossary_term(self) -> GlossaryTerm | None:
        """Get currently selected glossary term."""
        if self.glossary_filtered and self.selected_term_idx < len(self.glossary_filtered):
            return self.glossary_filtered[self.selected_term_idx]
        return None

    def load_glossary_definition(self, term: str) -> bool:
        """
        Load cached glossary definition for a term.

        Returns True if definition was found in cache.
        """
        cached = GlossaryManager.get_cached_definition(term)
        if cached:
            self.glossary_definition = cached.to_glossary_entry()
            self.glossary_error = ""
            return True
        self.glossary_definition = None
        return False

    def get_visible_glossary_rows(self) -> int:
        """Calculate how many glossary term rows fit in the visible area."""
        # Header=3, Footer=3, Panel borders=2, search box=2, help text=2
        return max(1, self.term_height - 12)

    def adjust_glossary_scroll(self) -> None:
        """Adjust scroll offset to keep selected term visible."""
        visible = self.get_visible_glossary_rows()
        if self.selected_term_idx >= self.glossary_scroll_offset + visible:
            self.glossary_scroll_offset = self.selected_term_idx - visible + 1
        if self.selected_term_idx < self.glossary_scroll_offset:
            self.glossary_scroll_offset = self.selected_term_idx

    def set_message(self, msg: str, error: bool = False) -> None:
        """Set status message."""
        self.message = msg
        self.message_error = error

    def clear_message(self) -> None:
        """Clear status message."""
        self.message = ""
        self.message_error = False

    def start_input(self, prompt: str, callback: str, initial: str = "") -> None:
        """Start text input mode."""
        self.input_mode = True
        self.input_prompt = prompt
        self.input_value = initial
        self.input_callback = callback

    def cancel_input(self) -> None:
        """Cancel text input mode."""
        self.input_mode = False
        self.input_prompt = ""
        self.input_value = ""
        self.input_callback = ""

    def navigate_to(self, view: View) -> None:
        """Navigate to a view."""
        self.previous_view = self.current_view
        self.current_view = view
        self.clear_message()
