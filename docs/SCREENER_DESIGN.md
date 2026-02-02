# Design Document: Stock Screener

## 1. Overview
The Stock Screener is a new feature for the S&P 500 TUI application that allows users to filter the universe of companies based on financial metrics, sector, and price data.

**Goal:** Provide a flexible, "bloomberg-terminal-like" filtering experience where users can type complex logic (e.g., `pe < 15 and yield > 0.03`) and see instant results.

## 2. Architecture: The "In-Memory Universe"
Given the dataset size (S&P 500 = ~500 rows), we have opted for an **In-Memory** architecture rather than Dynamic SQL generation.

### Rationale
*   **Data Volume:** 500 rows × 50 columns is < 1MB of RAM. Even scaling to 5,000 tickers, memory usage is negligible.
*   **Performance:** Python's `eval()` loop over 500 items takes microseconds. It is faster than the network round-trip to PostgreSQL.
*   **Flexibility:** Python expressions allow for complex logic (`pe * pb < 22.5`, string matching) that is difficult to safely expose via SQL generation.

## 3. Data Model

### `ScreenerResult`
A unified dataclass representing a single row in the screener table. It flattens data from multiple SQL tables.

```python
@dataclass
class ScreenerResult:
    ticker: str
    name: str
    sector: str
    price: float
    market_cap: float
    pe: float          # Price to Earnings
    pb: float          # Price to Book
    dividend_yield: float
    debt_to_equity: float
    roe: float         # Return on Equity
    eps: float
```

### Data Fetching
*   **Source:** A single optimized SQL query joining `companies`, `stock_prices` (latest), and `financial_ratios` (latest).
*   **Loading Strategy:** Lazy loading. The data is fetched only when the user first navigates to the Screener view. It is then cached in `AppState`.

## 4. Logic Engine (`ScreenerEngine`)

The engine is responsible for safely evaluating user input against the dataset.

### Evaluation Strategy
We use Python's built-in `eval()` function with a strictly restricted `locals` dictionary.

*   **Input:** User query string (e.g., `sector == "Technology" and pe < 20`).
*   **Context:** For each row, variables like `pe`, `yield`, `price` are mapped to the object's values.
*   **Safety:** `__builtins__` is set to `{}`. Only safe helper functions (like `len`, `startswith`) are exposed.

## 5. UI Design

### Layout
*   **Header (3 lines):**
    *   Input box for the query.
    *   Status/Error message area.
*   **Body (Remaining):**
    *   Scrollable `Table` component.
    *   Columns: Ticker, Name, Sector, Price, P/E, Yield, ROE, Market Cap.

### Interaction
*   **`F6`**: Navigate to Screener.
*   **`/`**: Focus input box.
*   **`Enter`**: Navigate to the Stock Detail view for the selected row.

## 6. Scalability Analysis
*   **Current Scale:** 500 rows. Response time: < 1ms.
*   **Future Scale:** 5,000 rows. Response time: ~50ms.
*   **Threshold:** If dataset exceeds 50,000 rows, we would migrate the engine to `pandas` for vectorized evaluation, keeping the same API.

## 7. Implementation Plan
1.  **Backend:** Add `ScreenerResult` model and `StockQueries.get_screener_universe()`.
2.  **Logic:** Implement `ScreenerEngine` in `sp500_tui/screener.py`.
3.  **State:** Add `screener_universe` and `screener_results` to `AppState`.
4.  **UI:** Implement `render_screener_view` in `sp500_tui/views_screener.py`.
5.  **Integration:** Wire up keyboard shortcuts in `app.py`.
