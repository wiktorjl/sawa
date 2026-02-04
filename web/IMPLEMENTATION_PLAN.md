# ORION Web Implementation Plan - TUI Feature Parity

## Current Status

### Implemented
- [x] Login/Authentication with session management
- [x] Dashboard with market stats, watchlist preview, top movers
- [x] Stocks page with watchlist management
- [x] Stock detail page with price chart, key stats, news, AI overview display
- [x] Add/remove stocks from watchlist
- [x] Stock search
- [x] ORION sci-fi design system

### Placeholder Pages (Need Implementation)
- [ ] Fundamentals
- [ ] Economy
- [ ] Screener
- [ ] Glossary
- [ ] Settings
- [ ] User Management (Admin)

---

## Phase 1: Fundamentals Page

### Features to Implement
1. **Three-tab layout**: Income Statement, Balance Sheet, Cash Flow
2. **Data table** with historical periods (quarters/years)
3. **Timeframe toggle**: Quarterly vs Annual
4. **Ticker search/selection**
5. **Sparkline trend charts** for key metrics

### Database Queries Needed
- `fundamentals_income` - Revenue, Gross Profit, Operating Income, Net Income, EPS, EBITDA
- `fundamentals_balance` - Total Assets, Liabilities, Equity, Cash, Debt
- `fundamentals_cashflow` - Operating CF, Investing CF, Financing CF, Net Change, CapEx

### Files to Create/Modify
- `web/sawa_web/routes/fundamentals.py` - Route handlers
- `web/sawa_web/templates/fundamentals/index.html` - Main page
- `web/sawa_web/templates/fundamentals/partials/income.html`
- `web/sawa_web/templates/fundamentals/partials/balance.html`
- `web/sawa_web/templates/fundamentals/partials/cashflow.html`

### Estimated Effort: Medium

---

## Phase 2: Economy Page

### Features to Implement
1. **Three-tab layout**: Treasury Yields, Inflation, Labor Market
2. **Summary panel** with key indicators and trends
3. **Yield curve inversion warning**
4. **Historical data tables** with trend indicators
5. **Sparkline charts** for key metrics

### Database Queries Needed
- `treasury_yields` - 1M through 30Y yields by date
- `inflation` - CPI, CPI Core, CPI YoY, PCE, PCE Core
- `labor_market` - Unemployment rate, participation rate, hourly earnings, job openings

### Files to Create/Modify
- `web/sawa_web/routes/economy.py`
- `web/sawa_web/templates/economy/index.html`
- `web/sawa_web/templates/economy/partials/yields.html`
- `web/sawa_web/templates/economy/partials/inflation.html`
- `web/sawa_web/templates/economy/partials/labor.html`

### Estimated Effort: Medium

---

## Phase 3: Glossary Page

### Features to Implement
1. **Two-panel layout**: Term list (left), Definition (right)
2. **Search/filter** terms
3. **AI-generated definitions** with streaming display
4. **Regeneration options**: Technical, Simple, More Examples, Custom
5. **Category badges** (VAL, PRF, LIQ, etc.)
6. **Related terms** with clickable links
7. **User-added terms** support
8. **Learn More links** (Investopedia)

### Database Queries Needed
- `glossary_terms` - Term list with categories
- AI generation via Z.AI API

### Files to Create/Modify
- `web/sawa_web/routes/glossary.py`
- `web/sawa_web/services/ai_service.py` - Z.AI integration
- `web/sawa_web/templates/glossary/index.html`
- `web/sawa_web/templates/glossary/partials/term_list.html`
- `web/sawa_web/templates/glossary/partials/definition.html`

### Estimated Effort: High (AI integration + streaming)

---

## Phase 4: Screener Page

### Features to Implement
1. **Query input** with natural language-like syntax
2. **Real-time validation** and error messages
3. **Results table** with sortable columns
4. **Click to view stock detail**
5. **Supported filters**: pe, yield, roe, price, market_cap, change, volume, sector

### Query Parser
- Parse expressions like `pe < 15 and yield > 0.03`
- Support operators: <, >, <=, >=, ==, !=
- Support logical: and, or

### Files to Create/Modify
- `web/sawa_web/routes/screener.py`
- `web/sawa_web/services/screener_service.py` - Query parsing
- `web/sawa_web/templates/screener/index.html`
- `web/sawa_web/templates/screener/partials/results.html`

### Estimated Effort: Medium-High (query parser)

---

## Phase 5: Settings Page

### Features to Implement
1. **Tabbed categories**: Display, Charts, Behavior, API Keys, Account
2. **Theme selection** (11 themes - adapt for web CSS variables)
3. **Chart period** setting (30, 60, 90, 180, 365 days)
4. **Number format** (compact vs full)
5. **API key management** (Z.AI) with masked input
6. **Default timeframe** (quarterly/annual)

### Database Queries Needed
- `user_settings` - Key-value per user
- `default_settings` - Template for new users

### Files to Create/Modify
- `web/sawa_web/routes/settings.py`
- `web/sawa_web/templates/settings/index.html`
- `web/sawa_web/static/css/themes/` - Multiple theme CSS files

### Estimated Effort: Medium

---

## Phase 6: User Management (Admin)

### Features to Implement
1. **User list** with admin badges
2. **Create new user**
3. **Delete user** with confirmation
4. **Toggle admin status**
5. **Rename user**
6. **Password management** (web-specific)

### Database Queries Needed
- `users` table operations

### Files to Create/Modify
- `web/sawa_web/routes/admin.py`
- `web/sawa_web/templates/admin/users.html`
- `web/sawa_web/templates/admin/partials/user_row.html`

### Estimated Effort: Medium

---

## Phase 7: Enhanced Stock Detail

### Features to Add
1. **News with sentiment indicators** (+/-/~)
2. **AI overview regeneration** with options (bullish/bearish/technical/custom)
3. **Streaming AI generation** display
4. **52-week range visualization**
5. **Company description panel**
6. **More financial ratios**

### Files to Modify
- `web/sawa_web/routes/stocks.py` - Add AI endpoints
- `web/sawa_web/templates/stocks/detail.html` - Enhanced layout
- `web/sawa_web/services/ai_service.py` - Overview generation

### Estimated Effort: High (AI streaming)

---

## Phase 8: Watchlist Enhancements

### Features to Add
1. **Create new watchlist**
2. **Rename watchlist**
3. **Delete watchlist**
4. **Set default watchlist**
5. **Reorder stocks** (drag & drop)

### Files to Modify
- `web/sawa_web/routes/stocks.py` - Watchlist CRUD
- `web/sawa_web/templates/stocks/list.html` - Modals for actions

### Estimated Effort: Low-Medium

---

## Phase 9: User Preferences & Themes

### Features to Implement
1. **Theme switcher** (real-time preview)
2. **CSS variables per theme**
3. **Persist preference** in database
4. **Apply on page load**

### Theme List
- Default (current ORION)
- Osaka Jade
- Mono
- High Contrast
- Dracula
- Catppuccin
- Gruvbox
- Nord
- Tokyo Night
- Solarized
- One Dark

### Files to Create
- `web/sawa_web/static/css/themes/*.css` - Theme overrides
- JavaScript theme switcher

### Estimated Effort: Medium

---

## Technical Considerations

### AI Integration (Z.AI API)
- Server-Sent Events (SSE) for streaming responses
- HTMX `hx-sse` extension for real-time updates
- Rate limiting and error handling
- Caching generated content

### Database Optimizations
- Indexes for frequent queries
- Connection pooling (already implemented)
- Query result caching for static data

### Frontend Enhancements
- Chart.js for all visualizations
- HTMX for dynamic updates
- CSS transitions for smooth UX

---

## Implementation Order (Recommended)

1. **Phase 5: Settings** - Needed for API key management
2. **Phase 1: Fundamentals** - Core financial data
3. **Phase 2: Economy** - Economic indicators
4. **Phase 3: Glossary** - AI feature foundation
5. **Phase 7: Enhanced Stock Detail** - AI overview generation
6. **Phase 4: Screener** - Advanced search
7. **Phase 6: User Management** - Admin features
8. **Phase 8: Watchlist Enhancements** - UX improvements
9. **Phase 9: Themes** - Polish

---

## Summary

| Phase | Feature | Priority | Effort |
|-------|---------|----------|--------|
| 5 | Settings | High | Medium |
| 1 | Fundamentals | High | Medium |
| 2 | Economy | High | Medium |
| 3 | Glossary | Medium | High |
| 7 | Stock Detail AI | Medium | High |
| 4 | Screener | Medium | Medium-High |
| 6 | User Management | Low | Medium |
| 8 | Watchlist Enhancements | Low | Low-Medium |
| 9 | Themes | Low | Medium |

**Total estimated phases**: 9
**Core functionality (Phases 1-5)**: Essential for TUI parity
**Enhanced features (Phases 6-9)**: Nice-to-have improvements
