-- trader_cards: stores parsed analysis output from chart-analysis card.md files

CREATE TABLE trader_cards (
    -- Keys
    ticker          VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    analysis_date   DATE NOT NULL,
    PRIMARY KEY (ticker, analysis_date),

    -- Trader Card header
    bias            VARCHAR(30),
    confidence      SMALLINT,
    confidence_bull SMALLINT,
    confidence_bear SMALLINT,
    grade           VARCHAR(3),
    regime          VARCHAR(40),
    atr_14          NUMERIC(12,4),
    atr_14_pct      NUMERIC(6,2),
    volume_ratio    NUMERIC(6,2),

    -- Levels & Triggers
    long_trigger    NUMERIC(12,4),
    long_volume     NUMERIC(6,2),
    short_trigger   NUMERIC(12,4),
    short_volume    NUMERIC(6,2),
    invalidation_long  NUMERIC(12,4),
    invalidation_short NUMERIC(12,4),
    upside_magnet   NUMERIC(12,4),
    downside_magnet NUMERIC(12,4),
    event_level     TEXT,

    -- Why
    bull_point_1    TEXT,
    bull_point_2    TEXT,
    bear_point_1    TEXT,
    bear_point_2    TEXT,
    tiebreaker      TEXT,

    -- Base Case
    base_expected   TEXT,
    base_alternate  TEXT,

    -- One Trade (NULL when grade=C / WAIT)
    trade_strategy  TEXT,
    trade_entry     NUMERIC(12,4),
    trade_entry_high NUMERIC(12,4),
    trade_stop      NUMERIC(12,4),
    trade_tp1       NUMERIC(12,4),
    trade_tp2       NUMERIC(12,4),
    trade_position_pct NUMERIC(5,2),
    trade_rr        VARCHAR(10),
    trade_win_condition TEXT,

    -- WAIT fields (NULL when grade >= B)
    wait_action     TEXT,
    wait_upgrade_1  TEXT,
    wait_upgrade_2  TEXT,
    wait_upgrade_3  TEXT,

    -- Metadata
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trader_cards_date ON trader_cards (analysis_date DESC);
CREATE INDEX idx_trader_cards_grade ON trader_cards (grade);
CREATE INDEX idx_trader_cards_bias ON trader_cards (bias);
