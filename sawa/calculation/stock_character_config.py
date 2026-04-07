"""Configuration for stock character classification.

All thresholds are tunable. Adjust and re-run classification to see impact.
"""

# === Stage 1: Classification Thresholds ===
HURST_RANGE_THRESHOLD = 0.45       # H below this = range-bound
HURST_TREND_THRESHOLD = 0.55       # H above this = trending
ADX_RANGE_MAX = 25                 # ADX below this confirms range
REGRESSION_R2_MIN = 0.70           # R² above this confirms trend
VOL_OF_VOL_THRESHOLD = 0.50        # boom-bust identifier

# DFA parameters
DFA_MIN_SCALE = 10                 # minimum box size for DFA
DFA_MAX_SCALE_RATIO = 0.25        # max scale = series_length * this
DFA_NUM_SCALES = 20                # number of log-spaced scales

# Classification windows (trading days)
WINDOW_FULL = 756                  # ~3 years
WINDOW_2YR = 504                   # ~2 years
WINDOW_1YR = 252                   # ~1 year
MIN_HISTORY_DAYS = 504             # minimum days required to classify

# ADX parameters
ADX_PERIOD = 20

# === Stage 2: Baseline Parameters ===
CORRELATION_WINDOW = 90            # rolling correlation window (days)
VOLUME_PROFILE_BUCKETS = 50        # price buckets for HVN/LVN
HVN_VOLUME_PERCENTILE = 70         # above this = HVN
LVN_VOLUME_PERCENTILE = 20         # below this = LVN
RANGE_HIGH_PERCENTILE = 98         # percentile for range high
RANGE_LOW_PERCENTILE = 2           # percentile for range low
BENCHMARK_TICKERS = ["SPY", "GLD", "TLT"]

# SMA adherence parameters
SMA_TOUCH_PROXIMITY_PCT = 0.015    # within 1.5% = "touch"
SMA_BOUNCE_CONFIRM_DAYS = 10       # days to confirm bounce after touch
SMA_BOUNCE_MIN_MOVE_PCT = 0.02     # 2% move away from SMA = confirmed bounce
SMA_ADHERENCE_MIN_RATIO = 0.60     # 60% touch-and-bounce ratio = adheres

# === Stage 3: Atypical Detection ===
RECENT_WINDOW_DAYS = 10            # lookback for atypical detection
EXTREMUM_HIGH_PCT = 90             # percentile threshold for high extreme
EXTREMUM_LOW_PCT = 10              # percentile threshold for low extreme
COMPRESSION_RATIO = 0.60           # recent/baseline ATR below this = compression
EXPANSION_RATIO = 1.80             # recent/baseline ATR above this = expansion
DECORRELATION_STDDEV = 2.0         # stddev threshold for correlation break
VOLUME_SPIKE_RATIO = 2.0           # volume spike multiplier
VOLUME_DROUGHT_RATIO = 0.50        # volume drought multiplier
SMA_PROXIMITY_PCT = 0.015          # within 1.5% of SMA = "at SMA"
HVN_PROXIMITY_PCT = 0.01           # within 1% of HVN level
SIGMA_THRESHOLD = 2.0              # sigma bands for trending stocks
