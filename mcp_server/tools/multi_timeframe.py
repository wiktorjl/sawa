"""Multi-timeframe analysis MCP tools.

Provides tools for analyzing stocks across multiple timeframes (daily, weekly, monthly),
relative strength comparison against benchmarks, and timeframe alignment scoring.
"""

import logging
from datetime import date, timedelta
from typing import Any, Literal

from psycopg import sql

from ..database import execute_query

logger = logging.getLogger(__name__)


def get_weekly_monthly_candles(
    ticker: str,
    timeframe: Literal["weekly", "monthly"] = "weekly",
    periods: int | None = None,
) -> list[dict[str, Any]]:
    """
    Aggregate daily OHLCV data to weekly or monthly candles.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        timeframe: "weekly" or "monthly"
        periods: Number of periods to return (default: 52 for weekly, 12 for monthly)

    Returns:
        List of candle dicts with period_start, open, high, low, close,
        volume, trading_days, and change_pct.
    """
    if periods is None:
        periods = 52 if timeframe == "weekly" else 12

    max_periods = 520 if timeframe == "weekly" else 120
    periods = min(periods, max_periods)

    trunc_unit = "week" if timeframe == "weekly" else "month"

    # Calculate lookback to cover requested periods plus buffer
    if timeframe == "weekly":
        lookback_days = periods * 7 + 14
    else:
        lookback_days = periods * 31 + 62

    start_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    query = sql.SQL("""
        WITH candles AS (
            SELECT
                DATE_TRUNC({trunc_unit}, date)::date as period_start,
                (ARRAY_AGG(open ORDER BY date ASC))[1] as open,
                MAX(high) as high,
                MIN(low) as low,
                (ARRAY_AGG(close ORDER BY date DESC))[1] as close,
                SUM(volume) as volume,
                COUNT(*) as trading_days,
                MIN(date) as first_date,
                MAX(date) as last_date
            FROM stock_prices
            WHERE ticker = %(ticker)s
              AND date >= %(start_date)s
            GROUP BY DATE_TRUNC({trunc_unit}, date)
            ORDER BY period_start DESC
            LIMIT %(periods)s
        )
        SELECT
            period_start,
            first_date,
            last_date,
            open,
            high,
            low,
            close,
            volume,
            trading_days,
            CASE WHEN open > 0
                 THEN ROUND(((close - open) / open * 100)::numeric, 2)
                 ELSE NULL END as change_pct
        FROM candles
        ORDER BY period_start ASC
    """).format(trunc_unit=sql.Literal(trunc_unit))

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "periods": periods,
    }

    return execute_query(query, params)


def get_multi_timeframe_alignment(
    ticker: str,
    indicators: list[str] | None = None,
    timeframes: list[str] | None = None,
) -> dict[str, Any]:
    """
    Compare technical indicators across daily, weekly, and monthly timeframes.

    Args:
        ticker: Stock ticker symbol
        indicators: Indicators to analyze (default: ["sma_trend", "rsi", "macd"])
        timeframes: Timeframes to compare (default: ["daily", "weekly", "monthly"])

    Returns:
        Dict with per-timeframe signals, alignment_score (-100 to +100),
        trend_consistency flag, and human-readable summary.
    """
    if indicators is None:
        indicators = ["sma_trend", "rsi", "macd"]
    if timeframes is None:
        timeframes = ["daily", "weekly", "monthly"]

    ticker = ticker.upper()
    result: dict[str, Any] = {
        "ticker": ticker,
        "timeframes": {},
        "alignment_score": 0,
        "trend_consistency": False,
        "summary": "",
    }

    # Get daily indicators (latest)
    daily_query = """
        SELECT
            ti.date,
            sp.close as price,
            ti.sma_20, ti.sma_50, ti.sma_200,
            ti.rsi_14,
            ti.macd_line, ti.macd_signal, ti.macd_histogram
        FROM technical_indicators ti
        JOIN stock_prices sp ON ti.ticker = sp.ticker AND ti.date = sp.date
        WHERE ti.ticker = %(ticker)s
        ORDER BY ti.date DESC
        LIMIT 1
    """
    daily_data = execute_query(daily_query, {"ticker": ticker})

    if not daily_data:
        return {"ticker": ticker, "error": "No data found"}

    daily = daily_data[0]
    result["analysis_date"] = str(daily["date"])

    if "daily" in timeframes:
        result["timeframes"]["daily"] = _analyze_daily_signals(daily)

    # Weekly signals from aggregated candle data
    if "weekly" in timeframes:
        weekly_start = (date.today() - timedelta(days=210 * 7)).isoformat()
        weekly_candles = _fetch_aggregated_candles(ticker, "week", weekly_start)
        if weekly_candles:
            result["timeframes"]["weekly"] = _compute_signals_from_candles(weekly_candles)

    # Monthly signals from aggregated candle data
    if "monthly" in timeframes:
        monthly_start = (date.today() - timedelta(days=60 * 31)).isoformat()
        monthly_candles = _fetch_aggregated_candles(ticker, "month", monthly_start)
        if monthly_candles:
            result["timeframes"]["monthly"] = _compute_signals_from_candles(monthly_candles)

    _calculate_alignment(result)
    return result


def calculate_relative_strength(
    ticker: str,
    benchmark: str = "SPY",
    lookback_days: int = 90,
) -> dict[str, Any]:
    """
    Calculate relative strength of a stock vs a benchmark.

    Args:
        ticker: Stock ticker symbol
        benchmark: Benchmark ticker (default: "SPY")
        lookback_days: Calendar days to analyze (default: 90, max: 365)

    Returns:
        Dict with ticker/benchmark returns, relative_return, rs_trend,
        rs_line samples, beta, and outperformance statistics.
    """
    ticker = ticker.upper()
    benchmark = benchmark.upper()
    lookback_days = min(lookback_days, 365)

    start_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    query = """
        SELECT
            t.date,
            t.close as ticker_close,
            b.close as benchmark_close
        FROM stock_prices t
        INNER JOIN stock_prices b ON t.date = b.date
        WHERE t.ticker = %(ticker)s
          AND b.ticker = %(benchmark)s
          AND t.date >= %(start_date)s
        ORDER BY t.date ASC
    """

    data = execute_query(query, {
        "ticker": ticker,
        "benchmark": benchmark,
        "start_date": start_date,
    })

    if len(data) < 2:
        return {
            "ticker": ticker,
            "benchmark": benchmark,
            "error": "Insufficient data for analysis",
        }

    dates = [row["date"] for row in data]
    ticker_prices = [float(row["ticker_close"]) for row in data]
    bench_prices = [float(row["benchmark_close"]) for row in data]

    first_ticker = ticker_prices[0]
    first_bench = bench_prices[0]

    if first_ticker <= 0 or first_bench <= 0:
        return {
            "ticker": ticker,
            "benchmark": benchmark,
            "error": "Invalid price data (zero or negative prices)",
        }

    # Normalize both to 100
    ticker_norm = [p / first_ticker * 100 for p in ticker_prices]
    bench_norm = [p / first_bench * 100 for p in bench_prices]

    # RS line = ticker_normalized / benchmark_normalized * 100
    rs_line = []
    for i in range(len(ticker_norm)):
        if bench_norm[i] > 0:
            rs_line.append(round(ticker_norm[i] / bench_norm[i] * 100, 2))
        else:
            rs_line.append(None)

    # Returns
    ticker_return = round((ticker_prices[-1] / first_ticker - 1) * 100, 2)
    bench_return = round((bench_prices[-1] / first_bench - 1) * 100, 2)
    relative_return = round(ticker_return - bench_return, 2)

    # RS trend (first half avg vs second half avg)
    valid_rs = [v for v in rs_line if v is not None]
    rs_trend = _compute_rs_trend(valid_rs)

    # Beta = Cov(stock, benchmark) / Var(benchmark)
    beta = _compute_beta(ticker_prices, bench_prices)

    # Outperformance tracking (daily)
    total_trading_days = len(ticker_prices) - 1
    outperformance_days = 0
    for i in range(1, len(ticker_prices)):
        t_ret = (ticker_prices[i] - ticker_prices[i - 1]) / ticker_prices[i - 1]
        b_ret = (bench_prices[i] - bench_prices[i - 1]) / bench_prices[i - 1]
        if t_ret > b_ret:
            outperformance_days += 1

    outperformance_pct = (
        round(outperformance_days / total_trading_days * 100, 1)
        if total_trading_days > 0
        else 0
    )

    # Sample RS line (keep response size reasonable)
    rs_sampled = _sample_rs_line(dates, rs_line, ticker_norm, bench_norm)

    return {
        "ticker": ticker,
        "benchmark": benchmark,
        "period": {
            "start": str(dates[0]),
            "end": str(dates[-1]),
            "trading_days": total_trading_days,
        },
        "ticker_return": ticker_return,
        "benchmark_return": bench_return,
        "relative_return": relative_return,
        "rs_trend": rs_trend,
        "rs_current": rs_line[-1] if rs_line else None,
        "beta": beta,
        "outperformance_days": outperformance_days,
        "outperformance_pct": outperformance_pct,
        "rs_line": rs_sampled,
    }


# --- Internal helpers ---


def _fetch_aggregated_candles(
    ticker: str,
    trunc_unit: str,
    start_date: str,
) -> list[dict[str, Any]]:
    """Fetch price data aggregated by week or month."""
    query = sql.SQL("""
        SELECT
            DATE_TRUNC({trunc_unit}, date)::date as period_start,
            (ARRAY_AGG(close ORDER BY date DESC))[1] as close,
            (ARRAY_AGG(open ORDER BY date ASC))[1] as open,
            MAX(high) as high,
            MIN(low) as low,
            SUM(volume) as volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
          AND date >= %(start_date)s
        GROUP BY DATE_TRUNC({trunc_unit}, date)
        ORDER BY period_start ASC
    """).format(trunc_unit=sql.Literal(trunc_unit))

    return execute_query(query, {"ticker": ticker, "start_date": start_date})


def _analyze_daily_signals(data: dict[str, Any]) -> dict[str, Any]:
    """Extract signals from a daily technical indicators row."""
    price = data.get("price")
    signals: dict[str, Any] = {"price": price}

    sma_20 = data.get("sma_20")
    sma_50 = data.get("sma_50")
    sma_200 = data.get("sma_200")

    if price and sma_20 and sma_50 and sma_200:
        above_20 = float(price) > float(sma_20)
        above_50 = float(price) > float(sma_50)
        above_200 = float(price) > float(sma_200)
        score = sum([above_20, above_50, above_200])

        signals["sma_trend"] = {
            "above_sma_20": above_20,
            "above_sma_50": above_50,
            "above_sma_200": above_200,
            "signal": "bullish" if score >= 2 else "bearish",
            "score": score,
        }

    rsi = data.get("rsi_14")
    if rsi is not None:
        rsi_val = float(rsi)
        signals["rsi"] = {
            "value": round(rsi_val, 2),
            "signal": _rsi_signal(rsi_val),
        }

    macd_line = data.get("macd_line")
    macd_signal_val = data.get("macd_signal")
    macd_hist = data.get("macd_histogram")

    if macd_line is not None and macd_signal_val is not None:
        bullish = float(macd_line) > float(macd_signal_val)
        signals["macd"] = {
            "line": round(float(macd_line), 4),
            "signal_line": round(float(macd_signal_val), 4),
            "histogram": round(float(macd_hist), 4) if macd_hist is not None else None,
            "signal": "bullish" if bullish else "bearish",
        }

    signals["direction"] = _overall_direction(signals)
    return signals


def _compute_signals_from_candles(candles: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute SMA/RSI/MACD signals from aggregated candle data."""
    if not candles:
        return {}

    closes = [float(c["close"]) for c in candles if c.get("close") is not None]

    if len(closes) < 2:
        return {"price": closes[-1] if closes else None}

    signals: dict[str, Any] = {"price": closes[-1]}

    # SMA trend
    sma_signals: dict[str, Any] = {}
    for period, label in [(10, "short"), (20, "medium"), (50, "long")]:
        if len(closes) >= period:
            sma = sum(closes[-period:]) / period
            sma_signals[f"above_sma_{period}"] = closes[-1] > sma
            sma_signals[f"sma_{period}"] = round(sma, 2)

    if sma_signals:
        above_count = sum(1 for k, v in sma_signals.items() if k.startswith("above_") and v)
        total = sum(1 for k in sma_signals if k.startswith("above_"))
        sma_signals["signal"] = "bullish" if above_count > total / 2 else "bearish"
        sma_signals["score"] = above_count
        signals["sma_trend"] = sma_signals

    # RSI
    rsi_val = _simple_rsi(closes, 14)
    if rsi_val is not None:
        signals["rsi"] = {
            "value": round(rsi_val, 2),
            "signal": _rsi_signal(rsi_val),
        }

    # MACD
    macd_result = _simple_macd(closes)
    if macd_result is not None:
        line, sig, hist = macd_result
        signals["macd"] = {
            "line": round(line, 4),
            "signal_line": round(sig, 4),
            "histogram": round(hist, 4),
            "signal": "bullish" if line > sig else "bearish",
        }

    signals["direction"] = _overall_direction(signals)
    return signals


def _rsi_signal(rsi_val: float) -> str:
    """Classify RSI value into a signal."""
    if rsi_val >= 70:
        return "overbought"
    elif rsi_val <= 30:
        return "oversold"
    elif rsi_val >= 50:
        return "bullish"
    return "bearish"


def _overall_direction(signals: dict[str, Any]) -> str:
    """Determine overall direction from individual signals."""
    bullish_count = 0
    total = 0

    for key in ("sma_trend", "rsi", "macd"):
        if key in signals and "signal" in signals[key]:
            total += 1
            sig = signals[key]["signal"]
            if sig in ("bullish", "overbought"):
                bullish_count += 1

    if total == 0:
        return "unknown"

    # Majority wins: 2/3 -> bullish, 1/3 -> bearish, 1/2 -> neutral
    if bullish_count * 2 > total:
        return "bullish"
    elif bullish_count * 2 < total:
        return "bearish"
    return "neutral"


def _simple_rsi(closes: list[float], period: int = 14) -> float | None:
    """Calculate RSI from a list of close prices."""
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []
    for i in range(len(closes) - period, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _simple_ema(values: list[float], period: int) -> float:
    """Calculate final EMA value for a series."""
    if len(values) <= period:
        return sum(values) / len(values)

    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period

    for value in values[period:]:
        ema = (value - ema) * multiplier + ema

    return ema


def _simple_macd(
    closes: list[float],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple[float, float, float] | None:
    """Calculate MACD (line, signal, histogram) from close prices."""
    min_required = slow + signal_period
    if len(closes) < min_required:
        return None

    mult_fast = 2 / (fast + 1)
    mult_slow = 2 / (slow + 1)
    mult_signal = 2 / (signal_period + 1)

    # Build EMA-fast series
    ema_fast = sum(closes[:fast]) / fast
    ema_fast_series = [None] * (fast - 1) + [ema_fast]
    for i in range(fast, len(closes)):
        ema_fast = (closes[i] - ema_fast) * mult_fast + ema_fast
        ema_fast_series.append(ema_fast)

    # Build EMA-slow series
    ema_slow = sum(closes[:slow]) / slow
    ema_slow_series = [None] * (slow - 1) + [ema_slow]
    for i in range(slow, len(closes)):
        ema_slow = (closes[i] - ema_slow) * mult_slow + ema_slow
        ema_slow_series.append(ema_slow)

    # MACD line (valid from index slow-1 onwards)
    macd_values = []
    for i in range(slow - 1, len(closes)):
        macd_values.append(ema_fast_series[i] - ema_slow_series[i])

    if len(macd_values) < signal_period:
        return None

    # Signal line = EMA of MACD values
    signal_val = sum(macd_values[:signal_period]) / signal_period
    for i in range(signal_period, len(macd_values)):
        signal_val = (macd_values[i] - signal_val) * mult_signal + signal_val

    macd_line = macd_values[-1]
    histogram = macd_line - signal_val

    return (macd_line, signal_val, histogram)


def _calculate_alignment(result: dict[str, Any]) -> None:
    """Calculate alignment score and trend consistency across timeframes."""
    timeframes = result.get("timeframes", {})
    if not timeframes:
        return

    directions = []
    scores = []

    for tf_data in timeframes.values():
        direction = tf_data.get("direction")
        if direction and direction != "unknown":
            directions.append(direction)
            if direction == "bullish":
                scores.append(1)
            elif direction == "bearish":
                scores.append(-1)
            else:
                scores.append(0)

    if not scores:
        return

    avg_score = sum(scores) / len(scores)
    result["alignment_score"] = round(avg_score * 100)
    result["trend_consistency"] = len(set(directions)) == 1

    if result["trend_consistency"]:
        result["summary"] = f"All timeframes aligned {directions[0]}"
    else:
        tf_list = [
            f"{tf}: {data.get('direction', 'unknown')}"
            for tf, data in timeframes.items()
        ]
        result["summary"] = f"Mixed signals - {', '.join(tf_list)}"


def _compute_rs_trend(valid_rs: list[float]) -> str:
    """Determine RS line trend from first half vs second half average."""
    if len(valid_rs) < 4:
        return "insufficient_data"

    mid = len(valid_rs) // 2
    first_half_avg = sum(valid_rs[:mid]) / mid
    second_half_avg = sum(valid_rs[mid:]) / (len(valid_rs) - mid)

    if first_half_avg == 0:
        return "insufficient_data"

    pct_change = (second_half_avg - first_half_avg) / first_half_avg * 100
    if pct_change > 1:
        return "improving"
    elif pct_change < -1:
        return "declining"
    return "flat"


def _compute_beta(
    ticker_prices: list[float],
    bench_prices: list[float],
) -> float | None:
    """Calculate beta as Cov(stock, benchmark) / Var(benchmark)."""
    if len(ticker_prices) < 6:
        return None

    ticker_returns = []
    bench_returns = []
    for i in range(1, len(ticker_prices)):
        ticker_returns.append(
            (ticker_prices[i] - ticker_prices[i - 1]) / ticker_prices[i - 1]
        )
        bench_returns.append(
            (bench_prices[i] - bench_prices[i - 1]) / bench_prices[i - 1]
        )

    n = len(ticker_returns)
    mean_t = sum(ticker_returns) / n
    mean_b = sum(bench_returns) / n

    covariance = sum(
        (t - mean_t) * (b - mean_b) for t, b in zip(ticker_returns, bench_returns)
    ) / (n - 1)

    variance_b = sum((b - mean_b) ** 2 for b in bench_returns) / (n - 1)

    if variance_b == 0:
        return None

    return round(covariance / variance_b, 3)


def _sample_rs_line(
    dates: list,
    rs_line: list[float | None],
    ticker_norm: list[float],
    bench_norm: list[float],
    max_points: int = 50,
) -> list[dict[str, Any]]:
    """Sample RS line data to keep response size manageable."""
    step = max(1, len(dates) // max_points)
    sampled = []

    for i in range(0, len(dates), step):
        if rs_line[i] is not None:
            sampled.append({
                "date": str(dates[i]),
                "rs_ratio": rs_line[i],
                "ticker_normalized": round(ticker_norm[i], 2),
                "benchmark_normalized": round(bench_norm[i], 2),
            })

    # Always include the last point
    if rs_line[-1] is not None:
        last_entry = {
            "date": str(dates[-1]),
            "rs_ratio": rs_line[-1],
            "ticker_normalized": round(ticker_norm[-1], 2),
            "benchmark_normalized": round(bench_norm[-1], 2),
        }
        if not sampled or sampled[-1]["date"] != last_entry["date"]:
            sampled.append(last_entry)

    return sampled
