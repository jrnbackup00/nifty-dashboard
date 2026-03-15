import pandas as pd
import numpy as np
from database import SessionLocal
from models import MarketCandle


# -----------------------------
# SUPER TREND
# -----------------------------
def compute_supertrend(df, period=10, multiplier=2.1):

    df["H-L"] = df["High"] - df["Low"]
    df["H-PC"] = abs(df["High"] - df["Close"].shift(1))
    df["L-PC"] = abs(df["Low"] - df["Close"].shift(1))

    df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)
    df["ATR"] = df["TR"].rolling(period).mean()

    df["UpperBand"] = ((df["High"] + df["Low"]) / 2) + (multiplier * df["ATR"])
    df["LowerBand"] = ((df["High"] + df["Low"]) / 2) - (multiplier * df["ATR"])

    supertrend = []
    direction = []

    for i in range(len(df)):
        if i == 0:
            supertrend.append(np.nan)
            direction.append("NA")
            continue

        if df["Close"].iloc[i] > df["UpperBand"].iloc[i - 1]:
            direction.append("UP")
            supertrend.append(df["LowerBand"].iloc[i])
        elif df["Close"].iloc[i] < df["LowerBand"].iloc[i - 1]:
            direction.append("DOWN")
            supertrend.append(df["UpperBand"].iloc[i])
        else:
            direction.append(direction[i - 1])
            supertrend.append(supertrend[i - 1])

    df["Supertrend"] = supertrend
    df["ST_Direction"] = direction

    return df


# -----------------------------
# RESAMPLING
# -----------------------------
def resample_timeframe(df, timeframe):

    if timeframe == "weekly":
        df = df.resample("W-FRI").agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum"
        })

    elif timeframe == "monthly":
        df = df.resample("ME").agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum"
        })

    return df.dropna()


# -----------------------------
# MAIN SCANNER
# -----------------------------
def run_strategy_scan(strategy_type, timeframe, lookback, use_live_candle=False):

    db = SessionLocal()

    try:
        query = db.query(MarketCandle)

        if timeframe == "2h":
            query = query.filter(
                MarketCandle.timeframe == "2h",
                MarketCandle.symbol.in_(["^NSEI", "^NSEBANK"])
            )
        else:
            query = query.filter(MarketCandle.timeframe == "1d")

        rows = query.order_by(
            MarketCandle.symbol,
            MarketCandle.timestamp
        ).all()

    finally:
        db.close()

    if not rows:
        return []

    df = pd.DataFrame([{
        "symbol": r.symbol,
        "timestamp": r.timestamp,
        "Open": r.open,
        "High": r.high,
        "Low": r.low,
        "Close": r.close,
        "Volume": r.volume
    } for r in rows])

    results = []

    grouped = df.groupby("symbol")

    for symbol, g in grouped:

        g = g.sort_values("timestamp")
        g.set_index("timestamp", inplace=True)

        # ---------------------------------------
        # FIX: Remove duplicate daily candles
        # Yahoo sometimes returns 2 timestamps
        # (00:00 and 18:30) for the same day
        # ---------------------------------------
        """if timeframe == "daily":
            g["date"] = g.index.date
            g = g.groupby("date").last()
            g.index = pd.to_datetime(g.index)"""

        if timeframe == "daily":
        # Remove duplicate daily candles (Yahoo timezone issue)
            g = g[~g.index.normalize().duplicated(keep="last")]

        if timeframe in ["weekly", "monthly"]:
            g = resample_timeframe(g, timeframe)

        if timeframe == "daily" and len(g) < 200:
            continue

        if timeframe == "2h" and len(g) < 200:
            continue

        if timeframe in ["weekly", "monthly"] and len(g) < 30:
            continue

        # EMAs
        g["EMA5"] = g["Close"].ewm(span=5, adjust=False).mean()
        g["EMA20"] = g["Close"].ewm(span=20, adjust=False).mean()
        g["EMA55"] = g["Close"].ewm(span=55, adjust=False).mean()
        g["EMA80"] = g["Close"].ewm(span=80, adjust=False).mean()
        g["EMA200"] = g["Close"].ewm(span=200, adjust=False).mean()

        
       # Detect Crossovers
    
        g["cross_up"] = (
            (g["EMA5"] > g["EMA20"]) &
            (g["EMA5"].shift(1) <= g["EMA20"].shift(1))
        )

        g["cross_down"] = (
            (g["EMA5"] < g["EMA20"]) &
            (g["EMA5"].shift(1) >= g["EMA20"].shift(1))
        )

        

        # Determine candle set based on mode
        if use_live_candle:
            g_closed = g
        else:
            g_closed = g.iloc[:-1]

        if strategy_type == "cross_above":
            cross_points = g_closed.index[g_closed["cross_up"]]
        else:
            cross_points = g_closed.index[g_closed["cross_down"]]

        # No cross ever happened
        if len(cross_points) == 0:
            continue

        last_cross_index = cross_points[-1]

        bars_since_cross = len(g_closed) - g_closed.index.get_loc(last_cross_index) - 1

        # Apply lookback filter
        if bars_since_cross > lookback:
            continue

        # Supertrend
        g = compute_supertrend(g)

        latest = g.iloc[-1]

        results.append({
            "symbol": symbol,
            "price": round(float(latest["Close"]), 2),
            "ema5": round(float(latest["EMA5"]), 2),
            "ema20": round(float(latest["EMA20"]), 2),
            "ema55": round(float(latest["EMA55"]), 2),
            "ema80": round(float(latest["EMA80"]), 2),
            "ema200": round(float(latest["EMA200"]), 2),
            "st_direction": latest["ST_Direction"],
            "st_value": round(float(latest["Supertrend"]), 2)
                if not np.isnan(latest["Supertrend"]) else None,
            "bars_since_cross": bars_since_cross
        })

    # Sort by most recent cross first
    results = sorted(results, key=lambda x: x["bars_since_cross"])
    return results