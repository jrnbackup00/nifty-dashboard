import pandas as pd
import numpy as np
from database import SessionLocal
from models import MarketCandle
from signal_scan_service import get_inverse_hammer_signals
from models import Signal, Symbol

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

def run_ema_scan(
    strategy_type,
    timeframe,
    lookback,
    use_live_candle=False,
    universe="all"
):
    results = []

    fno_symbols = set()

    if universe == "fno":

        db = SessionLocal()

        try:

            fno_symbols = set(
                x[0]
                for x in db.query(Symbol.symbol)
                .filter(Symbol.is_fno == True)
                .all()
            )

        finally:
            db.close()

    ## REGULAR SCANNER CODE 

    db = SessionLocal()

    try:
        query = db.query(MarketCandle)

        if timeframe == "2h":

            query = query.filter(
                MarketCandle.timeframe == "2h"
            )

        else:

            query = query.filter(
                MarketCandle.timeframe == "1d"
            )

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

    iterator = grouped

    # ---------------------------------------
    # Common iterator
    # ---------------------------------------

    for symbol, g in iterator:
        if universe == "fno" and symbol not in fno_symbols:
            continue
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

       ## if strategy_type == "cross_above":
         ##   cross_points = g_closed.index[g_closed["cross_up"]]
       ## else:
         ##   cross_points = g_closed.index[g_closed["cross_down"]]

    
        cross_points = []

        if strategy_type == "cross_above":

            cross_points = g_closed.index[
            g_closed["cross_up"]
            ]

        elif strategy_type == "cross_below":

            cross_points = g_closed.index[
            g_closed["cross_down"]
            ]

        # -----------------------------------
        # EMA Cross scanners only
        # -----------------------------------

        if strategy_type in ["cross_above", "cross_below"]:

            # No cross ever happened
            if len(cross_points) == 0:
                continue

            last_cross_index = cross_points[-1]

            bars_since_cross = (
                len(g_closed)
                - g_closed.index.get_loc(last_cross_index)
                - 1
            )

        

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
            "st_value": (round(float(latest["Supertrend"]), 2)
                if not np.isnan(latest["Supertrend"])
                else None
            ),
            "bars_since_cross": bars_since_cross
        })

    # Sort by most recent cross first
    # results = sorted(results, key=lambda x: x["bars_since_cross"])
    
    results = sorted(
    results,
    key=lambda x: x["bars_since_cross"]
    )

    return results
    
def run_strategy_scan(
    strategy_type,
    timeframe,
    lookback,
    use_live_candle=False,
    universe="all",
    signal_window="today",
    ema_filter=None
):
    if strategy_type in ["cross_above", "cross_below"]:

        return run_ema_scan(
            strategy_type=strategy_type,
            timeframe=timeframe,
            lookback=lookback,
            use_live_candle=use_live_candle,
            universe=universe
        )
    ## ABOVE IF BLOCK ADDED FOR EMA DEFUNCTION LOGIC

    # -----------------------------------
    # BIG MOVE SCANNER
    # -----------------------------------

    if strategy_type == "big_move":

        signal_tf = timeframe

        if timeframe == "2h":
            signal_tf = "120"

        db = SessionLocal()

        try:
            signal_query = db.query(Signal).filter(
                Signal.signal_type == "inverse_hammer",
                Signal.timeframe == signal_tf
            )
            
            # -----------------------------------
            # SIGNAL WINDOW FILTER
            # -----------------------------------

            now = pd.Timestamp.now(tz="UTC")

            if signal_window == "today":

                start = now.normalize()

                signal_query = signal_query.filter(
                    Signal.timestamp >= start
                )

            elif signal_window == "yesterday":

                today_start = now.normalize()

                yesterday_start = today_start - pd.Timedelta(days=1)

                signal_query = signal_query.filter(
                    Signal.timestamp >= yesterday_start,
                    Signal.timestamp < today_start
                )

            elif signal_window == "week":

                week_start = now.normalize() - pd.Timedelta(days=7)

                signal_query = signal_query.filter(
                    Signal.timestamp >= week_start
                )

            signals = signal_query.all()
            print("DEBUG SIGNAL COUNT:", len(signals))
            print("DEBUG SIGNAL TF:", signal_tf)
            print("DEBUG UNIQUE SIGNAL SYMBOLS:",len(set(s.symbol for s in signals)))

        finally:
            db.close()

        if not signals:
            return []
        
        results = []

        for signal in signals:

            results.append({
                "symbol": signal.symbol,
                "signal_time": signal.timestamp,
                "price": None,
                "ema5": None,
                "ema20": None,
                "trend": "N/A",
                "bias": "N/A",
                "st_direction": "N/A"
            })

        print("DEBUG RESULTS COUNT:", len(results))

        ## return results

        signal_symbols = list(set([
            s.symbol for s in signals
        ]))

    # -----------------------------------
    # UNIVERSE FILTER
    # -----------------------------------

    if universe == "fno":

        db = SessionLocal()

        try:

            fno_symbols = db.query(Symbol.symbol).filter(
                Symbol.is_fno == True
            ).all()

            fno_symbols = set([x[0] for x in fno_symbols])

            signal_symbols = [
                s for s in signal_symbols
                if s in fno_symbols
            ]

        finally:
            db.close()
    
    # -----------------------------------
    # EMA CONFIRMATION FILTER
    # -----------------------------------

    ema_confirmed = None

    

    if (strategy_type == "big_move"
        and ema_filter in ["cross_above", "cross_below"]
        ):

        ema_results = run_ema_scan(
            strategy_type=ema_filter,
            timeframe=timeframe,
            lookback=lookback,
            use_live_candle=use_live_candle,
            universe=universe
        )

        ema_confirmed = set([
            r["symbol"] for r in ema_results
        ])

        signal_symbols = [
            s for s in signal_symbols
            if s in ema_confirmed
        ]

    

    

    ## REGULAR SCANNER CODE 
    
    db = SessionLocal()

    try:
        query = db.query(MarketCandle)

        if timeframe == "2h":

            query = query.filter(
                MarketCandle.timeframe == "2h"
            )

        else:

            query = query.filter(
                MarketCandle.timeframe == "1d"
            )

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

    # ---------------------------------------------------
    # BIG MOVE uses every signal occurrence
    # EMA scanners continue to use grouped iterator
    # ---------------------------------------------------

    if strategy_type == "big_move":

        grouped_dict = {
            symbol: g
            for symbol, g in grouped
        }

        ##print("FIRST 20 GROUPED SYMBOLS:")
        ##print(list(grouped_dict.keys())[:20])

        iterator = []

        for signal in signals:

            if signal.symbol not in signal_symbols:
                continue
            
            ##print("LOOKUP:", repr(signal.symbol))
            """ if signal.symbol not in grouped_dict:
                print(
                    "MISSING:",
                    signal.symbol,
                    signal.timeframe,
                    signal.timestamp
                )
                continue """

            iterator.append(
                (
                    signal,
                    grouped_dict.get(signal.symbol)
                )
            )
        print("ITERATOR LENGTH:", len(iterator))

    else:

        iterator = grouped

    # ---------------------------------------
    # Common iterator
    # ---------------------------------------
    processed = 0
    for item in iterator:
        processed += 1

        if strategy_type == "big_move":

            signal = item[0]
            symbol = signal.symbol
            g = item[1]

        else:

            symbol, g = item
        
        if strategy_type == "big_move" and g is None:
            
            signal_time = pd.Timestamp(signal.timestamp)

            if signal_time.tzinfo is None:
                signal_time = signal_time.tz_localize("UTC")

            signal_time_ist = (
                signal_time
                .tz_convert("Asia/Kolkata")
                .strftime("%d-%b-%Y %I:%M %p")
            )
            results.append({
                "symbol": symbol,
                "price": None,
                "ema5": None,
                "ema20": None,
                "ema55": None,
                "ema80": None,
                "ema200": None,
                "trend": "No Candle Data",
                "st_direction": None,
                "st_value": None,
                "bars_since_signal": signal_time_ist
            })

            continue

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

       ## if strategy_type == "cross_above":
         ##   cross_points = g_closed.index[g_closed["cross_up"]]
       ## else:
         ##   cross_points = g_closed.index[g_closed["cross_down"]]

    
        cross_points = []

        if strategy_type == "cross_above":

            cross_points = g_closed.index[
            g_closed["cross_up"]
            ]

        elif strategy_type == "cross_below":

            cross_points = g_closed.index[
            g_closed["cross_down"]
            ]

        # -----------------------------------
        # EMA Cross scanners only
        # -----------------------------------

        if strategy_type in ["cross_above", "cross_below"]:

            # No cross ever happened
            if len(cross_points) == 0:
                continue

            last_cross_index = cross_points[-1]

            bars_since_cross = (
                len(g_closed)
                - g_closed.index.get_loc(last_cross_index)
                - 1
            )

        # ---------------------------------------------------
        # Big Move SCANNER
        # ---------------------------------------------------

        if strategy_type == "big_move":

            # 'signal' already comes from the iterator.
            # No database lookup required here.
            pass

            # # -----------------------------
            # # LOOKBACK FILTER
            # # -----------------------------

            # latest_signal_ts = pd.Timestamp(signal.timestamp)

            # if latest_signal_ts.tzinfo is None:
            #     latest_signal_ts = latest_signal_ts.tz_localize("UTC")

            # # -----------------------------------
            # # Find nearest candle timestamp
            # # -----------------------------------

            # nearest_idx = g.index.get_indexer(
            #     [latest_signal_ts],
            #     method="nearest"
            # )[0]

            # if nearest_idx == -1:
            #     continue

            # bars_since_signal = len(g) - nearest_idx - 1

            

            # -----------------------------
            # F&O FILTER
            # -----------------------------

            symbol_obj = db.query(Symbol).filter_by(
                symbol=symbol
            ).first()

            if universe == "fno":

                if not symbol_obj or not symbol_obj.is_fno:
                    continue

        

            # -----------------------------
            # SIGNAL CANDLE CONTEXT
            # -----------------------------

            signal_ts = pd.Timestamp(signal.timestamp)

            # Make timezone compatible with candle index
            if g.index.tz is not None:

                if signal_ts.tzinfo is None:
                    signal_ts = signal_ts.tz_localize(g.index.tz)
                else:
                    signal_ts = signal_ts.tz_convert(g.index.tz)

            else:

                if signal_ts.tzinfo is not None:
                    signal_ts = signal_ts.tz_localize(None)

            # -----------------------------
            # Display timestamp in IST
            # -----------------------------

            signal_time = pd.Timestamp(signal.timestamp)

            if signal_time.tzinfo is None:
                signal_time = signal_time.tz_localize("UTC")

            signal_time_ist = (
                signal_time
                .tz_convert("Asia/Kolkata")
                .strftime("%d-%b-%Y %I:%M %p")
            )

            signal_idx = g.index.get_indexer(
                [signal_ts],
                method="nearest"
            )[0]

            if signal_idx == -1:
                continue

            signal_candle = g.iloc[signal_idx]

            latest = g.iloc[-1]

            bars_since_signal = len(g) - signal_idx - 1

            if latest["EMA5"] > latest["EMA20"]:
                trend = "Bullish"

            elif latest["EMA5"] < latest["EMA20"]:
                trend = "Bearish"

            else:
                trend = "Neutral"

            
            

            # -----------------------------
            # EMA FILTER
            # -----------------------------

            if ema_filter == "cross_above":

                if not latest["EMA5"] > latest["EMA20"]:
                    continue

            elif ema_filter == "cross_below":

                if not latest["EMA5"] < latest["EMA20"]:
                    continue

            # -----------------------------
            # SUPER TREND
            # -----------------------------

            g = compute_supertrend(g)

            latest = g.iloc[-1]

            # -----------------------------
            # Bars Since Signal
            # -----------------------------

            # latest_ts = g.index[-1]
            # signal_ts = pd.Timestamp(signal.timestamp)

            # Make both timestamps timezone compatible
            """ if latest_ts.tzinfo is not None:
                if signal_ts.tzinfo is None:
                    signal_ts = signal_ts.tz_localize(latest_ts.tzinfo)
                else:
                    signal_ts = signal_ts.tz_convert(latest_ts.tzinfo)
            else:
                if signal_ts.tzinfo is not None:
                    signal_ts = signal_ts.tz_localize(None)

            delta = latest_ts - signal_ts

            if timeframe == "2h":
                bars_since_signal = max(
                    0,
                    int(delta.total_seconds() // (2 * 3600))
                )

            elif timeframe == "daily":
                bars_since_signal = max(0, delta.days)

            elif timeframe == "weekly":
                bars_since_signal = max(0, delta.days // 7)

            elif timeframe == "monthly":
                bars_since_signal = max(0, delta.days // 30)

            else:
                bars_since_signal = None
 """
           

            results.append({
                "symbol": symbol,
                "price": round(float(latest["Close"]), 2),
                "ema5": round(float(latest["EMA5"]), 2),
                "ema20": round(float(latest["EMA20"]), 2),
                "ema55": round(float(latest["EMA55"]), 2),
                "ema80": round(float(latest["EMA80"]), 2),
                "ema200": round(float(latest["EMA200"]), 2),

                "trend": trend,

                "st_direction": latest["ST_Direction"],
                "st_value": (
                    round(float(latest["Supertrend"]), 2)
                    if not np.isnan(latest["Supertrend"])
                    else None
                ),
                "bars_since_signal": signal_time_ist
            })

            continue

        # Apply lookback filter
        if bars_since_cross > lookback:
            continue

        # Supertrend
        g = compute_supertrend(g)

        latest = g.iloc[-1]

        # -----------------------------
        # Bars Since Signal
        # -----------------------------

        # latest_ts = pd.Timestamp(g.index[-1])
        # signal_ts = pd.Timestamp(signal.timestamp)

        # Make timestamps comparable
        """ if latest_ts.tzinfo is not None and signal_ts.tzinfo is None:
            signal_ts = signal_ts.tz_localize("UTC")

        elif latest_ts.tzinfo is None and signal_ts.tzinfo is not None:
            signal_ts = signal_ts.tz_localize(None)

        delta = latest_ts - signal_ts

        if timeframe == "2h":
            bars_since_signal = max(0, int(delta.total_seconds() // (2 * 3600)))

        elif timeframe == "daily":
            bars_since_signal = max(0, delta.days)

        elif timeframe == "weekly":
            bars_since_signal = max(0, delta.days // 7)

        elif timeframe == "monthly":
            bars_since_signal = max(0, delta.days // 30)

        else:
            bars_since_signal = None
 """ 
        

        results.append({
            "symbol": symbol,
            "price": round(float(latest["Close"]), 2),
            "ema5": round(float(latest["EMA5"]), 2),
            "ema20": round(float(latest["EMA20"]), 2),
            "ema55": round(float(latest["EMA55"]), 2),
            "ema80": round(float(latest["EMA80"]), 2),
            "ema200": round(float(latest["EMA200"]), 2),
            "st_direction": latest["ST_Direction"],
            "st_value": (round(float(latest["Supertrend"]), 2)
                if not np.isnan(latest["Supertrend"])
                else None
            ),
            "bars_since_cross": bars_since_cross
        })

    print("PROCESSED:", processed)
    print("FINAL RESULTS:", len(results))
    # Sort by most recent cross first
    # results = sorted(results, key=lambda x: x["bars_since_cross"])
    
    if strategy_type == "big_move":
        results = sorted(
            results,
            key=lambda x: x["symbol"]
        )

    else:
        results = sorted(
            results,
            key=lambda x: x["bars_since_cross"]
        )

    return results