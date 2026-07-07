from datetime import datetime, timedelta, timezone

import pandas as pd

from database import SessionLocal
from models import Signal, MarketCandle, Symbol


# ---------------------------------------------------
# Helper: calculate EMA exactly like existing scanners
# ---------------------------------------------------

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


# ---------------------------------------------------
# Main Scanner
# ---------------------------------------------------

def get_inverse_hammer_signals(
    timeframe="2h",
    lookback="today",
    fno_only=False,
    ema_filter=None,          # None / crossover / crossdown
    use_live_candle=False
):

    db = SessionLocal()

    try:

        now = datetime.now(timezone.utc)

        # ---------------------------------------------------
        # Time filters
        # ---------------------------------------------------

        if lookback == "today":

            start_time = datetime.combine(
                now.date(),
                datetime.min.time(),
                tzinfo=timezone.utc
            )

            end_time = now

        elif lookback == "yesterday":

            yesterday = now.date() - timedelta(days=1)

            start_time = datetime.combine(
                yesterday,
                datetime.min.time(),
                tzinfo=timezone.utc
            )

            end_time = datetime.combine(
                now.date(),
                datetime.min.time(),
                tzinfo=timezone.utc
            )

        elif lookback == "week":

            start_time = now - timedelta(days=7)
            end_time = now

        else:

            start_time = now - timedelta(days=1)
            end_time = now

        # ---------------------------------------------------
        # Fetch signals
        # ---------------------------------------------------

        signals = db.query(Signal).filter(
            Signal.signal_type == "inverse_hammer",
            Signal.timeframe == timeframe,
            Signal.timestamp >= start_time,
            Signal.timestamp < end_time
        ).order_by(Signal.timestamp.desc()).all()

        results = []

        # ---------------------------------------------------
        # Process each signal
        # ---------------------------------------------------

        for s in signals:

            # ---------------------------------------------------
            # Validate symbol
            # ---------------------------------------------------

            symbol_obj = db.query(Symbol).filter_by(
                symbol=s.symbol
            ).first()

            if not symbol_obj:
                continue

            # ---------------------------------------------------
            # F&O filter
            # ---------------------------------------------------

            if fno_only and not symbol_obj.is_fno:
                continue

            # ---------------------------------------------------
            # Fetch historical candles
            # ---------------------------------------------------

            candles = db.query(MarketCandle).filter(
                MarketCandle.symbol == s.symbol,
                MarketCandle.timeframe == timeframe,
                MarketCandle.timestamp <= s.timestamp
            ).order_by(MarketCandle.timestamp.asc()).all()

            if len(candles) < 25:
                continue

            # ---------------------------------------------------
            # Convert to dataframe
            # ---------------------------------------------------

            df = pd.DataFrame([
                {
                    "timestamp": c.timestamp,
                    "close": c.close
                }
                for c in candles
            ])

            # ---------------------------------------------------
            # EMA calculation
            # ---------------------------------------------------

            df["ema5"] = calculate_ema(df["close"], 5)
            df["ema20"] = calculate_ema(df["close"], 20)

            latest = df.iloc[-1]

            ema5 = float(latest["ema5"])
            ema20 = float(latest["ema20"])

            # ---------------------------------------------------
            # Trend determination
            # ---------------------------------------------------

            if ema5 > ema20:

                trend = "uptrend"

                # Inverse hammer at top
                bias = "bearish_reversal"

            elif ema5 < ema20:

                trend = "downtrend"

                # Inverse hammer at bottom
                bias = "bullish_reversal"

            else:

                trend = "sideways"
                bias = "neutral"

            # ---------------------------------------------------
            # EMA FILTERS
            # ---------------------------------------------------

            if ema_filter == "crossover":

                if ema5 <= ema20:
                    continue

            elif ema_filter == "crossdown":

                if ema5 >= ema20:
                    continue

            # ---------------------------------------------------
            # Build response
            # ---------------------------------------------------

            results.append({
                "symbol": s.symbol,
                "timestamp": s.timestamp,
                "timeframe": s.timeframe,
                "signal": s.signal_type,
                "trend": trend,
                "bias": bias,
                "ema5": round(ema5, 2),
                "ema20": round(ema20, 2),
                "is_fno": symbol_obj.is_fno
            })

        return results

    finally:
        db.close()