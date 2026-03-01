import time
import pandas as pd
from database import SessionLocal
from models import MarketCandle
from candle_service import get_candles
from symbol_service import get_all_symbols


# ---------------------------
# Cache Config
# ---------------------------
CACHE = {}
CACHE_TTL = 300  # seconds



def calculate_breadth(ema5_filter=None, ema20_filter=None):

    cache_key = f"{ema5_filter}_{ema20_filter}"

    if cache_key in CACHE:
        cached_time, cached_data = CACHE[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_data

    print("Calculating breadth (optimized) 🔄")

    db = SessionLocal()

    try:
        rows = (
            db.query(MarketCandle)
            .filter(MarketCandle.timeframe == "1d")
            .order_by(MarketCandle.symbol, MarketCandle.timestamp)
            .all()
        )

    finally:
        db.close()

    if not rows:
        return {}

    # Build DataFrame once
    df = pd.DataFrame([{
        "symbol": r.symbol,
        "timestamp": r.timestamp,
        "Open": r.open,
        "High": r.high,
        "Low": r.low,
        "Close": r.close,
        "Volume": r.volume
    } for r in rows])

    result = {
        "advances": [],
        "declines": [],
        "up_4_percent": [],
        "down_4_percent": [],
        "up_20_percent_monthly": [],
        "down_20_percent_monthly": [],
        "above_10_dma": [],
        "above_20_dma": [],
        "above_40_dma": []
    }

    nifty_data = {"close": 0, "change": 0, "pct_change": 0}
    banknifty_data = {"close": 0, "change": 0, "pct_change": 0}

    grouped = df.groupby("symbol")

    for symbol, g in grouped:

        g = g.sort_values("timestamp")

        if len(g) < 40:
            continue

        today = g["Close"].iloc[-1]
        yesterday = g["Close"].iloc[-2]

        change = today - yesterday
        daily_pct = (change / yesterday) * 100

        ema5 = g["Close"].ewm(span=5).mean().iloc[-1]
        ema20 = g["Close"].ewm(span=20).mean().iloc[-1]

        stock_data = {
            "symbol": symbol,
            "price": round(float(today), 2),
            "change": round(float(change), 2),
            "pct": round(float(daily_pct), 2),
            "above_ema5": today > ema5,
            "above_ema20": today > ema20
        }

        if symbol == "^NSEI":
            nifty_data = {
                "close": stock_data["price"],
                "change": stock_data["change"],
                "pct_change": stock_data["pct"]
            }
            continue

        if symbol == "^NSEBANK":
            banknifty_data = {
                "close": stock_data["price"],
                "change": stock_data["change"],
                "pct_change": stock_data["pct"]
            }
            continue

        if ema5_filter == "above" and not stock_data["above_ema5"]:
            continue

        if ema5_filter == "below" and stock_data["above_ema5"]:
            continue

        if ema20_filter == "above" and not stock_data["above_ema20"]:
            continue

        if ema20_filter == "below" and stock_data["above_ema20"]:
            continue

        if daily_pct > 0:
            result["advances"].append(stock_data)
        else:
            result["declines"].append(stock_data)

        if daily_pct >= 4:
            result["up_4_percent"].append(stock_data)

        if daily_pct <= -4:
            result["down_4_percent"].append(stock_data)

        monthly_pct = (
            (today - g["Close"].iloc[-22])
            / g["Close"].iloc[-22]
            * 100
        )

        if monthly_pct >= 20:
            result["up_20_percent_monthly"].append(stock_data)

        if monthly_pct <= -20:
            result["down_20_percent_monthly"].append(stock_data)

        dma10 = g["Close"].rolling(10).mean().iloc[-1]
        dma20 = g["Close"].rolling(20).mean().iloc[-1]
        dma40 = g["Close"].rolling(40).mean().iloc[-1]

        if today > dma10:
            result["above_10_dma"].append(stock_data)

        if today > dma20:
            result["above_20_dma"].append(stock_data)

        if today > dma40:
            result["above_40_dma"].append(stock_data)

    final = {}

    for category, stocks in result.items():
        if category in ["declines", "down_4_percent", "down_20_percent_monthly"]:
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"])
        else:
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"], reverse=True)

        final[category] = {
            "count": len(sorted_stocks),
            "stocks": sorted_stocks
        }

    final["indices"] = {
        "nifty": nifty_data,
        "banknifty": banknifty_data
    }

    CACHE[cache_key] = (time.time(), final)

    return final