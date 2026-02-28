import time
import yfinance as yf
from data_fetcher import fetch_market_data

# ---------------------------
# Cache Config
# ---------------------------
CACHE = {}
CACHE_TTL = 300  # seconds


# ---------------------------
# SAFE INDEX FETCHER
# ---------------------------
def get_index_data(symbol):
    try:
        df = yf.Ticker(symbol).history(period="2d")

        if df.empty or len(df) < 2:
            print(f"Index fetch failed for {symbol}")
            return {
                "close": None,
                "change": None,
                "pct_change": None
            }

        today = df["Close"].iloc[-1]
        yesterday = df["Close"].iloc[-2]

        change = today - yesterday
        pct = (change / yesterday) * 100

        return {
            "close": round(float(today), 2),
            "change": round(float(change), 2),
            "pct_change": round(float(pct), 2)
        }

    except Exception as e:
        print(f"Index error {symbol}:", e)
        return {
            "close": None,
            "change": None,
            "pct_change": None
        }


def calculate_breadth(ema5_filter=None, ema20_filter=None):

    cache_key = f"{ema5_filter}_{ema20_filter}"

    # ---------------------------
    # Cache Check
    # ---------------------------
    if cache_key in CACHE:
        cached_time, cached_data = CACHE[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_data

    batches = fetch_market_data(60)

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

    # ---------------------------
    # Main Loop
    # ---------------------------
    for data in batches:

        if data.empty:
            continue

        for symbol in data.columns.levels[0]:

            try:
                df = data[symbol].dropna()

                if len(df) < 40:
                    continue

                today = df["Close"].iloc[-1]
                yesterday = df["Close"].iloc[-2]

                change = today - yesterday
                daily_pct = (change / yesterday) * 100

                ema5 = df["Close"].ewm(span=5).mean().iloc[-1]
                ema20 = df["Close"].ewm(span=20).mean().iloc[-1]

                stock_data = {
                    "symbol": symbol,
                    "price": round(float(today), 2),
                    "change": round(float(change), 2),
                    "pct": round(float(daily_pct), 2),
                    "above_ema5": today > ema5,
                    "above_ema20": today > ema20
                }

                # ---------------------------
                # EMA FILTERS
                # ---------------------------
                if ema5_filter == "above" and not stock_data["above_ema5"]:
                    continue

                if ema5_filter == "below" and stock_data["above_ema5"]:
                    continue

                if ema20_filter == "above" and not stock_data["above_ema20"]:
                    continue

                if ema20_filter == "below" and stock_data["above_ema20"]:
                    continue

                # ---------------------------
                # CLASSIFICATION
                # ---------------------------
                if daily_pct > 0:
                    result["advances"].append(stock_data)
                else:
                    result["declines"].append(stock_data)

                if daily_pct >= 4:
                    result["up_4_percent"].append(stock_data)

                if daily_pct <= -4:
                    result["down_4_percent"].append(stock_data)

                monthly_pct = (
                    (today - df["Close"].iloc[-22])
                    / df["Close"].iloc[-22]
                    * 100
                )

                if monthly_pct >= 20:
                    result["up_20_percent_monthly"].append(stock_data)

                if monthly_pct <= -20:
                    result["down_20_percent_monthly"].append(stock_data)

                dma10 = df["Close"].rolling(10).mean().iloc[-1]
                dma20 = df["Close"].rolling(20).mean().iloc[-1]
                dma40 = df["Close"].rolling(40).mean().iloc[-1]

                if today > dma10:
                    result["above_10_dma"].append(stock_data)

                if today > dma20:
                    result["above_20_dma"].append(stock_data)

                if today > dma40:
                    result["above_40_dma"].append(stock_data)

            except Exception:
                continue

    # ---------------------------
    # SORT & FORMAT
    # ---------------------------
    final = {}

    for category in result:

        stocks = result[category]

        if category in ["declines", "down_4_percent", "down_20_percent_monthly"]:
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"])
        else:
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"], reverse=True)

        final[category] = {
            "count": len(sorted_stocks),
            "stocks": sorted_stocks
        }

    # ---------------------------
    # FETCH INDICES SEPARATELY
    # ---------------------------
    final["indices"] = {
        "nifty": get_index_data("^NSEI"),
        "banknifty": get_index_data("^NSEBANK")
    }

    # ---------------------------
    # CACHE
    # ---------------------------
    CACHE[cache_key] = (time.time(), final)

    return final