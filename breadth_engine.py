import time
from data_fetcher import fetch_market_data

# Cache variables
CACHE = {}
CACHE_TTL = 300  # 5 minutes


def calculate_breadth(ema5_filter=None, ema20_filter=None):

    cache_key = f"{ema5_filter}_{ema20_filter}"

    # Return cached result if valid
    if cache_key in CACHE:
        cached_time, cached_data = CACHE[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            print("Returning cached breadth data ✅")
            return cached_data

    print("Calculating fresh breadth data 🔄")

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

    nifty_data = None
    banknifty_data = None

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

                # Calculate EMAs
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

                # Apply EMA Filters (AND logic)
                if ema5_filter == "above" and not stock_data["above_ema5"]:
                    continue
                if ema5_filter == "below" and stock_data["above_ema5"]:
                    continue

                if ema20_filter == "above" and not stock_data["above_ema20"]:
                    continue
                if ema20_filter == "below" and stock_data["above_ema20"]:
                    continue

                # Capture indices
                if symbol == "^NSEI":
                    nifty_data = stock_data
                if symbol == "^NSEBANK":
                    banknifty_data = stock_data

                # Daily classification
                if daily_pct > 0:
                    result["advances"].append(stock_data)
                else:
                    result["declines"].append(stock_data)

                if daily_pct >= 4:
                    result["up_4_percent"].append(stock_data)

                if daily_pct <= -4:
                    result["down_4_percent"].append(stock_data)

                # Monthly calculation
                monthly_pct = (
                    (today - df["Close"].iloc[-22]) /
                    df["Close"].iloc[-22] * 100
                )

                if monthly_pct >= 20:
                    result["up_20_percent_monthly"].append(stock_data)

                if monthly_pct <= -20:
                    result["down_20_percent_monthly"].append(stock_data)

                # DMA calculations
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

    final = {}

    # Sorting logic
    for key, stocks in result.items():

        if key in ["declines", "down_4_percent", "down_20_percent_monthly"]:
            # Most negative first
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"])
        else:
            # Highest positive first
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"], reverse=True)

        final[key] = {
            "count": len(sorted_stocks),
            "stocks": sorted_stocks
        }

    # Indices block
    final["indices"] = {
        "nifty": nifty_data,
        "banknifty": banknifty_data
    }

    # Save in cache
    CACHE[cache_key] = (time.time(), final)

    return final
