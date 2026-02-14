import time
from data_fetcher import fetch_market_data

CACHE = None
LAST_UPDATED = 0
CACHE_TTL = 300  # 5 minutes


def calculate_breadth():
    global CACHE, LAST_UPDATED

    # Return cached version if fresh
    if CACHE is not None and (time.time() - LAST_UPDATED < CACHE_TTL):
        print("Returning cached breadth data ✅")
        return CACHE

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

                stock_data = {
                    "symbol": symbol,
                    "price": round(float(today), 2),
                    "change": round(float(change), 2),
                    "pct": round(float(daily_pct), 2)
                }

                # Capture indices
                if symbol == "^NSEI":
                    nifty_data = stock_data
                if symbol == "^NSEBANK":
                    banknifty_data = stock_data

                # Classification
                if daily_pct > 0:
                    result["advances"].append(stock_data)
                else:
                    result["declines"].append(stock_data)

                if daily_pct >= 4:
                    result["up_4_percent"].append(stock_data)

                if daily_pct <= -4:
                    result["down_4_percent"].append(stock_data)

                monthly_pct = (
                    (today - df["Close"].iloc[-22]) /
                    df["Close"].iloc[-22] * 100
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

            except:
                continue

    final = {}

    for key, stocks in result.items():
        final[key] = {
            "count": len(stocks),
            "stocks": sorted(stocks, key=lambda x: x["symbol"])
        }

    final["indices"] = {
        "nifty": nifty_data,
        "banknifty": banknifty_data
    }

    CACHE = final
    LAST_UPDATED = time.time()

    return final
