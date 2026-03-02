import time
import pandas as pd
from database import SessionLocal
from models import MarketCandle
from universe import load_nifty50_universe, load_banknifty_universe
from models import SymbolMetadata

# ---------------------------
# Cache Config
# ---------------------------
CACHE = {}
CACHE_TTL = 300  # seconds
CACHE.clear()

print("USING NEW CALCULATE_BREADTH FUNCTION 🚀")
def calculate_breadth(ema5_filter=None, ema20_filter=None):

    cache_key = f"{ema5_filter}_{ema20_filter}"

    ##if cache_key in CACHE:
        ##cached_time, cached_data = CACHE[cache_key]
        ##if time.time() - cached_time < CACHE_TTL:
            ##return cached_data

    print("Calculating breadth (optimized) 🔄")

    db = SessionLocal()

    try:
        rows = (
            db.query(MarketCandle)
            .filter(MarketCandle.timeframe == "1d")
            .order_by(MarketCandle.symbol, MarketCandle.timestamp)
            .all()
        )

        metadata_rows = db.query(SymbolMetadata).all()
        sector_map = {m.symbol: m.sector for m in metadata_rows}

    finally:
        db.close()

    if not rows:
        return {}

    nifty50_symbols = load_nifty50_universe()
    banknifty_symbols = load_banknifty_universe()

    df = pd.DataFrame([{
        "symbol": r.symbol,
        "timestamp": r.timestamp,
        "Open": r.open,
        "High": r.high,
        "Low": r.low,
        "Close": r.close,
        "Volume": r.volume
    } for r in rows])

    latest_timestamp = max(r.timestamp for r in rows)

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

    sector_result = {}

    nifty_data = {"close": 0, "change": 0, "pct_change": 0}
    banknifty_data = {"close": 0, "change": 0, "pct_change": 0}

    nifty_adv, nifty_dec = [], []
    bank_adv, bank_dec = [], []

    grouped = df.groupby("symbol")

    total_universe = len([
        s for s in grouped.groups.keys()
        if s not in ["^NSEI", "^NSEBANK"]
    ])

    for symbol, g in grouped:

        g = g.sort_values("timestamp")

        # Minimum requirement: at least 2 candles for daily change
        if len(g) < 2:
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

        # -----------------------
        # Index Handling
        # -----------------------
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

        # -----------------------
        # EMA Filters
        # -----------------------
        if ema5_filter == "above" and not stock_data["above_ema5"]:
            continue
        if ema5_filter == "below" and stock_data["above_ema5"]:
            continue
        if ema20_filter == "above" and not stock_data["above_ema20"]:
            continue
        if ema20_filter == "below" and stock_data["above_ema20"]:
            continue

        # -----------------------
        # Advance / Decline
        # -----------------------
        if daily_pct > 0:
            result["advances"].append(stock_data)
        else:
            result["declines"].append(stock_data)

        # -----------------------
        # Sector Breadth
        # -----------------------
        sector = sector_map.get(symbol)

        if sector:
            if sector not in sector_result:
                sector_result[sector] = {"adv": [], "dec": []}

            if daily_pct > 0:
                sector_result[sector]["adv"].append(stock_data)
            else:
                sector_result[sector]["dec"].append(stock_data)

        # -----------------------
        # Nifty / Bank Breadth
        # -----------------------
        if symbol in nifty50_symbols:
            (nifty_adv if daily_pct > 0 else nifty_dec).append(stock_data)

        if symbol in banknifty_symbols:
            (bank_adv if daily_pct > 0 else bank_dec).append(stock_data)

        # -----------------------
        # ±4%
        # -----------------------
        if daily_pct >= 4:
            result["up_4_percent"].append(stock_data)
        if daily_pct <= -4:
            result["down_4_percent"].append(stock_data)

        # -----------------------
        # Monthly ±20% (needs 22 candles)
        # -----------------------
        if len(g) >= 22:
            monthly_pct = (
                (today - g["Close"].iloc[-22])
                / g["Close"].iloc[-22]
                * 100
            )

            if monthly_pct >= 20:
                result["up_20_percent_monthly"].append(stock_data)
            if monthly_pct <= -20:
                result["down_20_percent_monthly"].append(stock_data)

        # -----------------------
        # DMA checks (guarded)
        # -----------------------
        if len(g) >= 10:
            dma10 = g["Close"].rolling(10).mean().iloc[-1]
            if today > dma10:
                result["above_10_dma"].append(stock_data)

        if len(g) >= 20:
            dma20 = g["Close"].rolling(20).mean().iloc[-1]
            if today > dma20:
                result["above_20_dma"].append(stock_data)

        if len(g) >= 40:
            dma40 = g["Close"].rolling(40).mean().iloc[-1]
            if today > dma40:
                result["above_40_dma"].append(stock_data)

    # -----------------------------------
    # Final Structuring
    # -----------------------------------
    final = {}

    for category, stocks in result.items():

        if category in ["declines", "down_4_percent", "down_20_percent_monthly"]:
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"])
        else:
            sorted_stocks = sorted(stocks, key=lambda x: x["pct"], reverse=True)

        count = len(sorted_stocks)
        pct_of_universe = round((count / total_universe) * 100, 1) if total_universe else 0

        final[category] = {
            "count": count,
            "percent": pct_of_universe,
            "stocks": sorted_stocks
        }

    # -----------------------------------
    # Sector Final Structuring
    # -----------------------------------
    final["sectors"] = []

    for sector, values in sector_result.items():

        total = len(values["adv"]) + len(values["dec"])
        if total == 0:
            continue

        adv_count = len(values["adv"])
        dec_count = len(values["dec"])

        final["sectors"].append({
            "sector": sector,
            "adv": adv_count,
            "dec": dec_count,
            "adv_pct": round((adv_count / total) * 100, 1),
            "dec_pct": round((dec_count / total) * 100, 1),
            "adv_stocks": sorted(values["adv"], key=lambda x: x["pct"], reverse=True),
            "dec_stocks": sorted(values["dec"], key=lambda x: x["pct"])
        })

    final["sectors"] = sorted(final["sectors"], key=lambda x: x["adv_pct"], reverse=True)

    # -----------------------------------
    # Index Summary
    # -----------------------------------
    final["indices"] = {
        "nifty": nifty_data,
        "banknifty": banknifty_data
    }

    final["nifty_breadth"] = {
        "adv": len(nifty_adv),
        "dec": len(nifty_dec),
        "adv_pct": round(len(nifty_adv) / len(nifty50_symbols) * 100, 1),
        "dec_pct": round(len(nifty_dec) / len(nifty50_symbols) * 100, 1),
        "adv_stocks": sorted(nifty_adv, key=lambda x: x["pct"], reverse=True),
        "dec_stocks": sorted(nifty_dec, key=lambda x: x["pct"])
    }

    final["banknifty_breadth"] = {
        "adv": len(bank_adv),
        "dec": len(bank_dec),
        "adv_pct": round(len(bank_adv) / len(banknifty_symbols) * 100, 1),
        "dec_pct": round(len(bank_dec) / len(banknifty_symbols) * 100, 1),
        "adv_stocks": sorted(bank_adv, key=lambda x: x["pct"], reverse=True),
        "dec_stocks": sorted(bank_dec, key=lambda x: x["pct"])
    }

    final["last_updated"] = latest_timestamp.strftime("%d %b %Y %H:%M")

    ##CACHE[cache_key] = (time.time(), final)

    return final