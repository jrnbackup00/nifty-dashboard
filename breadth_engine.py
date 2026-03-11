import time
import pandas as pd

from database import SessionLocal
from models import MarketCandle, SymbolGroupMap
from universe import load_nifty50_universe, load_banknifty_universe
from zoneinfo import ZoneInfo

ROTATION_HISTORY = None

# ---------------------------
# Cache Config
# ---------------------------
CACHE = {}
CACHE_TTL = 300

ROTATION_HISTORY = None

def calculate_breadth(ema5_filter=None, ema20_filter=None):

    cache_key = f"{ema5_filter}_{ema20_filter}"

    if cache_key in CACHE:
        cached_time, cached_data = CACHE[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_data

    db = SessionLocal()

    try:

        rows = (
            db.query(MarketCandle)
            .filter(MarketCandle.timeframe == "1d")
            .order_by(MarketCandle.symbol, MarketCandle.timestamp)
            .all()
        )

        group_rows = db.query(SymbolGroupMap).all()

        group_map = {}

        for row in group_rows:

            symbol = row.symbol.upper()

            if symbol not in group_map:
                group_map[symbol] = []

            group_map[symbol].append(row.group_name)

    finally:
        db.close()

    if not rows:
        return {}

    nifty50_symbols = load_nifty50_universe()
    banknifty_symbols = load_banknifty_universe()

    df = pd.DataFrame([{
        "symbol": r.symbol,
        "timestamp": r.timestamp,
        "Close": r.close
    } for r in rows])

    # -----------------------------------
    # Precompute technical indicators (vectorized)
    # -----------------------------------

    df = df.sort_values(["symbol", "timestamp"])

    df["EMA5"] = df.groupby("symbol")["Close"].transform(lambda x: x.ewm(span=5).mean())
    df["EMA20"] = df.groupby("symbol")["Close"].transform(lambda x: x.ewm(span=20).mean())

    df["DMA10"] = df.groupby("symbol")["Close"].transform(lambda x: x.rolling(10).mean())
    df["DMA20"] = df.groupby("symbol")["Close"].transform(lambda x: x.rolling(20).mean())
    df["DMA40"] = df.groupby("symbol")["Close"].transform(lambda x: x.rolling(40).mean())

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

    # -----------------------------------
    # Pre-compute symbol dataframe lookup (performance optimization)
    # -----------------------------------

    symbol_lookup = {
        symbol: g.sort_values("timestamp")
        for symbol, g in grouped
        }

    total_universe = len([
        s for s in grouped.groups.keys()
        if s not in ["^NSEI", "^NSEBANK"]
    ])

    

    for symbol, g in grouped:

        g = g.sort_values("timestamp")

        latest = g.iloc[-1]

        g = g.sort_values("timestamp")

        if len(g) < 2:
            continue

        today = g["Close"].iloc[-1]
        yesterday = g["Close"].iloc[-2]

        change = today - yesterday
        daily_pct = (change / yesterday) * 100

        ema5 = latest["EMA5"]
        ema20 = latest["EMA20"]

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
        # Sector / Theme Breadth
        # -----------------------

        groups = group_map.get(symbol, [])

        for group in groups:

            if group not in sector_result:
                sector_result[group] = {"adv": [], "dec": []}

            if daily_pct > 0:
                sector_result[group]["adv"].append(stock_data)
            else:
                sector_result[group]["dec"].append(stock_data)

        # -----------------------
        # Index Constituents
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
        # Monthly ±20%
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
        # DMA checks
        # -----------------------

        if len(g) >= 10:

            ##dma10 = g["Close"].rolling(10).mean().iloc[-1]
            dma10 = latest["DMA10"]

            if today > dma10:
                result["above_10_dma"].append(stock_data)

        if len(g) >= 20:

            dma20 = latest["DMA20"]


            if today > dma20:
                result["above_20_dma"].append(stock_data)

        if len(g) >= 40:

            dma40 = latest["DMA40"]

            if today > dma40:
                result["above_40_dma"].append(stock_data)

    # -----------------------------------
    # Final Breadth Structuring
    # -----------------------------------

    final = {}

    for category, stocks in result.items():

        sorted_stocks = sorted(
            stocks,
            key=lambda x: x["pct"],
            reverse=(category not in ["declines", "down_4_percent", "down_20_percent_monthly"])
        )

        count = len(sorted_stocks)

        pct_of_universe = round(
            (count / total_universe) * 100,
            1
        ) if total_universe else 0

        final[category] = {
            "count": count,
            "percent": pct_of_universe,
            "stocks": sorted_stocks
        }

    # -----------------------------------
    # Sector Structuring + Momentum
    # -----------------------------------

    final["sectors"] = []

    for sector, values in sector_result.items():

        adv_stocks = values["adv"]
        dec_stocks = values["dec"]

        total = len(adv_stocks) + len(dec_stocks)

        if total == 0:
            continue

        adv_count = len(adv_stocks)
        dec_count = len(dec_stocks)

        adv_pct = round((adv_count / total) * 100, 1)
        dec_pct = round((dec_count / total) * 100, 1)

        momentum_values = []

        for stock in adv_stocks + dec_stocks:

            symbol = stock["symbol"]

            symbol_df = symbol_lookup.get(symbol)

            if symbol_df is None or len(symbol_df) < 6:
                continue

            

            last_6 = symbol_df.tail(6)

            moves = []

            for i in range(1, len(last_6)):

                today_close = last_6["Close"].iloc[i]
                prev_close = last_6["Close"].iloc[i-1]

                pct = (today_close - prev_close) / prev_close * 100

                moves.append(1 if pct > 0 else 0)

            if moves:
                momentum_values.append(sum(moves) / len(moves) * 100)

        avg_breadth = sum(momentum_values) / len(momentum_values) if momentum_values else adv_pct

        momentum = adv_pct - avg_breadth

        # Institutional sector strength score
        strength = (0.7 * adv_pct) + (0.3 * momentum)

        final["sectors"].append({
            "sector": sector,
            "adv": adv_count,
            "dec": dec_count,
            "adv_pct": adv_pct,
            "dec_pct": dec_pct,
            "momentum": round(momentum, 2),
            "strength": round(strength, 1),
            "adv_stocks": sorted(adv_stocks, key=lambda x: x["pct"], reverse=True),
            "dec_stocks": sorted(dec_stocks, key=lambda x: x["pct"])
        })

    final["sectors"] = sorted(final["sectors"], key=lambda x: x["strength"], reverse=True)
    

    # -----------------------------------
    # Sector Rotation
    # -----------------------------------

    rotation = {
        "leading": [],
        "weakening": [],
        "lagging": [],
        "improving": []
    }

    for sector in final["sectors"]:

        breadth = sector["adv_pct"]
        momentum = sector["momentum"]

        if breadth >= 50 and momentum >= 0:
            rotation["leading"].append(sector)

        elif breadth >= 50 and momentum < 0:
            rotation["weakening"].append(sector)

        elif breadth < 50 and momentum < 0:
            rotation["lagging"].append(sector)

        else:
            rotation["improving"].append(sector)

    final["sector_rotation"] = rotation

    # -----------------------------------
    # Emerging Sector Detection
    # -----------------------------------

    emerging_sectors = []

    for sector in final["sectors"]:

        score = sector.get("strength", 0)
        momentum = sector.get("momentum", 0)
        breadth = sector.get("adv_pct", 0)

        # Emerging criteria
        if (
            score >= 50 and score < 70 and
            momentum > 0 and
            breadth >= 40
        ):
            emerging_sectors.append({
                "sector": sector["sector"],
                "score": score,
                "momentum": momentum,
                "breadth": breadth
            })

    # Sort by strength descending
    emerging_sectors = sorted(
        emerging_sectors,
        key=lambda x: x["score"],
        reverse=True
    )
    
    final["emerging_sectors"] = emerging_sectors

    # -----------------------------------
    # Sector Acceleration Detector
    # -----------------------------------

    accelerating_sectors = []

    for sector in final["sectors"]:

        score = sector.get("strength", 0)
        momentum = sector.get("momentum", 0)

        # Detect sectors gaining strength rapidly
        if momentum >= 10 and score >= 50:

            accelerating_sectors.append({
                "sector": sector["sector"],
                "score": score,
                "momentum": momentum
            })

    # Sort by momentum descending
    accelerating_sectors = sorted(
        accelerating_sectors,
        key=lambda x: x["momentum"],
        reverse=True
    )

    # Limit to top 5
    accelerating_sectors = accelerating_sectors[:5]

    final["accelerating_sectors"] = accelerating_sectors

    # -----------------------------------
    # Market Regime Detection
    # -----------------------------------

    adv_pct = final["advances"]["percent"]
    dec_pct = final["declines"]["percent"]

    leading_count = len(rotation["leading"])
    lagging_count = len(rotation["lagging"])

    if adv_pct > 60 and leading_count >= 3:
        regime = {
            "state": "Risk ON",
            "color": "success",
            "message": "Broad market participation. Trend friendly environment."
        }

    elif adv_pct < 40 and lagging_count >= 3:
        regime = {
            "state": "Risk OFF",
            "color": "danger",
            "message": "Weak market breadth. Defensive positioning advised."
        }

    elif leading_count >= 2 and lagging_count >= 2:
        regime = {
            "state": "Sector Rotation",
            "color": "warning",
            "message": "Capital rotating between sectors."
        }

    else:
        regime = {
            "state": "Distribution",
            "color": "secondary",
            "message": "Momentum weakening. Watch for breakdowns."
        }

    final["market_regime"] = regime


# -----------------------------------
# Detect Sector Rotation Flow
# -----------------------------------

    global ROTATION_HISTORY

    rotation_flow = []

    if "ROTATION_HISTORY" in globals() and ROTATION_HISTORY:

        previous = ROTATION_HISTORY

        def get_phase(sector_name, rotation_map):

            for phase, sectors in rotation_map.items():
                for s in sectors:
                    if s["sector"] == sector_name:
                        return phase

            return None

        for s in final["sectors"]:

            name = s["sector"]

            prev_phase = get_phase(name, previous)
            new_phase = get_phase(name, rotation)

            if prev_phase and new_phase and prev_phase != new_phase:

                rotation_flow.append({
                    "sector": name,
                    "from": prev_phase,
                    "to": new_phase
                })

    ROTATION_HISTORY = rotation

    final["rotation_flow"] = rotation_flow

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

    ist_time = latest_timestamp.astimezone(ZoneInfo("Asia/Kolkata"))
    final["last_updated"] = ist_time.strftime("%d - %b - %Y %-I%p IST")

    CACHE[cache_key] = (time.time(), final)

    return final