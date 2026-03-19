import yfinance as yf
from datetime import datetime, timezone, timedelta
from sqlalchemy.dialects.postgresql import insert
from database import SessionLocal
from models import MarketCandle
from universe import load_stock_universe
from database import SessionLocal
from ingestion_logs import log_ingestion
from market_calendar import is_market_day
from telegram_alert import send_telegram_alert
import pytz
import pandas as pd
from zoneinfo import ZoneInfo
from telegram_alert import send_telegram_alert
from datetime import time, datetime


# ----------------------------
# CONFIG
# ----------------------------

STOCK_SYMBOLS = load_stock_universe()
INDEX_SYMBOLS = ["^NSEI", "^NSEBANK"]

DAILY_SYMBOLS = STOCK_SYMBOLS + INDEX_SYMBOLS
INTRADAY_SYMBOLS = INDEX_SYMBOLS  # only indices get 2h

TIMEFRAMES = {
    "1d": {"interval": "1d", "period": "7d"},
    "2h": {"interval": "60m", "period": "7d"},
}




""" def build_2h_candles(df):

    if df.empty:
        return []

    df = df.copy()
    df.index = df.index.tz_convert("Asia/Kolkata")

    df = df.between_time("09:15", "15:30")
    df["date"] = df.index.date

    candles = []

    for date, g in df.groupby("date"):

        g = g.sort_index()

        # -------------------------
        # NSE FIXED WINDOWS
        # -------------------------
        windows = [
            (time(9,15), time(11,15), "2h"),
            (time(11,15), time(13,15), "2h"),
            (time(13,15), time(15,15), "2h"),
            (time(15,15), time(15,30), "15m"),
        ]

        for start, end, tf in windows:

            chunk = g[
                (g.index.time >= start) &
                (g.index.time <= end)
            ]

            # Need at least 2 datapoints to form candle
            if len(chunk) < 2:
                continue

            ts = datetime.combine(date, end)
            ts = ts.replace(tzinfo=ZoneInfo("Asia/Kolkata")).astimezone(ZoneInfo("UTC"))

            candles.append({
                "timeframe": tf,
                "timestamp": ts,
                "open": chunk.iloc[0]["Open"],
                "high": chunk["High"].max(),
                "low": chunk["Low"].min(),
                "close": chunk.iloc[-1]["Close"],
                "volume": chunk["Volume"].sum(),
            })

    return candles
 """


def build_2h_candles(df):

    if df.empty:
        return []

    df = df.copy()
    df.index = df.index.tz_convert("Asia/Kolkata")

    # Keep only NSE trading hours
    df = df.between_time("09:15", "15:30")
    df["date"] = df.index.date

    candles = []

    for date, g in df.groupby("date"):

        g = g.sort_index()

        # -------------------------
        # NSE FIXED WINDOWS
        # -------------------------
        windows = [
            (time(9,15),  time(11,15)),
            (time(11,15), time(13,15)),
            (time(13,15), time(15,15)),
            (time(15,15), time(15,30)),  # 🔥 include as 2H continuation
        ]

        for start, end in windows:

            # ✅ Strict boundary: [start, end)
            if end == time(15,30):
                # last candle inclusive
                chunk = g[
                    (g.index.time >= start) &
                    (g.index.time <= end)
                ]
            else:
                chunk = g[
                    (g.index.time >= start) &
                    (g.index.time < end)
                ]

            if chunk.empty:
                continue

            ts = datetime.combine(date, end)
            ts = ts.replace(tzinfo=ZoneInfo("Asia/Kolkata")).astimezone(ZoneInfo("UTC"))

            candles.append({
                "timeframe": "2h",   # ✅ ALWAYS 2h
                "timestamp": ts,
                "open": float(chunk.iloc[0]["Open"]),
                "high": float(chunk["High"].max()),
                "low": float(chunk["Low"].min()),
                "close": float(chunk.iloc[-1]["Close"]),
                "volume": int(chunk["Volume"].sum()),
            })

    return candles

# ----------------------------
# CORE SAVE FUNCTION
# ----------------------------

def save_candles(symbol, timeframe, interval, period):

    df = yf.download(
    symbol,
    interval=interval,
    period=period,
    auto_adjust=True,   
    threads=True        
    )

    if df.empty:
        print(f"No data for {symbol} {timeframe}")
        return

    if hasattr(df.columns, "levels"):
        df.columns = df.columns.get_level_values(0)

    # -----------------------------------
    # STEP 1: Remove duplicate timestamps from Yahoo
    # -----------------------------------
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    db = SessionLocal()

    try:
        today_utc = datetime.now(timezone.utc).date()

        # -----------------------------------
        # STEP 2: BUILD RECORDS SAFELY
        # -----------------------------------

        records_map = {}

        # ----------------------------
        # 2H FLOW
        # ----------------------------
        if timeframe == "2h":

            candles = build_2h_candles(df)

            for c in candles:
                ts = c["timestamp"]

                key = (symbol, c["timeframe"], ts)

                records_map[key] = {
                    "symbol": symbol,
                    "timeframe": c["timeframe"],
                    "timestamp": ts,
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": int(c["volume"]),
                }

        # ----------------------------
        # DAILY FLOW
        # ----------------------------
        else:

            for timestamp, row in df.iterrows():
                ts = timestamp.to_pydatetime().astimezone(timezone.utc)

                key = (symbol, timeframe, ts)

                records_map[key] = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": ts,
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                }

        records = list(records_map.values())

        if not records:
            return

        # -----------------------------------
        # STEP 3: INSERT / UPSERT
        # -----------------------------------

        stmt = insert(MarketCandle).values(records)

        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "timeframe", "timestamp"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
            },
            where=MarketCandle.timestamp >= datetime.combine(
                today_utc,
                datetime.min.time(),
                tzinfo=timezone.utc
            )
        )

        result = db.execute(stmt)
        db.commit()

        print(f"{symbol} {timeframe} → Upserted {result.rowcount}")

    finally:
        db.close()

# ----------------------------
# INCREMENTAL INGESTION
# ----------------------------

def run_incremental_ingestion():

    print("Starting Daily ingestion (Nifty 500 + Indices)")

    for symbol in DAILY_SYMBOLS:
        save_candles(
            symbol,
            "1d",
            TIMEFRAMES["1d"]["interval"],
            TIMEFRAMES["1d"]["period"]
        )

    print("Starting 2h ingestion (Indices only)")

    for symbol in INTRADAY_SYMBOLS:
        save_candles(
            symbol,
            "2h",
            TIMEFRAMES["2h"]["interval"],
            TIMEFRAMES["2h"]["period"]
        )

    print("Incremental ingestion complete ✅")


# ----------------------------
# CLI ENTRY
# ----------------------------

if __name__ == "__main__":
    run_incremental_ingestion()

# -----------------------------------
# Scheduler Job — Intraday 2H Ingestion
# -----------------------------------
# -----------------------------------
# Helper Functions for Scheduler
# -----------------------------------

def ingest_2h_candles():

    count = 0

    for symbol in INTRADAY_SYMBOLS:
        save_candles(
            symbol,
            "2h",
            TIMEFRAMES["2h"]["interval"],
            TIMEFRAMES["2h"]["period"]
        )
        count += 1

    return count


def ingest_daily_candles():

    count = 0

    for symbol in DAILY_SYMBOLS:
        save_candles(
            symbol,
            "1d",
            TIMEFRAMES["1d"]["interval"],
            TIMEFRAMES["1d"]["period"]
        )
        count += 1

    return count


def reingest_day(target_date):

    print(f"Repairing candles for {target_date}")

    ingest_daily_candles()
    ingest_2h_candles()


# -----------------------------------
# Scheduler Job — Intraday 2H Ingestion
# -----------------------------------

def run_intraday_ingestion():

    if not is_market_day():
        print("Skipping ingestion — market closed")
        return

    try:

        rows = ingest_2h_candles()

        log_ingestion(
            job_type="intraday_2h",
            status="SUCCESS",
            rows=rows
        )

        print("Intraday ingestion complete")

    except Exception as e:

        message = f"""
        Nifty Dashboard Alert

        Job: Intraday 2H Ingestion
        Status: FAILED
        Error: {str(e)}
        """

        send_telegram_alert(message)

        log_ingestion(
        job_type="intraday_2h",
        status="FAILED",
        rows=0,
        error=str(e)
        )

        print("Intraday ingestion failed:", e)

# -----------------------------------
# Scheduler Job — Market Close Cycle
# -----------------------------------

def run_market_close_ingestion():

    if not is_market_day():
        print("Skipping close ingestion — market closed")
        return

    try:

        rows_2h = ingest_2h_candles()
        rows_1d = ingest_daily_candles()

        repair_last_days(3)

        total_rows = rows_2h + rows_1d

        # -----------------------------
        # Detect silent ingestion failure
        # -----------------------------

        if total_rows == 0:

            ist_now = datetime.now(timezone.utc).astimezone(
                    ZoneInfo("Asia/Kolkata")
            )

            warning_msg = f"""
                Nifty Dashboard Warning

                Job: Market Close Ingestion
                Status: COMPLETED but NO DATA

                Rows Updated: 0

                Time: {ist_now.strftime("%d %b %Y %I:%M %p IST")}

                Possible causes:
                • Yahoo API returned empty data
                • Network issue
                • Market holiday mismatch
                """

            send_telegram_alert(warning_msg)

        log_ingestion(
            job_type="market_close",
            status="SUCCESS",
            rows=total_rows
        )

        # -----------------------------
        # Telegram SUCCESS Alert
        # -----------------------------

        ist_now = datetime.now(timezone.utc).astimezone(
            ZoneInfo("Asia/Kolkata")
        )

        message = f"""
            ✅ Nifty Dashboard

            Job: Market Close Ingestion
            Status: SUCCESS
            Rows Updated: {total_rows}

            Time: {ist_now.strftime("%d %b %Y %I:%M %p IST")}
            """

        send_telegram_alert(message)

        print("Market close ingestion complete")



    except Exception as e:

        # -----------------------------
        # Telegram FAILURE Alert
        # -----------------------------

        ist_now = datetime.now(timezone.utc).astimezone(
            ZoneInfo("Asia/Kolkata")
        )

        message = f"""
            Nifty Dashboard Alert

            Job: Market Close Ingestion
            Status: FAILED

            Time: {ist_now.strftime("%d %b %Y %I:%M %p IST")}

            Error:
            {str(e)}
            """

        send_telegram_alert(message)

        log_ingestion(
            job_type="market_close",
            status="FAILED",
            rows=0,
            error=str(e)
        )

        print("Market close ingestion failed:", e)

# -----------------------------------
# Repair last N days
# -----------------------------------

def repair_last_days(days):

    repaired = 0

    for i in range(days + 1):

        target_date = datetime.now(timezone.utc).date() - timedelta(days=i)

        print(f"Repairing candles for {target_date}")

        reingest_day(target_date)

        repaired += 1

    # -----------------------------
    # Notify admin when repair finishes
    # -----------------------------

    ist_now = datetime.now(timezone.utc).astimezone(
        ZoneInfo("Asia/Kolkata")
    )

    message = f"""
    Nifty Dashboard Repair Completed

    Days Repaired: {repaired}

    Time: {ist_now.strftime("%d %b %Y %I:%M %p IST")}
    """

    send_telegram_alert(message)

# -----------------------------
# Temp 2H repair for last 60 days
# -----------------------------
def repair_intraday_days(days):

    print(f"Repairing last {days} days of 2H candles")

    for symbol in INTRADAY_SYMBOLS:
        save_candles(
            symbol,
            "2h",
            TIMEFRAMES["2h"]["interval"],
            f"{days}d"   # dynamic period
        )

    print("2H repair complete ✅")