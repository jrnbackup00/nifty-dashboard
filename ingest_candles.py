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
from zoneinfo import ZoneInfo
from telegram_alert import send_telegram_alert

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


# ----------------------------
# CORE SAVE FUNCTION
# ----------------------------

def save_candles(symbol, timeframe, interval, period):

    df = yf.download(
        symbol,
        interval=interval,
        period=period,
        auto_adjust=False,
        threads=True
    )

    if df.empty:
        print(f"No data for {symbol} {timeframe}")
        return

    if hasattr(df.columns, "levels"):
        df.columns = df.columns.get_level_values(0)

    db = SessionLocal()

    try:
        today_utc = datetime.now(timezone.utc).date()

        records = []

        for timestamp, row in df.iterrows():
            ts = timestamp.to_pydatetime().astimezone(timezone.utc)

            record = {
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": ts,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
            }

            records.append(record)

        if not records:
            return

        stmt = insert(MarketCandle).values(records)

        # Upsert — update only today's candles
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