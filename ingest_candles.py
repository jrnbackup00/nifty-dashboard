import yfinance as yf
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert
from database import SessionLocal
from models import MarketCandle
from universe import load_stock_universe


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