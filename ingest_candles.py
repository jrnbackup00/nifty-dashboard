import yfinance as yf
from datetime import timezone
from database import SessionLocal
from models import MarketCandle
from sqlalchemy.dialects.postgresql import insert
from universe import load_stock_universe

# ----------------------------
# CONFIG
# ----------------------------

STOCK_SYMBOLS = load_stock_universe()
INDEX_SYMBOLS = ["^NSEI", "^NSEBANK"]

DAILY_SYMBOLS = STOCK_SYMBOLS + INDEX_SYMBOLS
INTRADAY_SYMBOLS = INDEX_SYMBOLS  # only indices get 2h

TIMEFRAMES = {
    "1d": {"interval": "1d", "period": "90d"},
    "2h": {"interval": "60m", "period": "60d"},
}


# ----------------------------
# INGEST FUNCTION
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
        records = []

        for timestamp, row in df.iterrows():
            records.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": timestamp.to_pydatetime().replace(tzinfo=timezone.utc),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
            })

        stmt = insert(MarketCandle).values(records)

        stmt = stmt.on_conflict_do_nothing(
            index_elements=["symbol", "timeframe", "timestamp"]
        )

        result = db.execute(stmt)
        db.commit()

        print(f"{symbol} {timeframe} → Inserted {result.rowcount}")

    finally:
        db.close()


# ----------------------------
# MAIN
# ----------------------------

if __name__ == "__main__":

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

    print("Ingestion complete ✅")