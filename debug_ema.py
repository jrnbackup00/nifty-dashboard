from database import SessionLocal
from models import MarketCandle
import pandas as pd

# -----------------------------
# LOAD DATA FROM DB
# -----------------------------
db = SessionLocal()

rows = db.query(MarketCandle).filter(
    MarketCandle.symbol == "^NSEI",
    MarketCandle.timeframe == "2h"
).order_by(MarketCandle.timestamp).all()

db.close()

# -----------------------------
# BUILD DATAFRAME
# -----------------------------
df = pd.DataFrame([{
    "timestamp": r.timestamp,
    "close": float(r.close)
} for r in rows])

# VERY IMPORTANT
df = df.sort_values("timestamp")

# -----------------------------
# CALCULATE EMA (TradingView match)
# -----------------------------
df["EMA5"] = df["close"].ewm(span=5, adjust=False).mean()
df["EMA20"] = df["close"].ewm(span=20, adjust=False).mean()

# -----------------------------
# PRINT LAST 10 ROWS
# -----------------------------
print("\n===== LAST 10 CANDLES =====\n")
print(df.tail(10))

# -----------------------------
# PRINT LATEST VALUE CLEANLY
# -----------------------------
latest = df.iloc[-1]

print("\n===== LATEST EMA CHECK =====")
print(f"Timestamp : {latest['timestamp']}")
print(f"Close     : {latest['close']}")
print(f"EMA5      : {round(latest['EMA5'], 2)}")
print(f"EMA20     : {round(latest['EMA20'], 2)}")