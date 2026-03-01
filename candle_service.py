from sqlalchemy.orm import Session
from database import SessionLocal
from models import MarketCandle
import pandas as pd


def get_candles(symbol: str, timeframe: str, limit: int = 100):
    db: Session = SessionLocal()

    try:
        rows = (
            db.query(MarketCandle)
            .filter(
                MarketCandle.symbol == symbol,
                MarketCandle.timeframe == timeframe
            )
            .order_by(MarketCandle.timestamp.desc())
            .limit(limit)
            .all()
        )

        if not rows:
            return pd.DataFrame()

        data = [
            {
                "timestamp": r.timestamp,
                "Open": r.open,
                "High": r.high,
                "Low": r.low,
                "Close": r.close,
                "Volume": r.volume,
            }
            for r in reversed(rows)
        ]

        df = pd.DataFrame(data)
        df.set_index("timestamp", inplace=True)

        return df

    finally:
        db.close()