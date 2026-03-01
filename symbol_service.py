from sqlalchemy.orm import Session
from database import SessionLocal
from models import MarketCandle


def get_all_symbols(timeframe="1d"):
    db: Session = SessionLocal()

    try:
        rows = (
            db.query(MarketCandle.symbol)
            .filter(MarketCandle.timeframe == timeframe)
            .distinct()
            .all()
        )

        return [r[0] for r in rows]

    finally:
        db.close()