from sqlalchemy.orm import Session
from database import SessionLocal
from models import MarketCandle
from models import Symbol
from fno_universe import load_fno_universe


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




def seed_fno_symbols():

    db = SessionLocal()

    try:
        symbols = load_fno_universe()

        inserted = 0

        for sym in symbols:

            exists = db.query(Symbol).filter_by(symbol=sym).first()

            if not exists:
                db.add(Symbol(
                    symbol=sym,
                    is_fno=True,
                    is_index=sym.startswith("^")
                ))
                inserted += 1

        db.commit()
        print(f"Seed complete → {inserted} new symbols added")

    finally:
        db.close()