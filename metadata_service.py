import yfinance as yf
from datetime import datetime
from database import SessionLocal
from models import SymbolMetadata
from universe import load_stock_universe


def update_symbol_metadata():

    symbols = load_stock_universe()
    db = SessionLocal()

    try:
        for symbol in symbols:

            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info

                sector = info.get("sector")
                industry = info.get("industry")

            except Exception:
                print(f"Failed fetching metadata for {symbol}")
                continue

            existing = db.query(SymbolMetadata).filter_by(symbol=symbol).first()

            if existing:
                existing.sector = sector
                existing.industry = industry
                existing.last_updated = datetime.utcnow()
            else:
                db.add(SymbolMetadata(
                    symbol=symbol,
                    sector=sector,
                    industry=industry,
                    last_updated=datetime.utcnow()
                ))

            print(f"Updated metadata for {symbol}")

        db.commit()

    finally:
        db.close()

    print("Metadata update complete ✅")