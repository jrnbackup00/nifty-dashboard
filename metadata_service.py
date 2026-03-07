import yfinance as yf
from datetime import datetime
from database import SessionLocal
from models import SymbolMetadata
from universe import load_stock_universe
import os
import pandas as pd
from models import SymbolGroupMap

GROUP_DIR = "data/groups"

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

def update_group_mappings():

    db = SessionLocal()

    try:

        db.query(SymbolGroupMap).delete()

        for group_type in ["sectors", "themes"]:

            folder = os.path.join(GROUP_DIR, group_type)

            for file in os.listdir(folder):

                if not file.endswith(".csv"):
                    continue

                group_name = file.replace(".csv", "").replace("_"," ").upper()

                df = pd.read_csv(os.path.join(folder, file))

                for symbol in df["symbol"]:

                    db.add(
                        SymbolGroupMap(
                            symbol=symbol.strip().upper(),
                            group_name=group_name,
                            group_type=group_type[:-1]  # sector/theme
                        )
                    )

        db.commit()

        print("Group mappings updated")

    finally:
        db.close()