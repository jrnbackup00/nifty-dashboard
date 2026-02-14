import requests
import pandas as pd
from datetime import datetime, timedelta

CACHE_FILE = "stocks_cache.csv"
CACHE_EXPIRY_HOURS = 24


def fetch_nse_stocks():
    """
    Fetch all NSE equity symbols
    """

    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

    df = pd.read_csv(url)

    symbols = df["SYMBOL"].tolist()

    # Convert to Yahoo format
    yahoo_symbols = [s + ".NS" for s in symbols]

    # Add indices
    yahoo_symbols += ["^NSEI", "^NSEBANK"]

    return sorted(set(yahoo_symbols))


def load_stock_universe():
    """
    Load cached data or refresh
    """

    try:
        df = pd.read_csv(CACHE_FILE)

        last_updated = datetime.fromisoformat(df.attrs.get("timestamp"))

        if datetime.now() - last_updated < timedelta(hours=CACHE_EXPIRY_HOURS):
            return df["symbol"].tolist()

    except:
        pass

    symbols = fetch_nse_stocks()

    df = pd.DataFrame({"symbol": symbols})
    df.attrs["timestamp"] = datetime.now().isoformat()
    df.to_csv(CACHE_FILE, index=False)

    return symbols
