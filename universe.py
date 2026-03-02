import pandas as pd
import requests

CACHE_FILE = "nifty500_cache.csv"

def fetch_nifty500():
    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
    df = pd.read_csv(url)

    symbols = df["Symbol"].tolist()
    yahoo_symbols = [s + ".NS" for s in symbols]

    # Add indices
    yahoo_symbols += ["^NSEI", "^NSEBANK"]

    return sorted(set(yahoo_symbols))


def load_stock_universe():
    try:
        df = pd.read_csv(CACHE_FILE)
        return df["symbol"].tolist()
    except:
        symbols = fetch_nifty500()
        pd.DataFrame({"symbol": symbols}).to_csv(CACHE_FILE, index=False)
        return symbols
import pandas as pd


def load_nifty50_universe():
    df = pd.read_csv("data/nifty50.csv")
    return df["symbol"].dropna().tolist()


def load_banknifty_universe():
    df = pd.read_csv("data/banknifty.csv")
    return df["symbol"].dropna().tolist()