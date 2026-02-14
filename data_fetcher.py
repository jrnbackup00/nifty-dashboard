import yfinance as yf
from universe import load_stock_universe


def fetch_market_data(days=60):

    symbols = load_stock_universe()
    print(f"Fetching data for {len(symbols)} symbols...")

    all_data = []
    batch_size = 100

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]

        print(f"Fetching batch {i//batch_size + 1}")

        data = yf.download(
            tickers=" ".join(batch),
            period=f"{days}d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            threads=True
        )

        all_data.append(data)

    print("All batches complete ✅")

    return all_data


if __name__ == "__main__":
    fetch_market_data()
