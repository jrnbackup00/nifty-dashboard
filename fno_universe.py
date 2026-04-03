import pandas as pd
import os

def load_fno_universe():
    try:
        file_path = os.path.join(os.path.dirname(__file__), "fno_universe.csv")

        df = pd.read_csv(file_path)

        symbols = df["symbol"].dropna().unique().tolist()

        # Always include indices
        symbols += ["^NSEI", "^NSEBANK"]

        return symbols

    except Exception as e:
        print("F&O universe load failed:", e)

        # Fallback (VERY IMPORTANT)
        return ["^NSEI", "^NSEBANK"]