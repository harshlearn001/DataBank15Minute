import yfinance as yf
import pandas as pd
import os
import time

# ==============================
# PATHS
# ==============================
SYMBOL_FILE = r"H:\DataBank15Minute\symbols\stocks.csv"
OUTPUT = r"H:\GB_SCANNER\data\daily_yahoo"

os.makedirs(OUTPUT, exist_ok=True)

# ==============================
# LOAD SYMBOLS
# ==============================
df_symbols = pd.read_csv(SYMBOL_FILE)
stocks = df_symbols["symbol"].dropna().tolist()

# ==============================
# FUNCTION
# ==============================
def convert_symbol(sym):
    return sym + ".NS"   # NSE format

# ==============================
# DOWNLOAD
# ==============================
for s in stocks:
    try:
        ticker = convert_symbol(s)

        print("⬇️", ticker)

        df = yf.download(
            ticker,
            period="1y",
            interval="1d",
            auto_adjust=False,
            progress=False
        )

        if df.empty:
            print("⚠️ No data")
            continue

        df.reset_index(inplace=True)

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        df = df[["date", "open", "high", "low", "close", "volume"]]

        out_path = os.path.join(OUTPUT, f"{s}.csv")
        df.to_csv(out_path, index=False)

        print("✅ saved")

        time.sleep(0.2)

    except Exception as e:
        print("❌", s, e)

print("\n🚀 Yahoo daily update complete")