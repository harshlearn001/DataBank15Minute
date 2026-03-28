import json
import pandas as pd
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import os
import time

API_KEY = "v3pczlie66ksj1p3"
DATA_PATH = r"H:\DataBank15Minute\data\15min"
SYMBOL_FILE = r"H:\DataBank15Minute\symbols\stocks.csv"

FROM_DATE = datetime(2024, 1, 1)
TO_DATE   = datetime.now()

# Load symbols
df_symbols = pd.read_csv(SYMBOL_FILE)
stocks = df_symbols["symbol"].dropna().tolist()

# Load token
with open("token.json") as f:
    token_data = json.load(f)

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(token_data["access_token"])

os.makedirs(DATA_PATH, exist_ok=True)

# Load instruments
inst = pd.DataFrame(kite.instruments("NSE"))
inst["norm"] = inst["tradingsymbol"].str.replace("&","").str.replace("-","")

def fetch(token, start, end):
    data_all = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=180), end)
        data = kite.historical_data(token, cur, nxt, "15minute")
        data_all.extend(data)
        cur = nxt + timedelta(days=1)
        time.sleep(0.2)
    return pd.DataFrame(data_all)

for s in stocks:
    try:
        row = inst[inst["norm"] == s.replace("&","").replace("-","")]
        if row.empty:
            print("❌", s)
            continue

        token = int(row["instrument_token"].values[0])
        print("⬇️", s)

        df = fetch(token, FROM_DATE, TO_DATE)
        if df.empty:
            continue

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        df.to_csv(f"{DATA_PATH}\\{s}.csv")
        print("✅ saved")

    except Exception as e:
        print("❌", s, e)