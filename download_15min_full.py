import json
import pandas as pd
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import os
import time

# ==============================
# CONFIG
# ==============================
API_KEY = "v3pczlie66ksj1p3"
DATA_PATH = r"H:\DataBank15Minute\data\15min"
SYMBOL_FILE = r"H:\DataBank15Minute\symbols\stocks.csv"

FROM_DATE = datetime(2024, 1, 1)
TO_DATE   = datetime.now()

MIN_ROWS = 5000
MIN_DAYS = 50

# ==============================
# LOAD SYMBOLS
# ==============================
df_symbols = pd.read_csv(SYMBOL_FILE)
stocks = df_symbols["symbol"].dropna().tolist()

# ==============================
# LOAD TOKEN
# ==============================
with open("token.json") as f:
    token_data = json.load(f)

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(token_data["access_token"])

os.makedirs(DATA_PATH, exist_ok=True)

# ==============================
# LOAD INSTRUMENTS
# ==============================
inst = pd.DataFrame(kite.instruments("NSE"))
inst["norm"] = inst["tradingsymbol"].str.replace("&","").str.replace("-","")

# ==============================
# FETCH FUNCTION
# ==============================
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

# ==============================
# CLEAN + FIX DATE (CRITICAL)
# ==============================
def fix_date(df):
    # Convert UTC → IST
    df["date"] = pd.to_datetime(df["date"], utc=True)\
        .dt.tz_convert("Asia/Kolkata")\
        .dt.tz_localize(None)

    # 🔥 REMOVE NON-MARKET HOURS
    df = df[
        (df["date"].dt.time >= pd.to_datetime("09:15").time()) &
        (df["date"].dt.time <= pd.to_datetime("15:30").time())
    ]

    return df

# ==============================
# VALIDATION FUNCTION (FINAL)
# ==============================
def is_bad_file(path):
    if not os.path.exists(path):
        return True

    try:
        df = pd.read_csv(path)

        # ❌ Empty
        if df.empty:
            return True

        # ❌ Required columns
        required = {"date", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            print("⚠️ Missing columns")
            return True

        # ❌ Corrupt columns
        if any("(" in str(col) for col in df.columns):
            print("⚠️ Corrupt columns")
            return True

        # Convert date properly (IST)
        df["date"] = pd.to_datetime(df["date"], utc=True)\
            .dt.tz_convert("Asia/Kolkata")\
            .dt.tz_localize(None)

        # ❌ Invalid dates
        if df["date"].isna().sum() > 0:
            print("⚠️ Invalid dates")
            return True

        # ❌ Too few rows
        if len(df) < MIN_ROWS:
            print("⚠️ Too few rows:", len(df))
            return True

        # ❌ Too few trading days
        if df["date"].dt.date.nunique() < MIN_DAYS:
            print("⚠️ Too few trading days")
            return True

        # ❌ Not recent
        last_date = df["date"].max()
        if (datetime.now() - last_date).days > 5:
            print("⚠️ Data not recent:", last_date)
            return True

        return False

    except Exception as e:
        print("⚠️ Read error:", e)
        return True

# ==============================
# SAFE SAVE
# ==============================
def safe_save(df, path):
    temp = path.replace(".csv", "_temp.csv")
    df.to_csv(temp, index=False)
    os.replace(temp, path)

# ==============================
# MAIN LOOP
# ==============================
for s in stocks:
    try:
        file_path = os.path.join(DATA_PATH, f"{s}.csv")

        print(f"\n🔍 Checking {s}")

        # ==============================
        # VALIDATE
        # ==============================
        if not is_bad_file(file_path):
            print("✅ Already good, skipping")
            continue

        print("📥 Re-downloading:", s)

        # ==============================
        # FIND TOKEN
        # ==============================
        row = inst[inst["norm"] == s.replace("&","").replace("-","")]

        if row.empty:
            print("❌ Symbol not found:", s)
            continue

        token = int(row["instrument_token"].values[0])

        # ==============================
        # FETCH DATA
        # ==============================
        df = fetch(token, FROM_DATE, TO_DATE)

        if df.empty:
            print("⚠️ No data:", s)
            continue

        # ==============================
        # CLEAN DATA
        # ==============================
        df = fix_date(df)

        df = df[["date", "open", "high", "low", "close", "volume"]]

        df.drop_duplicates(subset=["date"], inplace=True)
        df.sort_values("date", inplace=True)

        # ==============================
        # SAVE
        # ==============================
        safe_save(df, file_path)

        print(f"✅ Fixed {s} | Rows: {len(df)}")

    except Exception as e:
        print(f"❌ Error {s}:", e)

print("\n🚀 MASTER DATA ENGINE COMPLETE")