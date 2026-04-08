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
TOKEN_PATH = r"H:\DataBank15Minute\token.json"

os.makedirs(DATA_PATH, exist_ok=True)

# ==============================
# USER INPUT MODE
# ==============================
user_input = input("Enter start date (YYYY-MM-DD) or press Enter for LIVE mode: ").strip()

if user_input:
    try:
        MANUAL_MODE = True
        MANUAL_START_DATE = datetime.strptime(user_input, "%Y-%m-%d")
        print(f"📅 Manual mode ON from {MANUAL_START_DATE}")
    except:
        print("❌ Invalid date format → LIVE mode")
        MANUAL_MODE = False
else:
    MANUAL_MODE = False

# ==============================
# LOAD SYMBOLS
# ==============================
df_symbols = pd.read_csv(SYMBOL_FILE)
stocks = df_symbols.iloc[:, 0].dropna().astype(str).str.strip().str.upper().tolist()

# ==============================
# INIT KITE
# ==============================
def get_kite():
    with open(TOKEN_PATH) as f:
        token_data = json.load(f)

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(token_data["access_token"])
    return kite

# ==============================
# LOAD INSTRUMENTS
# ==============================
def load_instruments(kite):
    inst = pd.DataFrame(kite.instruments("NSE"))
    inst["norm"] = inst["tradingsymbol"].str.replace("&", "").str.replace("-", "")
    return inst

# ==============================
# HELPERS
# ==============================
def make_ist(series):
    return pd.to_datetime(series, utc=True, errors="coerce")\
        .dt.tz_convert("Asia/Kolkata")\
        .dt.tz_localize(None)

def clean_df(df):
    df = df.dropna(subset=["date"])
    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date")
    return df

def safe_save(df, path):
    temp = path.replace(".csv", "_temp.csv")
    df.to_csv(temp, index=False)
    os.replace(temp, path)

# ==============================
# FETCH
# ==============================
def fetch(kite, token, start, end):
    try:
        data = kite.historical_data(token, start, end, "15minute")
        return pd.DataFrame(data)
    except Exception as e:
        print(f"⚠️ Fetch error: {e}")
        return pd.DataFrame()

# ==============================
# MAIN LOOP
# ==============================
while True:

    now = datetime.now()
    print("\n" + "="*60)
    print("🚀 ENGINE RUN:", now)
    print("="*60)

    try:
        kite = get_kite()
        inst = load_instruments(kite)
    except Exception as e:
        print("❌ Token error:", e)
        time.sleep(300)
        continue

    for s in stocks:
        try:
            file_path = os.path.join(DATA_PATH, f"{s}.csv")

            row = inst[inst["norm"] == s.replace("&", "").replace("-", "")]
            if row.empty:
                print("❌ Symbol not found:", s)
                continue

            token = int(row["instrument_token"].values[0])
            print(f"\n⬇️ {s}")

            # ==============================
            # LOAD OLD DATA
            # ==============================
            if os.path.exists(file_path):
                df_old = pd.read_csv(file_path)

                if not df_old.empty and "date" in df_old.columns:
                    df_old["date"] = pd.to_datetime(df_old["date"], errors="coerce")
                    df_old = clean_df(df_old)
                    last_date = df_old["date"].max()
                else:
                    df_old = pd.DataFrame()
                    last_date = None
            else:
                df_old = pd.DataFrame()
                last_date = None

            print("🧠 Last candle:", last_date)

            # ==============================
            # START DATE LOGIC (FIXED)
            # ==============================
            if MANUAL_MODE:
                start_date = MANUAL_START_DATE
                print("📌 Manual fetch from:", start_date)
            else:
                if last_date is None:
                    start_date = now - timedelta(days=3)
                else:
                    start_date = last_date

            print("📅 Fetching from:", start_date)

            # ==============================
            # FETCH
            # ==============================
            df_new = fetch(kite, token, start_date, now)

            if df_new.empty:
                print("⚠️ No new data")
                continue

            df_new["date"] = make_ist(df_new["date"])
            df_new = df_new[["date", "open", "high", "low", "close", "volume"]]
            df_new = clean_df(df_new)

            # ==============================
            # STRICT FILTER (IMPORTANT)
            # ==============================
            if last_date is not None:
                df_new = df_new[df_new["date"] > last_date]

            if df_new.empty:
                print("⚠️ Nothing new to append")
                continue

            # ==============================
            # APPEND SAFE
            # ==============================
            if not df_old.empty:
                df = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df = df_new.copy()

            df = clean_df(df)

            # ==============================
            # DATA LOSS PROTECTION
            # ==============================
            if not df_old.empty and len(df) < len(df_old):
                print("🚨 Data loss detected! Skip")
                continue

            # ==============================
            # SAVE
            # ==============================
            safe_save(df, file_path)

            print(f"✅ Appended {len(df_new)} rows | Total: {len(df)}")

        except Exception as e:
            print(f"❌ Error {s}: {e}")

    print("\n⏳ Waiting 15 minutes...\n")
    time.sleep(900)