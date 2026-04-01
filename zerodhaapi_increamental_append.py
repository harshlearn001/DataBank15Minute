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
# LOAD SYMBOLS
# ==============================
df_symbols = pd.read_csv(SYMBOL_FILE)

stocks = (
    df_symbols.iloc[:, 0]
    .dropna()
    .astype(str)
    .str.strip()
    .str.upper()
    .tolist()
)

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

def market_hours(df):
    return df[
        (df["date"].dt.time >= pd.to_datetime("09:15").time()) &
        (df["date"].dt.time <= pd.to_datetime("15:30").time())
    ]

def safe_save(df, path):
    temp = path.replace(".csv", "_temp.csv")
    df.to_csv(temp, index=False)
    os.replace(temp, path)

def is_market_open():
    now = datetime.now().time()
    return pd.to_datetime("09:15").time() <= now <= pd.to_datetime("15:30").time()

# ==============================
# SMART SLEEP FUNCTION 🔥
# ==============================
def sleep_until_market():
    now = datetime.now()

    next_open = now.replace(hour=9, minute=15, second=0, microsecond=0)

    if now.time() > pd.to_datetime("15:30").time():
        next_open += timedelta(days=1)

    sleep_sec = (next_open - now).total_seconds()

    print(f"😴 Sleeping till market open: {next_open}")
    time.sleep(max(60, sleep_sec))

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
# MAIN ENGINE LOOP
# ==============================
while True:

    now = datetime.now()
    print("\n" + "="*60)
    print("🚀 ENGINE START:", now)
    print("="*60)

    # ==============================
    # MARKET CHECK
    # ==============================
    if not is_market_open():
        print("⛔ Market closed")
        sleep_until_market()
        continue

    # ==============================
    # INIT KITE + INSTRUMENTS
    # ==============================
    try:
        kite = get_kite()
        inst = load_instruments(kite)
    except Exception as e:
        print("❌ Token / Instrument error:", e)
        print("⏳ Retry in 5 minutes...")
        time.sleep(300)
        continue

    # ==============================
    # LOOP SYMBOLS
    # ==============================
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

                if df_old.empty:
                    last_date = None
                else:
                    df_old["date"] = pd.to_datetime(df_old["date"], errors="coerce")
                    df_old = clean_df(df_old)
                    df_old = market_hours(df_old)
                    last_date = df_old["date"].max()
            else:
                df_old = pd.DataFrame()
                last_date = None

            print("🧠 Last candle:", last_date)

            # ==============================
            # INCREMENTAL LOGIC
            # ==============================
            if last_date is None or pd.isna(last_date):
                start_date = now - timedelta(minutes=30)
                print("⚠️ Fresh fetch")
            else:
                start_date = last_date - timedelta(minutes=15)

                if (now - last_date).total_seconds() > 3600:
                    print("⚠️ Gap detected → recent fetch")
                    start_date = now - timedelta(minutes=30)

            print("📅 Fetching from:", start_date)

            # ==============================
            # FETCH
            # ==============================
            df_new = fetch(kite, token, start_date, now)

            print("📊 Rows:", len(df_new))

            if df_new.empty:
                print("⚠️ No new data")
                continue

            # ==============================
            # CLEAN NEW
            # ==============================
            df_new["date"] = make_ist(df_new["date"])
            df_new = df_new[["date", "open", "high", "low", "close", "volume"]]
            df_new = clean_df(df_new)
            df_new = market_hours(df_new)

            # ==============================
            # MERGE
            # ==============================
            df = pd.concat([df_old, df_new], ignore_index=True)
            df = clean_df(df)

            # ==============================
            # SAVE
            # ==============================
            safe_save(df, file_path)

            print(f"✅ Updated {s} | +{len(df_new)} rows")
            print("📌 Latest:", df["date"].max())

        except Exception as e:
            print(f"❌ Error {s}: {e}")

    # ==============================
    # WAIT FOR NEXT CANDLE
    # ==============================
    print("\n⏳ Waiting 15 minutes for next candle...\n")
    time.sleep(900)