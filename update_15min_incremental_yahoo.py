import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

# ==============================
# PATHS
# ==============================
SYMBOL_FILE = r"H:\DataBank15Minute\symbols\stocks.csv"
DATA_DIR = r"H:\DataBank15Minute\data\15min"

os.makedirs(DATA_DIR, exist_ok=True)

# ==============================
# LOAD SYMBOLS
# ==============================
symbols = pd.read_csv(SYMBOL_FILE).iloc[:, 0].dropna().tolist()

# ==============================
# HELPERS
# ==============================
def convert_symbol(sym):
    return str(sym).strip().upper() + ".NS"

def make_ist(series):
    return pd.to_datetime(series, utc=True)\
        .dt.tz_convert("Asia/Kolkata")\
        .dt.tz_localize(None)

def market_hours(df):
    return df[
        (df["date"].dt.time >= pd.to_datetime("09:15").time()) &
        (df["date"].dt.time <= pd.to_datetime("15:30").time())
    ]

def safe_save(df, path):
    temp = path.replace(".csv", "_temp.csv")
    df.to_csv(temp, index=False)
    os.replace(temp, path)

# ==============================
# MAIN LOOP
# ==============================
for sym in symbols:
    try:
        ticker = convert_symbol(sym)
        file_path = os.path.join(DATA_DIR, f"{sym}.csv")

        print(f"\n⬇️ Processing {ticker}")

        # ==============================
        # LOAD OLD DATA
        # ==============================
        if os.path.exists(file_path):
            df_old = pd.read_csv(file_path)

            if df_old.empty:
                start_date = datetime.now() - timedelta(days=5)
            else:
                df_old["date"] = make_ist(df_old["date"])
                df_old = market_hours(df_old)

                last_date = df_old["date"].max()

                # 🔥 Refresh last candle
                start_date = last_date - timedelta(minutes=15)
                start_date = max(start_date, datetime.now() - timedelta(days=60))

        else:
            df_old = pd.DataFrame()
            start_date = datetime.now() - timedelta(days=60)

        print("📅 Fetching from:", start_date)

        # ==============================
        # DOWNLOAD DATA
        # ==============================
        df_new = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            interval="15m",
            progress=False
        )

        if df_new.empty:
            print("⚠️ No new data")
            continue

        # ==============================
        # FIX MULTI-INDEX (CRITICAL)
        # ==============================
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)

        df_new.reset_index(inplace=True)

        df_new.rename(columns={
            "Datetime": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        df_new = df_new[["date", "open", "high", "low", "close", "volume"]]

        # ==============================
        # CLEAN DATA
        # ==============================
        df_new["date"] = make_ist(df_new["date"])
        df_new = market_hours(df_new)

        # ==============================
        # APPEND + FIX LAST CANDLE
        # ==============================
        df = pd.concat([df_old, df_new], ignore_index=True)

        df.drop_duplicates(subset=["date"], keep="last", inplace=True)
        df.sort_values("date", inplace=True)

        # ==============================
        # SAVE
        # ==============================
        safe_save(df, file_path)

        print(f"✅ Updated: {sym} ({len(df_new)} new rows)")

    except Exception as e:
        print(f"❌ Error {sym}:", e)

print("\n🚀 YFinance incremental system READY")