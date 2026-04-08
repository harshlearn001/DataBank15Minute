from kiteconnect import KiteConnect
import json
import os
from dotenv import load_dotenv

print("🔍 CHECKING ZERODHA LOGIN...\n")

# ==============================
# LOAD ENV
# ==============================
env_path = "H:\\DataBank15Minute\\.env"
load_dotenv(dotenv_path=env_path)

api_key = os.getenv("KITE_API_KEY")

# ==============================
# DEBUG
# ==============================
print("📂 Current Path:", os.getcwd())
print("📄 ENV EXISTS:", os.path.exists(env_path))
print("🔑 API KEY:", api_key)

if not api_key:
    print("\n❌ ERROR: API key not loaded")
    exit()

print(f"✅ ENV OK | {api_key[:5]}*****")

# ==============================
# LOAD TOKEN
# ==============================
try:
    with open("token.json", "r") as f:
        access_token = json.load(f)["access_token"]
except:
    print("❌ ERROR: token.json missing or invalid")
    exit()

# ==============================
# INIT
# ==============================
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# ==============================
# TEST
# ==============================
try:
    profile = kite.profile()

    print("\n✅ LOGIN VERIFIED SUCCESSFULLY 🎉")
    print(f"👤 Name   : {profile['user_name']}")
    print(f"📧 Email  : {profile['email']}")
    print(f"🆔 UserID : {profile['user_id']}")

except Exception as e:
    print("\n❌ LOGIN FAILED")
    print("Reason:", e)