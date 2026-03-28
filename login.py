import json
import webbrowser
import re
import os
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from dotenv import load_dotenv

# =========================
# LOAD ENV
# =========================
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

TOKEN_FILE = "token.json"

# =========================
# EXTRACT TOKEN
# =========================
def extract_request_token(text):
    text = text.strip()

    # If full URL
    if text.startswith("http"):
        parsed = urlparse(text)
        params = parse_qs(parsed.query)
        if "request_token" in params:
            return params["request_token"][0]

    # Regex fallback
    match = re.search(r"request_token=([A-Za-z0-9]+)", text)
    if match:
        return match.group(1)

    # Direct token
    if re.match(r"^[A-Za-z0-9]{20,}$", text):
        return text

    raise Exception("Invalid input. Cannot find request_token")

# =========================
# LOGIN
# =========================
def login():
    if not API_KEY or not API_SECRET:
        print("❌ ERROR: Check your .env file")
        return

    kite = KiteConnect(api_key=API_KEY)

    print("\n🔐 Zerodha Login Start...\n")

    login_url = kite.login_url()
    print("Opening browser...")
    webbrowser.open(login_url)

    print("\n👉 After login:")
    print("Copy FULL URL from browser")
    print("Paste below quickly (within 2 min)\n")

    user_input = input("Paste URL here: ")

    try:
        request_token = extract_request_token(user_input)
        print(f"\n✔ Token extracted")

        session = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = session["access_token"]

        kite.set_access_token(access_token)

        # Save token
        with open(TOKEN_FILE, "w") as f:
            json.dump({"access_token": access_token}, f)

        print("\n✅ LOGIN SUCCESS")
        print("Token saved → token.json")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")

# =========================
# RUN
# =========================
if __name__ == "__main__":
    login()