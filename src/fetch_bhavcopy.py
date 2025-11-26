import requests
import pandas as pd
from datetime import datetime, timedelta

# -------------------------
# VALID DAT FILENAME PATTERNS (ALL REAL NSE FORMAT)
# -------------------------
DAT_PATTERNS = [
    "https://archives.nseindia.com/content/fo/FNO_BC{dmy}.DAT",          # FNO_BC26112025.DAT
    "https://archives.nseindia.com/content/fo/FNOBC{dmy}.DAT",           # FNOBC26112025.DAT
    "https://archives.nseindia.com/content/fo/FNOBCT{dmy}.DAT",          # FNOBCT26112025.DAT
    "https://archives.nseindia.com/content/fo/FNO_BC{dmY}.DAT",          # FNO_BC26NOV2025.DAT
    "https://archives.nseindia.com/content/fo/fo{dmY}bhav.DAT",          # fo26NOV2025bhav.DAT
]

def fetch_bhavcopy():
    today = datetime.now()
    tried_urls = []

    for i in range(7):            # Last 7 days
        dt = today - timedelta(days=i)
        if dt.weekday() >= 5:     # Skip Sat/Sun
            continue

        dmy = dt.strftime("%d%m%Y")
        dmY = dt.strftime("%d%b%Y").upper()

        for p in DAT_PATTERNS:
            url = p.format(dmy=dmy, dmY=dmY)
            tried_urls.append(url)
            print(f"Trying DAT: {url}")

            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200 and len(r.content) > 10000:   # DAT must be large
                    print(f"✔ DAT found: {url}")
                    raw = r.content.decode("latin1")

                    df = pd.read_csv(
                        pd.io.common.StringIO(raw),
                        sep=",",
                        header=None
                    )

                    # DAT FORMAT → EXACT COLUMN HEADERS
                    df.columns = [
                        "INSTRUMENT", "SYMBOL", "EXPIRY_DT",
                        "STRIKE_PR", "OPTION_TYP",
                        "OPEN", "HIGH", "LOW", "CLOSE",
                        "SETTLE_PR", "CONTRACTS", "VAL_INLAKH",
                        "OPEN_INT", "CHG_IN_OI", "TIMESTAMP"
                    ]

                    return df

            except Exception:
                pass

    print("\nTried URLs:")
    for u in tried_urls:
        print(u)

    raise Exception("No valid DAT bhavcopy found in last 7 trading days.")
    
