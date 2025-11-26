# src/fetch_bhavcopy.py
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

HEADERS = {"User-Agent": "Mozilla/5.0"}

BASE = "https://archives.nseindia.com/content/fo"

MONTH_MAP = {
    1: "JAN", 2: "FEB", 3: "MAR",
    4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP",
    10: "OCT", 11: "NOV", 12: "DEC"
}

def fetch_bhavcopy(max_days_back=10):
    today = datetime.today()
    tried = []

    for i in range(max_days_back):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:  # skip Sat/Sun
            continue

        dd = d.strftime("%d")
        mon = MONTH_MAP[d.month]
        yyyy = d.strftime("%Y")

        filename = f"fo{dd}{mon}{yyyy}bhav.DAT"
        url = f"{BASE}/{filename}"
        tried.append(url)

        print("Trying:", url)
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200 and len(r.content) > 1000:
                print("Downloaded:", url)

                text = r.content.decode("latin1", errors="replace")

                # delimiter detection
                header = text.splitlines()[0]
                if '|' in header: sep = '|'
                elif ',' in header: sep = ','
                elif '\t' in header: sep = '\t'
                else: sep = ','

                df = pd.read_csv(StringIO(text), sep=sep)
                df.columns = [c.strip() for c in df.columns]
                print("Rows:", len(df))
                return df

        except Exception as e:
            print("Error:", e)

    print("\nTried URLs:")
    for u in tried:
        print(u)
    raise Exception("No valid F&O DAT file found in last 10 trading days.")
    
