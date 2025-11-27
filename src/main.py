import pandas as pd
import zipfile
import requests
import io
import json
from datetime import datetime, timedelta

BASE_URL = "https://archives.nseindia.com/content/fo/"

def download_latest_bhavcopy():
    today = datetime.today()

    for i in range(0, 5):   # last 5 days
        dt = today - timedelta(days=i)
        fname = f"BhavCopy_NSE_FO_0_0_0_{dt.strftime('%Y%m%d')}_F_0000.csv.zip"
        url = BASE_URL + fname

        print("Trying:", url)
        r = requests.get(url)

        if r.status_code == 200:
            print("Downloaded:", url)
            return r.content

    raise Exception("No valid BhavCopy found in last 5 days")

def extract_csv(zip_bytes):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    csv_name = z.namelist()[0]
    print("Extracting:", csv_name)
    df = pd.read_csv(z.open(csv_name))
    return df

def compute_signals(df):

    # Updated column names based on NSE bhavcopy screenshot
    required_cols = [
        "TckrSymb",
        "FinInstrmTp",
        "XpryDt",
        "StrkPric",          # UPDATED
        "OptnTp",
        "OpnIntrst",
        "ChngInOpnIntrst",
        "TrnOlTrdVol",       # UPDATED
        "LastPric",
        "ClsPric"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Missing column: {col}")

    # Build new signals
    df["OI_Buildup"] = df["ChngInOpnIntrst"]
    df["Volume_Spike"] = df["TrnOlTrdVol"]
    df["Price_Change"] = df["LastPric"] - df["ClsPric"]

    return df

def save_outputs(df):
    df.to_csv("fo_latest.csv", index=False)

    summary = df[[
        "TckrSymb",
        "XpryDt",
        "StrkPric",          # UPDATED
        "OptnTp",
        "OpnIntrst",
        "ChngInOpnIntrst",
        "TrnOlTrdVol",       # UPDATED
        "Price_Change"
    ]].head(50)

    with open("fo_signal.json", "w") as f:
        json.dump(summary.to_dict(orient="records"), f, indent=2)

def main():
    print("Fetching latest NSE F&O bhavcopy…")
    zip_data = download_latest_bhavcopy()
    df = extract_csv(zip_data)
    print("Rows:", len(df))

    print("Computing signals…")
    df = compute_signals(df)

    print("Saving CSV + JSON…")
    save_outputs(df)

if __name__ == "__main__":
    main()
    
