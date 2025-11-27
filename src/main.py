#!/usr/bin/env python3
import os
import json
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

BHAV_URL = "https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_{0}_{1}_{2}_{3}.csv.zip"

# ----------------------------------------------------------------------------
# Column Mapping — Handles NSE Bhavcopy Variants
# ----------------------------------------------------------------------------
COLUMN_MAP = {
    "last": ["LastPric", "LastPrice", "last"],
    "prev_close": ["PrvsClsgPric", "PrevClose", "prev_close"],
    "oi": ["OpnIntrst", "OpenInterest", "oi"],
    "coi": ["ChngInOpnIntrst", "ChangeInOI", "coi"],
    "volume": ["TtlTradgVol", "Volume", "total_volume"],
}

def find_column(df, keys):
    """Return the first matching column from list"""
    for k in keys:
        if k in df.columns:
            return k
    raise KeyError(f"Required column missing. Tried: {keys}")


# ----------------------------------------------------------------------------
# Download Latest Working Bhavcopy
# ----------------------------------------------------------------------------
def fetch_latest_bhavcopy():
    today = datetime.now()
    for i in range(2):
        d = today - timedelta(days=i)
        y, m, d2 = d.year, f"{d.month:02d}", f"{d.day:02d}"

        url = BHAV_URL.format("0", "0", "0", f"{y}{m}{d2}")
        print(f"Trying: {url}")

        r = requests.get(url)
        if r.status_code != 200:
            print("Failed:", r.status_code)
            continue

        zip_path = f"{DATA_DIR}/bhavcopy.zip"
        open(zip_path, "wb").write(r.content)

        with zipfile.ZipFile(zip_path, "r") as z:
            csv_name = z.namelist()[0]
            print("Extracting:", csv_name)
            z.extract(csv_name, DATA_DIR)

        csv_path = f"{DATA_DIR}/{csv_name}"
        print("Reading:", csv_path)
        df = pd.read_csv(csv_path)
        print("Rows:", len(df))
        return df

    raise Exception("No bhavcopy found for last 2 days")


# ----------------------------------------------------------------------------
# Signal Computation
# ----------------------------------------------------------------------------
def compute_signals(df):

    # find correct bhavcopy column names
    col_last = find_column(df, COLUMN_MAP["last"])
    col_prev = find_column(df, COLUMN_MAP["prev_close"])
    col_oi = find_column(df, COLUMN_MAP["oi"])
    col_coi = find_column(df, COLUMN_MAP["coi"])
    col_vol = find_column(df, COLUMN_MAP["volume"])

    # Rename to consistent names
    df = df.rename(columns={
        col_last: "last",
        col_prev: "prev_close",
        col_oi: "oi",
        col_coi: "coi",
        col_vol: "volume"
    })

    # -----------------------------  
    # Compute Signals  
    # -----------------------------

    # 1) Momentum %
    df["momentum"] = ((df["last"] - df["prev_close"]) / df["prev_close"]) * 100

    # 2) OI Spike
    df["oi_spike"] = df["coi"] / df["oi"].replace(0, 1)

    # 3) Volume Spike
    df["vol_spike"] = (df["volume"] / df["volume"].rolling(20).mean()).fillna(1)

    # 4) Reversal Detection
    df["reversal"] = (
        (df["momentum"].shift(1) > 0) & (df["momentum"] < 0)
    ) | (
        (df["momentum"].shift(1) < 0) & (df["momentum"] > 0)
    )

    return df


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
def main():
    print("Fetching latest bhavcopy...")
    df = fetch_latest_bhavcopy()

    print(df.columns.tolist())  # Debug (remove later)

    print("Computing signals...")
    df_sig = compute_signals(df)

    out_path = f"{DATA_DIR}/signals.json"
    df_sig.to_json(out_path, orient="records")
    print("Saved:", out_path)


if __name__ == "__main__":
    main()
    
