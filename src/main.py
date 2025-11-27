import pandas as pd
import json
from fetch_bhavcopy import fetch_bhavcopy

def compute_signals(df):
    """
    Compute long/short buildup from bhavcopy CSV dataframe.
    CSV contains: SYMBOL, EXPIRY_DT, OPTION_TYP, STRIKE_PR, OPEN_INT, CHG_IN_OI, VOL, SETTLE_PR, etc.
    """

    # Ensure columns exist
    required = ["SYMBOL", "EXPIRY_DT", "OPTION_TYP", "STRIKE_PR", "OPEN_INT", "CHG_IN_OI", "VOL", "SETTLE_PR"]
    for col in required:
        if col not in df.columns:
            print("Missing column:", col)
            return {}

    df["SIGNAL"] = ""

    for i in df.index:
        oi = df.at[i, "OPEN_INT"]
        change = df.at[i, "CHG_IN_OI"]
        volume = df.at[i, "VOL"]

        # Long Buildup
        if change > 0 and volume > 0:
            df.at[i, "SIGNAL"] = "Long Buildup"

        # Short Buildup
        if change < 0 and volume > 0:
            df.at[i, "SIGNAL"] = "Short Buildup"

        # Long Unwinding
        if change < 0 and volume < 0:
            df.at[i, "SIGNAL"] = "Long Unwinding"

        # Short Covering
        if change > 0 and volume < 0:
            df.at[i, "SIGNAL"] = "Short Covering"

    return df


def save_outputs(df):
    df.to_csv("fo_latest.csv", index=False)

    j = df[["SYMBOL", "EXPIRY_DT", "OPTION_TYP", "STRIKE_PR", "OPEN_INT", "CHG_IN_OI", "VOL", "SIGNAL"]].to_dict(orient="records")
    
    with open("fo_signals.json", "w") as f:
        json.dump(j, f, indent=2)


def main():
    print("Fetching latest NSE F&O bhavcopy…")
    df = fetch_bhavcopy()

    print("Computing signals…")
    df = compute_signals(df)

    print("Saving CSV + JSON…")
    save_outputs(df)

    print("Done! Files created:")
    print(" - fo_latest.csv")
    print(" - fo_signals.json")


if __name__ == "__main__":
    main()
  
