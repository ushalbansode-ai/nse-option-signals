from fetch_bhavcopy import fetch_bhavcopy
from parser_engine import parse_csv
from compare_engine import compare_signals
from dashboard import build_dashboard
import os


def main():

    print("[STEP] Fetching bhavcopy…")
    csv_today = fetch_bhavcopy()     # ← csv_today created here

    # -------------------------------------------------
    # If no fresh bhavcopy available
    # -------------------------------------------------
    if csv_today is None:
        print("[WARNING] No new bhavcopy downloaded.")
        print("[INFO] Checking last available CSV…")

        raw_files = [
            f for f in os.listdir("data/raw") if f.endswith(".csv")
        ]

        if not raw_files:
            print("[FATAL] No CSV files exist in data/raw/. Cannot continue.")
            return

        csv_today = sorted(raw_files)[-1]
        print(f"[INFO] Using previous CSV: {csv_today}")

    print(f"[INFO] Processing CSV: {csv_today}")

    # -------------------------------------------------
    # Parse & compute signals
    # -------------------------------------------------
    df_today = parse_csv(csv_today)

    df_signals = compare_signals(df_today)

    # -------------------------------------------------
    # Build dashboard
    # -------------------------------------------------
    build_dashboard(df_signals)

    print("[DONE] Signals + Dashboard updated.")


if __name__ == "__main__":
    main()
    
