import os
from utils import ensure_folder, load_csv_safely, save_csv_safely
from fetch_bhavcopy import download_bhavcopy
from compare_engine import compare_with_previous
from signal_engine import generate_signals
from build_dashboard import build_dashboard


RAW = "data/raw"
OUT = "data/out"
PREV = "data/previous.csv"
FINAL = "data/signals.csv"
DASH = "dashboard/index.html"


def main():
    ensure_folder(RAW)
    ensure_folder("dashboard")

    print("📥 Step 1 — Download NSE FO Bhavcopy")
    csv_path = download_bhavcopy(RAW)
    if csv_path is None:
        print("❌ No bhavcopy downloaded → stopping")
        return

    print("📂 Step 2 — Load latest & previous data")
    latest_df = load_csv_safely(csv_path)
    previous_df = load_csv_safely(PREV)

    print("🔍 Step 3 — Comparison engine")
    compared = compare_with_previous(latest_df, previous_df)

    print("📈 Step 4 — Signal generator")
    signals = generate_signals(compared)

    print("💾 Step 5 — Save outputs")
    save_csv_safely(signals, FINAL)
    save_csv_safely(latest_df, PREV)

    print("🌐 Step 6 — Build dashboard")
    build_dashboard(signals, DASH)

    print("🎉 DONE")


if __name__ == "__main__":
    main()
    
