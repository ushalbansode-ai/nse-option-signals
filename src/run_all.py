import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from fetch_bhavcopy import fetch_bhavcopy
from utils import get_latest_two_files, load_csv
from compare_engine import detect_signals
from signal_engine import save_signals
from scripts.build_dashboard import build_dashboard


def main():

    print("[STEP] Fetching bhavcopy…")
    csv_today = fetch_bhavcopy()

    prev_file, today_file = get_latest_two_files()

    if prev_file is None:
        print("[WARN] Not enough files to compare yet.")
        return

    print("[STEP] Loading CSV files…")
    prev_df = load_csv("data/raw/" + prev_file)
    today_df = load_csv("data/raw/" + today_file)

    print("[STEP] Detecting signals…")
    signals = detect_signals(prev_df, today_df)

    print("[STEP] Saving signals…")
    save_signals(signals)

    print("[STEP] Building dashboard…")
    build_dashboard()

    print("[DONE] Run complete.")
    

if __name__ == "__main__":
    main()
    
