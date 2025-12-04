from fetch_bhavcopy import fetch_bhavcopy
from utils import get_latest_two_files, load_csv
from compare_engine import detect_signals
from signal_engine import save_signals
from scripts.build_dashboard import build_dashboard

def main():

    # 1. Download bhavcopy
    csv_today = fetch_bhavcopy()

    # 2. Compare with previous day
    prev_file, today_file = get_latest_two_files()
    if prev_file is None:
        print("Not enough files to compare.")
        return

    prev_df = load_csv("data/raw/" + prev_file)
    today_df = load_csv("data/raw/" + today_file)

    # 3. Detect signals
    signals = detect_signals(prev_df, today_df)
    save_signals(signals)

    # 4. Build dashboard
    build_dashboard()


if __name__ == "__main__":
    main()
  
