import pandas as pd
from src.fetch_bhavcopy import fetch_bhavcopy
from src.compute_features import compute_features
from src.generate_signals import generate_signals

def main():
    print("\nFetching NSE Futures DAT bhavcopy...\n")
    raw_df = fetch_bhavcopy()  # Auto-scan fetcher
    print("Rows fetched:", len(raw_df))

    print("\nComputing features...")
    feat_df = compute_features(raw_df)

    print("\nGenerating signals...")
    sig_df = generate_signals(feat_df)

    # Save
    sig_df.to_json("data/signals/latest_signals.json", orient="records", indent=2)
    print("\nSignals saved to data/signals/latest_signals.json")

    print("\n✓ COMPLETED SUCCESSFULLY ✓")

if __name__ == "__main__":
    main()
    
