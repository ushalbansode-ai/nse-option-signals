from src.fetch_bhavcopy import fetch_bhavcopy
from src.compute_features import compute_features
from src.generate_signals import generate_signals

def main():
    print("Fetching NSE Futures DAT bhavcopy...")
    raw_df = fetch_bhavcopy()

    print("Computing features...")
    feat_df = compute_features(raw_df)

    print("Generating signals...")
    signals = generate_signals(feat_df)
    signals.to_csv("signals.csv", index=False)

    print("✔ signals.csv updated!")

if __name__ == "__main__":
    main()
    
