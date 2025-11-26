from src.fetch_bhavcopy import fetch_bhavcopy
from src.compute_metrics import compute_metrics
from src.generate_signals import generate_signals

def main():
    print("Fetching NSE Futures DAT bhavcopy…")
    df = fetch_bhavcopy()   # auto-scan DAT

    print("Computing metrics…")
    computed = compute_metrics(df)

    print("Generating signals…")
    signals = generate_signals(computed)

    print("Saving output…")
    computed.to_csv("latest.csv", index=False)
    signals.to_csv("signal.csv", index=False)

    print("Done.")

if __name__ == "__main__":
    main()
    
