from src.fetch_bhavcopy import fetch_dat
from src.parse_dat_bhav import parse_dat_bytes
from src.signals_advanced import generate_signals_advanced
from src.utils import ensure_dir
import json

def main():
    print("Fetching DAT bhavcopy...")
    raw = fetch_dat()

    print("Parsing...")
    df = parse_dat_bytes(raw)

    fut = df[df["INSTRUMENT"].isin(["FUTIDX", "FUTSTK"])]

    print("Running advanced model...")
    signals = generate_signals_advanced(fut)

    ensure_dir("data/signals")

    with open("data/signals/latest_signals.json", "w") as f:
        json.dump(signals, f, indent=2)

    print("Saved → data/signals/latest_signals.json")

if __name__ == "__main__":
    main()
  
