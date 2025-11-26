import numpy as np
from .features_engine import add_technical_features

def generate_signals_advanced(df):
    df = df.copy()
    df = add_technical_features(df)

    signals = []

    for _, row in df.iterrows():
        score = 0

        # Momentum
        if row["momentum3"] > 0:
            score += 1

        # VWAP premium
        if row["CLOSE"] > row["vwap"]:
            score += 1

        # Volume spike
        if row["vol_spike"] > 2:
            score += 1

        # Reversal pattern
        if (row["CLOSE"] > row["OPEN"]) and (row["LOW"] == row["LOW"]):
            score += 1

        if score >= 3:
            signals.append({
                "symbol": row["SYMBOL"],
                "expiry": row["EXPIRY_DT"],
                "close": row["CLOSE"],
                "score": score
            })

    return sorted(signals, key=lambda x: x["score"], reverse=True)
  
