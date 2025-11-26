def compute_features(df):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("compute_features(): input is NOT a DataFrame")

    df = df.copy()
    import pandas as pd

def compute_features(df):
    """
    Compute features for futures contracts.
    Returns a clean Pandas DataFrame with additional columns.
    """

    df = df.copy()

    # Convert important columns to numeric (safe conversion)
    numeric_cols = ["OPEN", "HIGH", "LOW", "CLOSE", "SETTLE_PR"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter only futures (FUTIDX, FUTSTK)
    df = df[df["INSTRUMENT"].str.contains("FUT", na=False)]

    # Basic price action features
    df["range"] = df["HIGH"] - df["LOW"]
    df["body"] = df["CLOSE"] - df["OPEN"]
    df["upper_wick"] = df["HIGH"] - df[["CLOSE", "OPEN"]].max(axis=1)
    df["lower_wick"] = df[["CLOSE", "OPEN"]].min(axis=1) - df["LOW"]

    # Momentum (1-period difference)
    df["momentum"] = df["CLOSE"] - df["CLOSE"].shift(1)

    # Remove rows with no numeric data
    df = df.dropna(subset=["OPEN", "HIGH", "LOW", "CLOSE"])

    return df
    
