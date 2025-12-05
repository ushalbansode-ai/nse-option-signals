import pandas as pd


def build_dashboard(df, path):
    if df.empty:
        print("⚠️ Dashboard skipped: empty data")
        return

    html = df.to_html(index=False)
    with open(path, "w") as f:
        f.write(html)

    print(f"✅ Dashboard generated → {path}")
  
