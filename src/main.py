import os
import requests
import zipfile
import datetime
import pandas as pd
import numpy as np

BASE_DIR = "data"
OUT_DIR = "data/signals"

# Ensure folders exist - FIXED VERSION
for p in [BASE_DIR, OUT_DIR]:
    if os.path.exists(p) and not os.path.isdir(p):
        # If it's a file, remove it first
        os.remove(p)
    os.makedirs(p, exist_ok=True)


# -----------------------------------------------------------
# 1) Download latest Bhavcopy (NEW NSE FORMAT)
# -----------------------------------------------------------

def fetch_latest_bhavcopy():
    print("Fetching latest bhavcopy...")

    base_url = (
        "https://archives.nseindia.com/content/fo/"
        "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
    )

    today = datetime.date.today()
    tried = []

    # Try last 4 days
    for i in range(0, 4):
        d = today - datetime.timedelta(days=i)
        date_str = d.strftime("%Y%m%d")

        url = base_url.format(date=date_str)
        tried.append(url)

        print("Trying:", url)

        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})

        if r.status_code == 200:
            zip_path = f"{BASE_DIR}/Bhav_{date_str}.zip"
            open(zip_path, "wb").write(r.content)
            print("Downloaded:", url)

            # Extract ZIP
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(BASE_DIR)
                extracted_file = z.namelist()[0]

            print("Extracted:", extracted_file)

            return f"{BASE_DIR}/{extracted_file}"

        else:
            print("Failed:", r.status_code)

    print("Tried URLs:")
    for u in tried:
        print(" -", u)

    raise Exception("No bhavcopy found for last 4 days")


# -----------------------------------------------------------
# 2) Compute Option Buying Opportunities from Futures Data
# -----------------------------------------------------------

def identify_option_opportunities(df):
    """
    Identify option buying opportunities based on futures data signals
    """
    
    # Filter only futures data (ending with 'FUT')
    futures_df = df[df['TckrSymb'].str.endswith('FUT', na=False)].copy()
    
    print(f"Found {len(futures_df)} futures contracts")
    
    if len(futures_df) == 0:
        return pd.DataFrame()
    
    # Calculate technical indicators for futures
    futures_df['momentum'] = futures_df['LastPric'] - futures_df['PrvsClsgPric']
    futures_df['oi_trend'] = futures_df['ChngInOpnIntrst']
    futures_df['vol_spike'] = futures_df['TtlTradgVol'].pct_change().fillna(0)
    
    # Strong bullish signals (for CALL options)
    bullish_conditions = (
        (futures_df['momentum'] > 0) &
        (futures_df['oi_trend'] > 0) &
        (futures_df['vol_spike'] > 0.25)
    )
    
    # Strong bearish signals (for PUT options)  
    bearish_conditions = (
        (futures_df['momentum'] < 0) &
        (futures_df['oi_trend'] < 0) &
        (futures_df['vol_spike'] > 0.25)
    )
    
    # Extract underlying symbol from futures (remove 'FUT' suffix)
    futures_df['underlying'] = futures_df['TckrSymb'].str.replace('FUT', '')
    
    # Get current price for strike selection
    futures_df['current_price'] = futures_df['LastPric']
    
    # Create opportunities dataframe
    opportunities = []
    
    # Bullish opportunities - BUY CALL options
    bullish_futures = futures_df[bullish_conditions]
    for _, future in bullish_futures.iterrows():
        opportunities.append({
            'underlying': future['underlying'],
            'future_price': future['LastPric'],
            'momentum': future['momentum'],
            'oi_change': future['oi_trend'],
            'volume_spike': future['vol_spike'],
            'recommendation': 'BUY CALL',
            'reason': 'Strong bullish momentum with OI increase and volume spike',
            'suggested_strikes': f"ATM to OTM (Current: {future['LastPric']:.2f})",
            'expiry_priority': 'Current month',
            'risk_level': 'Medium',
            'signal_strength': 'Strong'
        })
    
    # Bearish opportunities - BUY PUT options
    bearish_futures = futures_df[bearish_conditions]
    for _, future in bearish_futures.iterrows():
        opportunities.append({
            'underlying': future['underlying'],
            'future_price': future['LastPric'],
            'momentum': future['momentum'],
            'oi_change': future['oi_trend'],
            'volume_spike': future['vol_spike'],
            'recommendation': 'BUY PUT',
            'reason': 'Strong bearish momentum with OI decrease and volume spike',
            'suggested_strikes': f"ATM to OTM (Current: {future['LastPric']:.2f})",
            'expiry_priority': 'Current month',
            'risk_level': 'Medium',
            'signal_strength': 'Strong'
        })
    
    # Moderate opportunities (weaker signals)
    moderate_bullish = (
        (futures_df['momentum'] > 0) &
        (futures_df['oi_trend'] > 0) &
        (futures_df['vol_spike'].between(0.1, 0.25))
    )
    
    moderate_bearish = (
        (futures_df['momentum'] < 0) &
        (futures_df['oi_trend'] < 0) &
        (futures_df['vol_spike'].between(0.1, 0.25))
    )
    
    # Add moderate bullish opportunities
    mod_bullish_futures = futures_df[moderate_bullish]
    for _, future in mod_bullish_futures.iterrows():
        opportunities.append({
            'underlying': future['underlying'],
            'future_price': future['LastPric'],
            'momentum': future['momentum'],
            'oi_change': future['oi_trend'],
            'volume_spike': future['vol_spike'],
            'recommendation': 'BUY CALL',
            'reason': 'Moderate bullish momentum with OI increase',
            'suggested_strikes': f"ATM (Current: {future['LastPric']:.2f})",
            'expiry_priority': 'Current month',
            'risk_level': 'High',
            'signal_strength': 'Moderate'
        })
    
    # Add moderate bearish opportunities
    mod_bearish_futures = futures_df[moderate_bearish]
    for _, future in mod_bearish_futures.iterrows():
        opportunities.append({
            'underlying': future['underlying'],
            'future_price': future['LastPric'],
            'momentum': future['momentum'],
            'oi_change': future['oi_trend'],
            'volume_spike': future['vol_spike'],
            'recommendation': 'BUY PUT',
            'reason': 'Moderate bearish momentum with OI decrease',
            'suggested_strikes': f"ATM (Current: {future['LastPric']:.2f})",
            'expiry_priority': 'Current month',
            'risk_level': 'High',
            'signal_strength': 'Moderate'
        })
    
    return pd.DataFrame(opportunities)


# -----------------------------------------------------------
# 3) Generate Detailed Analysis Report
# -----------------------------------------------------------

def generate_analysis_report(opportunities_df, original_df):
    """
    Generate a detailed analysis report with additional insights
    """
    if len(opportunities_df) == 0:
        return "No strong option buying opportunities identified today."
    
    report = []
    report.append("OPTION BUYING OPPORTUNITIES REPORT")
    report.append("=" * 50)
    report.append(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total opportunities found: {len(opportunities_df)}")
    report.append("")
    
    # Group by recommendation type
    call_opportunities = opportunities_df[opportunities_df['recommendation'] == 'BUY CALL']
    put_opportunities = opportunities_df[opportunities_df['recommendation'] == 'BUY PUT']
    
    report.append(f"CALL Buying Opportunities: {len(call_opportunities)}")
    report.append(f"PUT Buying Opportunities: {len(put_opportunities)}")
    report.append("")
    
    # Strong signals first
    strong_signals = opportunities_df[opportunities_df['signal_strength'] == 'Strong']
    if len(strong_signals) > 0:
        report.append("🔥 STRONG SIGNALS (Higher Confidence):")
        report.append("-" * 40)
        for _, opp in strong_signals.iterrows():
            report.append(f"• {opp['underlying']}: {opp['recommendation']}")
            report.append(f"  Price: {opp['future_price']:.2f} | Momentum: {opp['momentum']:.2f}")
            report.append(f"  OI Change: {opp['oi_change']:+.0f} | Volume Spike: {opp['volume_spike']:.1%}")
            report.append(f"  Reason: {opp['reason']}")
            report.append(f"  Suggested: {opp['suggested_strikes']}")
            report.append("")
    
    # Moderate signals
    moderate_signals = opportunities_df[opportunities_df['signal_strength'] == 'Moderate']
    if len(moderate_signals) > 0:
        report.append("⚠️ MODERATE SIGNALS (Lower Confidence):")
        report.append("-" * 40)
        for _, opp in moderate_signals.iterrows():
            report.append(f"• {opp['underlying']}: {opp['recommendation']}")
            report.append(f"  Price: {opp['future_price']:.2f} | Momentum: {opp['momentum']:.2f}")
            report.append(f"  OI Change: {opp['oi_change']:+.0f} | Volume Spike: {opp['volume_spike']:.1%}")
            report.append(f"  Reason: {opp['reason']}")
            report.append(f"  Suggested: {opp['suggested_strikes']}")
            report.append("")
    
    # Additional market insights
    total_futures = len(original_df[original_df['TckrSymb'].str.endswith('FUT', na=False)])
    total_options = len(original_df) - total_futures
    
    report.append("MARKET OVERVIEW:")
    report.append("-" * 40)
    report.append(f"Total Futures Contracts: {total_futures}")
    report.append(f"Total Options Contracts: {total_options}")
    report.append(f"Opportunity Ratio: {len(opportunities_df)/total_futures:.1%} of futures show strong signals")
    
    return "\n".join(report)


# -----------------------------------------------------------
# 4) MAIN
# -----------------------------------------------------------

def main():
    print("Fetching latest bhavcopy...")

    csv_path = fetch_latest_bhavcopy()

    print("Reading:", csv_path)
    df = pd.read_csv(csv_path)

    print("Rows:", len(df))
    print("Columns:", list(df.columns))

    print("Identifying option buying opportunities...")
    opportunities_df = identify_option_opportunities(df)
    
    # Save opportunities to CSV
    if len(opportunities_df) > 0:
        opportunities_csv = f"{OUT_DIR}/option_opportunities_{datetime.date.today()}.csv"
        opportunities_df.to_csv(opportunities_csv, index=False)
        print(f"Saved opportunities: {opportunities_csv}")
        
        # Generate and save analysis report
        report = generate_analysis_report(opportunities_df, df)
        report_file = f"{OUT_DIR}/option_analysis_report_{datetime.date.today()}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        print(f"Saved analysis report: {report_file}")
        
        # Print report to console
        print("\n" + "="*60)
        print(report)
        print("="*60)
    else:
        print("No option buying opportunities identified today.")
        
        # Save empty report for tracking
        report_file = f"{OUT_DIR}/option_analysis_report_{datetime.date.today()}.txt"
        with open(report_file, 'w') as f:
            f.write("No option buying opportunities identified today.")
        print(f"Saved empty report: {report_file}")


# -----------------------------------------------------------
# Run
# -----------------------------------------------------------
if __name__ == "__main__":
    main()
