#!/usr/bin/env python3
"""
Debug script to check historical data
"""

import os
import glob
from datetime import datetime

def check_historical_data():
    print("🔍 DEBUG: Checking historical data directory...")
    
    processed_dir = "data/processed"
    
    if not os.path.exists(processed_dir):
        print(f"❌ Directory '{processed_dir}' does not exist!")
        return
    
    print(f"📁 Directory exists: {processed_dir}")
    
    # List all files
    all_files = os.listdir(processed_dir)
    print(f"\n📄 All files in directory:")
    for file in sorted(all_files):
        file_path = os.path.join(processed_dir, file)
        size = os.path.getsize(file_path) if os.path.isfile(file_path) else "DIR"
        modified = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M')
        print(f"  - {file} ({size} bytes, modified: {modified})")
    
    # Check CSV files
    csv_files = glob.glob(os.path.join(processed_dir, "*.csv"))
    print(f"\n📊 CSV files found: {len(csv_files)}")
    
    for csv_file in sorted(csv_files):
        filename = os.path.basename(csv_file)
        size_mb = os.path.getsize(csv_file) / (1024 * 1024)
        modified = datetime.fromtimestamp(os.path.getmtime(csv_file)).strftime('%Y-%m-%d %H:%M')
        
        print(f"\n📁 File: {filename}")
        print(f"   Size: {size_mb:.2f} MB")
        print(f"   Modified: {modified}")
        
        try:
            import pandas as pd
            df = pd.read_csv(csv_file)
            print(f"   Rows: {len(df):,}")
            print(f"   Columns: {len(df.columns)}")
            
            if 'instrument_type' in df.columns:
                futures_count = (df['instrument_type'] == 'futures').sum()
                options_count = (df['instrument_type'] == 'options').sum()
                print(f"   Futures: {futures_count:,}")
                print(f"   Options: {options_count:,}")
                
                # Check date
                if 'TradDt' in df.columns and len(df) > 0:
                    print(f"   Trading Date in data: {df['TradDt'].iloc[0]}")
            
        except Exception as e:
            print(f"   ❌ Error reading CSV: {e}")

if __name__ == "__main__":
    check_historical_data()
