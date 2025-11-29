"""
Enhanced Data Processor for NSE Futures & Options Analysis
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR, NSE_COLUMN_MAPPING

class DataProcessor:
    def __init__(self):
        self.raw_data_dir = RAW_DATA_DIR
        self.processed_data_dir = PROCESSED_DATA_DIR
    
    def process_bhavcopy_data(self, csv_file_path):
        """Process NSE bhavcopy data and separate futures/options"""
        print(f"    Loading data from: {csv_file_path}")
        
        try:
            # Load the CSV file
            df = pd.read_csv(csv_file_path)
            print(f"    Loaded {len(df)} rows, {len(df.columns)} columns")
            
            # Display data structure
            print("    NSE BHAVCOPY DATA STRUCTURE:")
            print(f"    Shape: {df.shape}")
            print(f"    Columns: {list(df.columns)}")
            
            # Identify instrument types
            if 'FinInstrmTp' in df.columns:
                instrument_col = 'FinInstrmTp'
            elif 'FinInstrmFp' in df.columns:
                instrument_col = 'FinInstrmFp'
            else:
                # Try to find instrument column
                instrument_cols = [col for col in df.columns if 'fin' in col.lower() and 'instrm' in col.lower()]
                if instrument_cols:
                    instrument_col = instrument_cols[0]
                else:
                    print("    ❌ Could not find instrument type column")
                    return None, None
            
            print(f"    Using instrument column: {instrument_col}")
            unique_instruments = df[instrument_col].unique()
            print(f"    Unique instruments: {list(unique_instruments)}")
            
            # Separate futures and options
            futures_df = self.extract_futures_data(df, instrument_col)
            options_df = self.extract_options_data(df, instrument_col)
            
            print(f"    Final count - Futures: {len(futures_df)}, Options: {len(options_df)}")
            
            return futures_df, options_df
            
        except Exception as e:
            print(f"    ❌ Error processing data: {e}")
            return None, None
    
    def extract_futures_data(self, df, instrument_col):
        """Extract futures data from bhavcopy"""
        # Identify futures contracts (typically 'FUTSTK' or similar)
        futures_keywords = ['FUT', 'FUTSTK', 'FUTIDX', 'STF', 'IDF']
        
        futures_mask = df[instrument_col].str.contains('|'.join(futures_keywords), case=False, na=False)
        futures_df = df[futures_mask].copy()
        
        if len(futures_df) > 0:
            print(f"    Found {len(futures_df)} futures contracts")
            print(f"    Futures sample: {list(futures_df['TckrSymb'].head().values)}")
            
            # Standardize column names
            futures_df = self.standardize_column_names(futures_df, 'futures')
        else:
            print("    ❌ No futures contracts found")
            
        return futures_df
    
    def extract_options_data(self, df, instrument_col):
        """Extract options data from bhavcopy"""
        # Identify options contracts (typically 'OPT' or similar)
        options_keywords = ['OPT', 'OPTSTK', 'OPTIDX', 'STO', 'IDO']
        
        options_mask = df[instrument_col].str.contains('|'.join(options_keywords), case=False, na=False)
        options_df = df[options_mask].copy()
        
        if len(options_df) > 0:
            print(f"    Found {len(options_df)} options contracts")
            print(f"    Options sample: {list(options_df['TckrSymb'].head().values)}")
            
            # Standardize column names
            options_df = self.standardize_column_names(options_df, 'options')
        else:
            print("    ❌ No options contracts found")
            
        return options_df
    
    def standardize_column_names(self, df, data_type):
        """Standardize column names for analysis"""
        print(f"    Using NSE Bhavcopy column structure...")
        print(f"    {data_type.capitalize()} columns: {list(df.columns)}")
        
        # Get the appropriate column mapping
        mapping = NSE_COLUMN_MAPPING.get(data_type, {})
        
        # Create a copy to avoid warnings
        df_standardized = df.copy()
        
        # Rename columns
        for old_name, new_name in mapping.items():
            if old_name in df_standardized.columns:
                df_standardized = df_standardized.rename(columns={old_name: new_name})
                print(f"    Mapped '{old_name}' -> '{new_name}'")
        
        print(f"    Standardized column names for {data_type}")
        return df_standardized
    
    def load_previous_data(self):
        """Load previous trading day data for comparison"""
        print("    Looking for previous trading day data...")
        
        # Get list of processed data files
        processed_files = []
        if os.path.exists(self.processed_data_dir):
            processed_files = [f for f in os.listdir(self.processed_data_dir) if f.endswith('.csv')]
        
        if not processed_files:
            print("    No previous trading data found")
            print("    PREVIOUS DAY DATA: Not available (first run or weekend)")
            return None, None, None
        
        # Sort files by date and get the most recent one
        processed_files.sort(reverse=True)
        latest_file = processed_files[0]
        
        try:
            # Load the latest processed data
            latest_path = os.path.join(self.processed_data_dir, latest_file)
            combined_data = pd.read_csv(latest_path)
            
            # Extract date from filename
            file_date = latest_file.replace('nse_fo_', '').replace('.csv', '')
            
            # Separate futures and options
            previous_futures = combined_data[combined_data['instrument_type'] == 'futures'].copy()
            previous_options = combined_data[combined_data['instrument_type'] == 'options'].copy()
            
            print(f"    Loaded previous data from: {file_date}")
            print(f"    Previous futures: {len(previous_futures)}, options: {len(previous_options)}")
            
            return previous_futures, previous_options, file_date
            
        except Exception as e:
            print(f"    ❌ Error loading previous data: {e}")
            return None, None, None
    
    def save_current_data(self, futures_data, options_data):
        """Save current day data for future historical analysis"""
        if futures_data is None or options_data is None:
            print("    ❌ No data to save")
            return
        
        try:
            # Add instrument type column
            futures_data['instrument_type'] = 'futures'
            options_data['instrument_type'] = 'options'
            
            # Combine data
            combined_data = pd.concat([futures_data, options_data], ignore_index=True)
            
            # Generate filename with current date
            today = datetime.now().strftime('%Y%m%d')
            filename = f"nse_fo_{today}.csv"
            filepath = os.path.join(self.processed_data_dir, filename)
            
            # Save as CSV
            combined_data.to_csv(filepath, index=False)
            print(f"    Saved historical data for {today}")
            
        except Exception as e:
            print(f"    ❌ Error saving current data: {e}")
