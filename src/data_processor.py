"""
Data processing module for futures and options data
"""

import pandas as pd
import numpy as np
import re
from collections import defaultdict

class DataProcessor:
    def __init__(self):
        pass
    
    def load_data(self, csv_path):
        """Load CSV data into DataFrame"""
        print(f"Loading data from: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def parse_instrument_symbol(self, symbol):
        """Parse instrument symbol to extract underlying, expiry, type, strike"""
        symbol_str = str(symbol).strip()
        
        # Futures
        if symbol_str.endswith('FUT'):
            return {
                'instrument_type': 'FUT',
                'underlying': symbol_str.replace('FUT', '').strip(),
                'strike': None,
                'option_type': None
            }
        
        # Options - handle CE/PE with strike price
        option_pattern = r'^([A-Z]+)(\d{2}[A-Z]{3}\d{2,4})(\d+)(CE|PE)$'
        match = re.match(option_pattern, symbol_str)
        
        if match:
            underlying = match.group(1)
            expiry_str = match.group(2)
            strike = float(match.group(3))
            option_type = match.group(4)
            
            return {
                'instrument_type': 'OPT',
                'underlying': underlying,
                'strike': strike,
                'option_type': option_type,
                'expiry_str': expiry_str
            }
        
        return {'instrument_type': 'UNKNOWN', 'underlying': symbol_str, 'strike': None, 'option_type': None}
    
    def separate_futures_options(self, df):
        """Separate futures and options data with proper parsing"""
        futures_data = []
        options_data = []
        
        print("Parsing instrument symbols...")
        
        for _, row in df.iterrows():
            parsed = self.parse_instrument_symbol(row['TckrSymb'])
            
            if parsed['instrument_type'] == 'FUT':
                futures_data.append({**row.to_dict(), **parsed})
            elif parsed['instrument_type'] == 'OPT':
                options_data.append({**row.to_dict(), **parsed})
        
        futures_df = pd.DataFrame(futures_data)
        options_df = pd.DataFrame(options_data)
        
        print(f"Futures contracts found: {len(futures_df)}")
        print(f"Options contracts found: {len(options_df)}")
        
        if len(options_df) > 0:
            print(f"Option types: {options_df['option_type'].value_counts().to_dict()}")
            print(f"Unique underlyings in options: {options_df['underlying'].nunique()}")
        
        return futures_df, options_df
    
    def create_option_chain(self, options_df, futures_df):
        """Create option chain structure for analysis"""
        print("Building option chain...")
        
        option_chain = defaultdict(lambda: defaultdict(dict))
        underlying_prices = {}
        
        # Get current prices from futures for each underlying
        for _, future in futures_df.iterrows():
            underlying_prices[future['underlying']] = future['LastPric']
        
        # Organize options by underlying, expiry, strike, and type
        for _, option in options_df.iterrows():
            underlying = option['underlying']
            strike = option['strike']
            option_type = option['option_type']
            expiry = option.get('expiry_str', 'UNKNOWN')
            
            if underlying not in underlying_prices:
                continue
                
            current_price = underlying_prices[underlying]
            
            option_chain[underlying][expiry][(strike, option_type)] = {
                'last_price': option['LastPric'],
                'oi': option['OpnIntrst'],
                'oi_change': option['ChngInOpnIntrst'],
                'volume': option['TtlTradgVol'],
                'prev_close': option['PrvsClsgPric'],
                'distance_from_spot': abs(current_price - strike),
                'is_ATM': abs(current_price - strike) / current_price < 0.02,
                'is_ITM': (option_type == 'CE' and strike < current_price) or 
                         (option_type == 'PE' and strike > current_price)
            }
        
        print(f"Option chain created for {len(option_chain)} underlyings")
        return option_chain, underlying_prices
