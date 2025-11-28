#!/usr/bin/env python3
"""
Main entry point for NSE Option Buying Strategy Analysis
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetcher import DataFetcher
from src.data_processor import DataProcessor
from src.strategy_engine import OptionBuyingStrategy
from src.report_generator import ReportGenerator
from config.settings import BASE_DIR, OUT_DIR

def main():
    """Main execution function"""
    print("🚀 NSE Option Buying Strategy Analysis Starting...")
    
    try:
        # Initialize components
        fetcher = DataFetcher()
        processor = DataProcessor()
        strategy = OptionBuyingStrategy()
        reporter = ReportGenerator()
        
        # Step 1: Fetch latest data
        print("\n📥 Step 1: Fetching latest bhavcopy...")
        csv_path = fetcher.fetch_latest_bhavcopy()
        
        # Step 2: Process data
        print("\n🔧 Step 2: Processing data...")
        df = processor.load_data(csv_path)
        futures_df, options_df = processor.separate_futures_options(df)
        option_chain, underlying_prices = processor.create_option_chain(options_df, futures_df)
        
        # Step 3: Run strategies
        print("\n🎯 Step 3: Analyzing option buying opportunities...")
        opportunities_df = strategy.analyze_all_setups(futures_df, option_chain, underlying_prices)
        
        # Step 4: Generate reports
        print("\n📊 Step 4: Generating reports...")
        reporter.generate_all_reports(opportunities_df, futures_df, options_df, option_chain, underlying_prices)
        
        print(f"\n✅ Analysis completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
