#!/usr/bin/env python3
"""
Enhanced Main Entry Point with Previous Day Data Analysis
"""

import sys
import os
import pandas as pd
from datetime import datetime, date, timedelta

from src.data_fetcher import DataFetcher
from src.data_processor import DataProcessor
from src.combined_analyzer import CombinedAnalyzer
from src.report_generator import ReportGenerator
from src.historical_manager import HistoricalManager
from config.settings import BASE_DIR, OUT_DIR

class SmartTradingCalendar:
    def __init__(self):
        self.trading_days = []
        
    def get_smart_analysis_date(self):
        """Smart date selection for analysis"""
        today = datetime.now()
        
        # If today is Monday, use Friday's data
        if today.weekday() == 0:  # Monday
            friday = today - timedelta(days=3)
            print(f"📅 Monday detected. Using Friday's data: {friday.strftime('%Y-%m-%d')}")
            return friday.date()
        
        # If today is Tuesday-Friday, use previous day
        elif today.weekday() in [1, 2, 3, 4]:  # Tue-Fri
            prev_day = today - timedelta(days=1)
            # If previous day was weekend, use Friday
            if prev_day.weekday() >= 5:  # Weekend
                friday = today - timedelta(days=(today.weekday() + 3) % 7)
                print(f"📅 Weekend detected. Using Friday's data: {friday.strftime('%Y-%m-%d')}")
                return friday.date()
            print(f"📅 Using previous day data: {prev_day.strftime('%Y-%m-%d')}")
            return prev_day.date()
        
        else:  # Weekend
            friday = today - timedelta(days=(today.weekday() - 4) % 7)
            print(f"📅 Weekend detected. Using Friday's data: {friday.strftime('%Y-%m-%d')}")
            return friday.date()

class EnhancedDataParser:
    def __init__(self):
        self.futures_keywords = ['FUT', 'FUTSTK', 'FUTIDX']
        self.options_keywords = ['OPT', 'OPTSTK', 'OPTIDX']
    
    def parse_instruments(self, df):
        """Parse futures and options contracts from dataframe"""
        futures_df = pd.DataFrame()
        options_df = pd.DataFrame()
        
        if df.empty:
            return futures_df, options_df
        
        # Try different column names for instrument type
        instrument_columns = ['Instrument', 'INSTRUMENT', 'instrument', 'SECURITY TYPE']
        instrument_col = None
        
        for col in instrument_columns:
            if col in df.columns:
                instrument_col = col
                break
        
        if instrument_col is None:
            print("❌ No instrument column found. Available columns:", df.columns.tolist())
            return futures_df, options_df
        
        print(f"🔍 Using instrument column: {instrument_col}")
        print(f"📊 Unique instruments: {df[instrument_col].unique()}")
        
        # Filter futures
        for keyword in self.futures_keywords:
            mask = df[instrument_col].astype(str).str.contains(keyword, case=False, na=False)
            if mask.any():
                futures_df = pd.concat([futures_df, df[mask]], ignore_index=True)
        
        # Filter options
        for keyword in self.options_keywords:
            mask = df[instrument_col].astype(str).str.contains(keyword, case=False, na=False)
            if mask.any():
                options_df = pd.concat([options_df, df[mask]], ignore_index=True)
        
        # Remove duplicates
        futures_df = futures_df.drop_duplicates()
        options_df = options_df.drop_duplicates()
        
        return futures_df, options_df

def main():
    """Enhanced Main execution with Historical Data Analysis"""
    print("🚀 NSE ENHANCED Combined Futures & Options Analysis Starting...")
    print("   📊 Using PREVIOUS DAY + CURRENT DAY data for accurate signals")
    
    try:
        # Initialize smart calendar and parser
        calendar = SmartTradingCalendar()
        parser = EnhancedDataParser()
        
        # Get smart analysis date
        current_date = calendar.get_smart_analysis_date()
        print(f"📅 ANALYSIS DATE: {current_date}")
        
        # Initialize components
        fetcher = DataFetcher()
        processor = DataProcessor()
        historical_mgr = HistoricalManager()
        analyzer = CombinedAnalyzer(historical_mgr)
        reporter = ReportGenerator()
        
        # Step 1: Fetch latest data for the analysis date
        print("\n📥 Step 1: Fetching bhavcopy...")
        csv_path = fetcher.fetch_latest_bhavcopy()  # This might need modification to accept date
        
        # Step 2: Process current day data with enhanced parsing
        print("\n🔧 Step 2: Processing data...")
        df = processor.load_data(csv_path)
        
        # Use enhanced parser for better instrument detection
        futures_df, options_df = parser.parse_instruments(df)
        
        # Fallback to original processor if enhanced parser finds nothing
        if len(futures_df) == 0 and len(options_df) == 0:
            print("🔄 Using original parser as fallback...")
            futures_df, options_df = processor.separate_futures_options(df)
        
        print(f"✅ Futures contracts found: {len(futures_df)}")
        print(f"✅ Options contracts found: {len(options_df)}")
        
        # Step 3: Load PREVIOUS day data
        print("\n📚 Step 3: Loading PREVIOUS day data for comparison...")
        prev_futures, prev_options, prev_date = historical_mgr.load_previous_data(current_date)
        
        if prev_futures is not None:
            print(f"✅ PREVIOUS DAY DATA: {prev_date} - {len(prev_futures)} futures, {len(prev_options)} options")
            historical_status = "AVAILABLE"
        else:
            print("⚠️ PREVIOUS DAY DATA: Not available (first run or weekend)")
            historical_status = "UNAVAILABLE"
            prev_date = None
        
        # Step 4: Save CURRENT day data for future use
        print("\n💾 Step 4: Saving CURRENT day data for future analysis...")
        historical_mgr.save_daily_data(futures_df, options_df, current_date)
        
        # Step 5: Run ENHANCED COMBINED analysis
        print("\n🎯 Step 5: Running ENHANCED COMBINED ANALYSIS...")
        print("   📅 Comparing PREVIOUS vs CURRENT day data...")
        print("   📈 Analyzing Futures trends with historical context...")
        print("   📊 Confirming with Options activity patterns...")
        print("   🎯 Generating FINAL VERDICT with data quality scores...")
        
        combined_opportunities = analyzer.analyze_combined(futures_df, options_df, prev_futures, prev_options)
        
        # Step 6: Generate comprehensive reports
        print("\n📊 Step 6: Generating enhanced reports...")
        reporter.generate_enhanced_reports(combined_opportunities, futures_df, options_df, current_date, prev_date, historical_status)
        
        # Final summary with historical context
        print(f"\n✅ ENHANCED ANALYSIS COMPLETED SUCCESSFULLY!")
        print(f"📈 Historical Data: {historical_status}")
        
        if len(combined_opportunities) > 0:
            buy_calls = len(combined_opportunities[combined_opportunities['recommendation'].str.contains('CALL')])
            buy_puts = len(combined_opportunities[combined_opportunities['recommendation'].str.contains('PUT')])
            high_conf = len(combined_opportunities[combined_opportunities['confidence'] == 'High'])
            high_quality = len(combined_opportunities[combined_opportunities['data_quality'] == 'HIGH'])
            
            print(f"🎯 FINAL VERDICT: {buy_calls} CALL buys, {buy_puts} PUT buys")
            print(f"   🔥 High Confidence: {high_conf}")
            print(f"   📊 High Quality Data: {high_quality}")
        else:
            print("🎯 FINAL VERDICT: No strong opportunities identified today")
            if historical_status == "UNAVAILABLE":
                print("   💡 Tip: Run again tomorrow for historical data comparison")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
