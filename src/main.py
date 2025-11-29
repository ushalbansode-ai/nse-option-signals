#!/usr/bin/env python3
"""
Enhanced Main Entry Point with Previous Day Data Analysis
"""

import sys
import os
import datetime

from src.data_fetcher import DataFetcher
from src.data_processor import DataProcessor
from src.combined_analyzer import CombinedAnalyzer
from src.report_generator import ReportGenerator
from src.historical_manager import HistoricalManager
from config.settings import BASE_DIR, OUT_DIR
def run_enhanced_analysis():
    """Enhanced main analysis function with holiday handling"""
    calendar = SmartTradingCalendar()
    parser = EnhancedDataParser()
    
    # Get the appropriate analysis date
    analysis_date, date_status = calendar.get_analysis_date()
    analysis_date_str = analysis_date.strftime('%Y-%m-%d')
    
    print(f"📅 ANALYSIS DATE: {analysis_date_str} ({date_status})")
    print(f"📅 TODAY: {datetime.now().strftime('%Y-%m-%d')}")
    
    # Step 1: Download data for analysis date
    print("💡 Step 1: Fetching bhavcopy...")
    
    # Replace your existing download function call with:
    csv_path = download_bhavcopy(analysis_date)  # Make sure your download function accepts a date parameter
    
    if not csv_path or not os.path.exists(csv_path):
        print("❌ Failed to download data. Market may be closed.")
        return
    
    # Step 2: Process current day data
    print("💡 Step 2: Processing data...")
    try:
        df = pd.read_csv(csv_path)
        print(f"📈 Loaded {len(df)} rows, {len(df.columns)} columns")
        
        # Debug data structure
        parser.debug_data_structure(df)
        
        # Parse instruments
        futures_df, options_df = parser.parse_instruments(df)
        
        print(f"✅ Futures contracts found: {len(futures_df)}")
        print(f"✅ Options contracts found: {len(options_df)}")
        
        if len(futures_df) > 0:
            print("📊 Futures sample symbols:", futures_df['Symbol'].head(3).tolist() if 'Symbol' in futures_df.columns else "N/A")
        if len(options_df) > 0:
            print("📊 Options sample symbols:", options_df['Symbol'].head(3).tolist() if 'Symbol' in options_df.columns else "N/A")
            
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return
    
    # Continue with your existing analysis logic below...
    # [YOUR EXISTING ANALYSIS CODE CONTINUES HERE...]
def main():
    """Enhanced Main execution with Historical Data Analysis"""
    print("🚀 NSE ENHANCED Combined Futures & Options Analysis Starting...")
    print("   📊 Using PREVIOUS DAY + CURRENT DAY data for accurate signals")
    
    try:
        # Initialize components
        fetcher = DataFetcher()
        processor = DataProcessor()
        historical_mgr = HistoricalManager()
        analyzer = CombinedAnalyzer(historical_mgr)
        reporter = ReportGenerator()
        
        # Step 1: Fetch latest data
        print("\n📥 Step 1: Fetching latest bhavcopy...")
        csv_path = fetcher.fetch_latest_bhavcopy()
        
        # Step 2: Process current day data
        print("\n🔧 Step 2: Processing current day data...")
        df = processor.load_data(csv_path)
        futures_df, options_df = processor.separate_futures_options(df)
        
        # Step 3: Load PREVIOUS day data FIRST
        print("\n📚 Step 3: Loading PREVIOUS day data for comparison...")
        current_date = datetime.date.today()
        prev_futures, prev_options, prev_date = historical_mgr.load_previous_data(current_date)
        
        if prev_futures is not None:
            print(f"✅ PREVIOUS DAY DATA: {prev_date} - {len(prev_futures)} futures, {len(prev_options)} options")
            historical_status = "AVAILABLE"
        else:
            print("⚠️ PREVIOUS DAY DATA: Not available (first run or weekend)")
            historical_status = "UNAVAILABLE"
        
        # Step 4: Save CURRENT day data for future use
        print("\n💾 Step 4: Saving CURRENT day data for future analysis...")
        historical_mgr.save_daily_data(futures_df, options_df, current_date)
        
        # Step 5: Run ENHANCED COMBINED analysis (Previous + Current)
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
