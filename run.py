#!/usr/bin/env python3
"""
Enhanced NSE Combined Futures & Options Analysis
Main execution script
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from data_fetcher import DataFetcher
from data_processor import DataProcessor
from combined_analyzer import CombinedAnalyzer
from report_generator import ReportGenerator
from config.settings import STRATEGY_CONFIG, ANALYSIS_CONFIG  # Fixed import

def main():
    print("Run Combined Futures & Options Analysis")
    print("=" * 50)
    
    try:
        # Step 1: Fetch data
        print("\nStep 1: Fetching bhavcopy...")
        fetcher = DataFetcher()
        csv_file = fetcher.fetch_latest_bhavcopy()
        
        if not csv_file:
            print("❌ Failed to fetch bhavcopy data")
            return
        
        # Step 2: Process data
        print("\nStep 2: Processing data...")
        processor = DataProcessor()
        futures_data, options_data = processor.process_bhavcopy_data(csv_file)
        
        # Step 3: Load previous data
        print("\nStep 3: Loading PREVIOUS day data for comparison...")
        previous_futures, previous_options, prev_date = processor.load_previous_data()
        
        # Step 4: Save current data for future analysis
        print("\nStep 4: Saving CURRENT day data for future analysis...")
        processor.save_current_data(futures_data, options_data)
        
        # Step 5: Run combined analysis
        print("\nStep 5: Running ENHANCED COMBINED ANALYSIS...")
        analyzer = CombinedAnalyzer()
        opportunities = analyzer.identify_combined_opportunities(
            futures_data, options_data, previous_futures, previous_options
        )
        
        # Step 6: Generate enhanced reports
        print("\nStep 6: Generating enhanced reports...")
        reporter = ReportGenerator()
        
        # Determine historical status
        historical_status = "AVAILABLE" if previous_futures is not None else "UNAVAILABLE"
        current_date = fetcher.current_date.strftime('%Y-%m-%d')
        
        reporter.generate_enhanced_reports(
            opportunities, futures_data, options_data, 
            current_date, prev_date, historical_status
        )
        
        print("\n" + "=" * 50)
        print("ENHANCED ANALYSIS COMPLETED SUCCESSFULLY!")
        print(f"Historical Data: {historical_status}")
        
        if len(opportunities) > 0:
            print(f"FINAL VERDICT: {len(opportunities)} opportunities identified")
        else:
            print("FINAL VERDICT: No strong opportunities identified today")
            if historical_status == "UNAVAILABLE":
                print("Tip: Run again tomorrow for historical data comparison")
                
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
