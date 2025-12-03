#!/usr/bin/env python3
"""
Enhanced NSE Combined Futures & Options Analysis
Main execution script
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import json

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from data_fetcher import DataFetcher
from data_processor import DataProcessor
from combined_analyzer import CombinedAnalyzer
from report_generator import ReportGenerator
from config.settings import STRATEGY_CONFIG, ANALYSIS_CONFIG

def ensure_directories():
    """Create all necessary directories"""
    directories = [
        'data/raw',
        'data/processed',
        'data/comparison',
        'outputs/signals',
        'outputs/reports',
        'logs'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Directory ensured: {directory}")

def save_with_date(dataframe, data_type):
    """Save data with date in filename for workflow comparison"""
    today = datetime.now().strftime('%Y-%m-%d')
    filename = f"data/processed/{data_type}_{today}.csv"
    
    if dataframe is not None and not dataframe.empty:
        dataframe.to_csv(filename, index=False)
        print(f"✅ Saved {data_type} data: {filename}")
        return filename
    else:
        print(f"⚠️ No {data_type} data to save")
        return None

def load_previous_day_data():
    """Load data from previous day for comparison"""
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    previous_files = {
        'futures': None,
        'options': None
    }
    
    # Try to find yesterday's files
    for date in [yesterday.strftime('%Y-%m-%d'), (yesterday - timedelta(days=1)).strftime('%Y-%m-%d')]:
        futures_file = f"data/processed/futures_{date}.csv"
        options_file = f"data/processed/options_{date}.csv"
        
        if os.path.exists(futures_file):
            previous_files['futures'] = pd.read_csv(futures_file)
            print(f"✅ Loaded previous futures data: {futures_file}")
        
        if os.path.exists(options_file):
            previous_files['options'] = pd.read_csv(options_file)
            print(f"✅ Loaded previous options data: {options_file}")
            
        if previous_files['futures'] is not None or previous_files['options'] is not None:
            break
    
    return previous_files['futures'], previous_files['options'], yesterday.strftime('%Y-%m-%d')

def generate_comparison_report(current_futures, current_options, previous_futures, previous_options, current_date, previous_date):
    """Generate detailed comparison report"""
    comparison_data = {
        'analysis_date': current_date,
        'comparison_date': previous_date,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'summary': {},
        'futures_comparison': {},
        'options_comparison': {},
        'signals': []
    }
    
    # Summary statistics
    comparison_data['summary'] = {
        'current_futures_records': len(current_futures) if current_futures is not None else 0,
        'current_options_records': len(current_options) if current_options is not None else 0,
        'previous_futures_records': len(previous_futures) if previous_futures is not None else 0,
        'previous_options_records': len(previous_options) if previous_options is not None else 0,
        'has_historical_data': previous_futures is not None or previous_options is not None
    }
    
    # Futures comparison
    if current_futures is not None and previous_futures is not None:
        # Compare unique symbols
        current_symbols = set(current_futures['SYMBOL'].unique()) if 'SYMBOL' in current_futures.columns else set()
        previous_symbols = set(previous_futures['SYMBOL'].unique()) if 'SYMBOL' in previous_futures.columns else set()
        
        new_symbols = current_symbols - previous_symbols
        missing_symbols = previous_symbols - current_symbols
        common_symbols = current_symbols.intersection(previous_symbols)
        
        comparison_data['futures_comparison'] = {
            'new_symbols_count': len(new_symbols),
            'missing_symbols_count': len(missing_symbols),
            'common_symbols_count': len(common_symbols),
            'new_symbols': list(new_symbols)[:10],  # Limit to 10 for readability
            'missing_symbols': list(missing_symbols)[:10]
        }
    
    # Options comparison
    if current_options is not None and previous_options is not None:
        # Similar comparison logic for options
        comparison_data['options_comparison'] = {
            'records_change': len(current_options) - len(previous_options)
        }
    
    # Generate signals based on comparison
    signals = []
    
    # Signal 1: Data volume change
    if comparison_data['summary']['has_historical_data']:
        futures_change = comparison_data['summary']['current_futures_records'] - comparison_data['summary']['previous_futures_records']
        
        if futures_change > 50:
            signals.append({
                'type': 'BULLISH_VOLUME',
                'message': f'Significant increase in futures data volume (+{futures_change} records)',
                'confidence': 'HIGH',
                'action': 'Consider bullish positions'
            })
        elif futures_change < -50:
            signals.append({
                'type': 'BEARISH_VOLUME',
                'message': f'Significant decrease in futures data volume ({futures_change} records)',
                'confidence': 'MEDIUM',
                'action': 'Exercise caution, consider bearish positions'
            })
        else:
            signals.append({
                'type': 'NEUTRAL_VOLUME',
                'message': 'Stable data volume',
                'confidence': 'LOW',
                'action': 'Maintain current positions'
            })
    
    # Signal 2: New symbols
    if comparison_data['futures_comparison'].get('new_symbols_count', 0) > 0:
        signals.append({
            'type': 'NEW_INSTRUMENTS',
            'message': f"{comparison_data['futures_comparison']['new_symbols_count']} new symbols detected",
            'confidence': 'MEDIUM',
            'action': 'Research new symbols for opportunities'
        })
    
    comparison_data['signals'] = signals
    
    # Save comparison report
    comparison_file = f"data/comparison/comparison_{current_date}.json"
    with open(comparison_file, 'w') as f:
        json.dump(comparison_data, f, indent=2)
    
    # Save human-readable report
    txt_report = f"outputs/reports/comparison_report_{current_date}.txt"
    with open(txt_report, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write(f"NSE DATA COMPARISON REPORT\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Analysis Date: {current_date}\n")
        f.write(f"Comparison Date: {previous_date if previous_date else 'N/A'}\n")
        f.write(f"Generated: {comparison_data['timestamp']}\n\n")
        
        f.write("SUMMARY STATISTICS:\n")
        f.write("-" * 40 + "\n")
        f.write(f"Current Futures Records: {comparison_data['summary']['current_futures_records']}\n")
        f.write(f"Current Options Records: {comparison_data['summary']['current_options_records']}\n")
        if comparison_data['summary']['has_historical_data']:
            f.write(f"Previous Futures Records: {comparison_data['summary']['previous_futures_records']}\n")
            f.write(f"Previous Options Records: {comparison_data['summary']['previous_options_records']}\n\n")
        else:
            f.write("Previous Data: Not available (first run?)\n\n")
        
        if signals:
            f.write("GENERATED SIGNALS:\n")
            f.write("-" * 40 + "\n")
            for i, signal in enumerate(signals, 1):
                f.write(f"{i}. [{signal['type']}] {signal['message']}\n")
                f.write(f"   Confidence: {signal['confidence']}\n")
                f.write(f"   Suggested Action: {signal['action']}\n\n")
    
    print(f"✅ Comparison report generated: {comparison_file}")
    print(f"✅ Human-readable report: {txt_report}")
    
    return comparison_data

def main():
    print("=" * 60)
    print("ENHANCED NSE COMBINED FUTURES & OPTIONS ANALYSIS")
    print("=" * 60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Step 0: Ensure directories exist
        print("Step 0: Ensuring directories...")
        ensure_directories()
        
        # Step 1: Fetch data
        print("\nStep 1: Fetching bhavcopy...")
        fetcher = DataFetcher()
        csv_file = fetcher.fetch_latest_bhavcopy()
        
        if not csv_file:
            print("❌ Failed to fetch bhavcopy data")
            # Create empty placeholder files for workflow
            today = datetime.now().strftime('%Y-%m-%d')
            pd.DataFrame().to_csv(f"data/processed/futures_{today}.csv")
            pd.DataFrame().to_csv(f"data/processed/options_{today}.csv")
            print("⚠️ Created placeholder files for workflow continuity")
            return
        
        # Step 2: Process data
        print("\nStep 2: Processing data...")
        processor = DataProcessor()
        futures_data, options_data = processor.process_bhavcopy_data(csv_file)
        
        # Get current date
        current_date = fetcher.current_date.strftime('%Y-%m-%d')
        
        # Step 3: Save current data with date in filename (for workflow comparison)
        print("\nStep 3: Saving current data with date...")
        futures_file = save_with_date(futures_data, "futures")
        options_file = save_with_date(options_data, "options")
        
        # Step 4: Load previous day data
        print("\nStep 4: Loading previous day data for comparison...")
        previous_futures, previous_options, previous_date = load_previous_day_data()
        
        if previous_futures is not None or previous_options is not None:
            print(f"✅ Historical data loaded from: {previous_date}")
        else:
            print("⚠️ No historical data found (first run or data expired)")
        
        # Step 5: Generate comparison report
        print("\nStep 5: Generating comparison report...")
        comparison_data = generate_comparison_report(
            futures_data, options_data, 
            previous_futures, previous_options,
            current_date, previous_date
        )
        
        # Step 6: Run combined analysis (your existing logic)
        print("\nStep 6: Running ENHANCED COMBINED ANALYSIS...")
        analyzer = CombinedAnalyzer()
        opportunities = analyzer.identify_combined_opportunities(
            futures_data, options_data, previous_futures, previous_options
        )
        
        # Step 7: Generate enhanced reports
        print("\nStep 7: Generating enhanced reports...")
        reporter = ReportGenerator()
        
        # Determine historical status
        historical_status = "AVAILABLE" if previous_futures is not None else "UNAVAILABLE"
        
        reporter.generate_enhanced_reports(
            opportunities, futures_data, options_data, 
            current_date, previous_date, historical_status
        )
        
        # Step 8: Save opportunities as JSON for workflow
        print("\nStep 8: Saving opportunities data...")
        if opportunities:
            opportunities_file = f"outputs/signals/opportunities_{current_date}.json"
            # Convert opportunities to serializable format
            opportunities_list = []
            for opp in opportunities:
                if hasattr(opp, '__dict__'):
                    opportunities_list.append(opp.__dict__)
                elif isinstance(opp, dict):
                    opportunities_list.append(opp)
            
            with open(opportunities_file, 'w') as f:
                json.dump(opportunities_list, f, indent=2)
            print(f"✅ Opportunities saved: {opportunities_file}")
        
        # Step 9: Final summary
        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETE - WORKFLOW READY")
        print("=" * 60)
        
        print(f"\n📅 Analysis Date: {current_date}")
        print(f"📊 Historical Data: {historical_status}")
        
        if len(opportunities) > 0:
            print(f"🎯 Opportunities Identified: {len(opportunities)}")
            print("   (Check outputs/signals/ for details)")
        else:
            print("🎯 No strong opportunities identified today")
            if historical_status == "UNAVAILABLE":
                print("   Tip: Run again tomorrow for historical comparison")
        
        print(f"\n📁 Generated Files:")
        print(f"   - data/processed/futures_{current_date}.csv")
        print(f"   - data/processed/options_{current_date}.csv")
        print(f"   - data/comparison/comparison_{current_date}.json")
        print(f"   - outputs/reports/comparison_report_{current_date}.txt")
        
        if os.path.exists(f"outputs/signals/opportunities_{current_date}.json"):
            print(f"   - outputs/signals/opportunities_{current_date}.json")
        
        print(f"\n✅ Analysis completed successfully at {datetime.now().strftime('%H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ Error in main execution: {e}")
        import traceback
        traceback.print_exc()
        
        # Create minimal output files for workflow continuity
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            pd.DataFrame().to_csv(f"data/processed/futures_{today}.csv")
            pd.DataFrame().to_csv(f"data/processed/options_{today}.csv")
            print(f"⚠️ Created minimal output files for workflow")
        except:
            pass

if __name__ == "__main__":
    main()
