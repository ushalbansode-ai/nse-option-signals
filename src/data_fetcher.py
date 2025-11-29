"""
Enhanced Data Fetcher for NSE Futures & Options Analysis
"""

import requests
import pandas as pd
import os
import zipfile
from datetime import datetime, timedelta
from config.settings import RAW_DATA_DIR, NSE_BHAVCOPY_BASE_URL, NSE_BHAVCOPY_FILENAME_FORMAT

class DataFetcher:
    def __init__(self):
        self.raw_data_dir = RAW_DATA_DIR
        self.current_date = None
    
    def fetch_latest_bhavcopy(self):
        """Fetch the latest NSE bhavcopy data"""
        self.current_date = datetime.now().date()
        
        # For weekends, use Friday's data
        if self.current_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
            days_back = self.current_date.weekday() - 4
            self.current_date = self.current_date - timedelta(days=days_back)
            print(f"    Weekend detected. Using Friday's data: {self.current_date}")
        
        print(f"    ANALYSIS DATE: {self.current_date}")
        
        # Try current date first, then previous trading days
        dates_to_try = [self.current_date]
        
        # Add previous trading days (max 5 days back)
        for i in range(1, 6):
            prev_date = self.current_date - timedelta(days=i)
            if prev_date.weekday() < 5:  # Monday-Friday
                dates_to_try.append(prev_date)
        
        for attempt_date in dates_to_try:
            csv_file = self.download_bhavcopy(attempt_date)
            if csv_file:
                return csv_file
        
        print("    ❌ Failed to download bhavcopy after multiple attempts")
        return None
    
    def download_bhavcopy(self, date):
        """Download bhavcopy for specific date"""
        date_str = date.strftime('%Y%m%d')
        filename = NSE_BHAVCOPY_FILENAME_FORMAT.format(date=date_str)
        url = f"{NSE_BHAVCOPY_BASE_URL}/{filename}"
        
        print(f"    Trying: {url}")
        
        try:
            # Download the file
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Save zip file
            zip_path = os.path.join(self.raw_data_dir, filename)
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            
            print(f"    Downloaded: {url}")
            
            # Extract CSV
            csv_file = self.extract_csv(zip_path, date_str)
            return csv_file
            
        except requests.exceptions.RequestException as e:
            print(f"    Failed: {e.response.status_code if hasattr(e, 'response') else 'Network error'}")
            return None
    
    def extract_csv(self, zip_path, date_str):
        """Extract CSV from zip file"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get the CSV filename from zip
                csv_filename = zip_ref.namelist()[0]
                
                # Extract to raw data directory
                zip_ref.extract(csv_filename, self.raw_data_dir)
                
                # Rename to standardized name
                extracted_path = os.path.join(self.raw_data_dir, csv_filename)
                standardized_name = f"BhavCopy_NSE_FO_{date_str}.csv"
                standardized_path = os.path.join(self.raw_data_dir, standardized_name)
                
                os.rename(extracted_path, standardized_path)
                
                # Clean up zip file
                os.remove(zip_path)
                
                print(f"    Extracted: {standardized_name}")
                return standardized_path
                
        except Exception as e:
            print(f"    ❌ Error extracting zip: {e}")
            return None
