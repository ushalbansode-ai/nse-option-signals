#!/usr/bin/env python3
"""
Simple runner script for the option strategy analysis
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from src.main import main
    print("🚀 Starting NSE Option Strategy Analysis...")
    main()
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
