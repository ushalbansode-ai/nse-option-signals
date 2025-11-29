"""
Enhanced Combined Futures + Options Analyzer
with Historical Data Comparison
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config.settings import STRATEGY_CONFIG

class CombinedAnalyzer:
    def __init__(self):
        self.strategy_config = STRATEGY_CONFIG
    
    def analyze_option_chain(self, chain_df):
        """Analyze option chain for a single underlying symbol"""
        try:
            # Convert symbol to string to handle numeric underlying symbols
            if 'symbol' in chain_df.columns:
                chain_df['symbol'] = chain_df['symbol'].astype(str)
            
            # Check if we have both CALL and PUT data
            if 'call' not in chain_df['optionType'].str.lower().values or 'put' not in chain_df['optionType'].str.lower().values:
                return None
            
            # Group by strike and option type
            strikes = chain_df['strikePrice'].unique()
            chain_analysis = {
                'symbol': chain_df['symbol'].iloc[0] if 'symbol' in chain_df.columns else 'UNKNOWN',
                'total_strikes': len(strikes),
                'max_pain': self.calculate_max_pain(chain_df),
                'call_oi': chain_df[chain_df['optionType'].str.lower() == 'call']['openInterest'].sum(),
                'put_oi': chain_df[chain_df['optionType'].str.lower() == 'put']['openInterest'].sum(),
                'call_volume': chain_df[chain_df['optionType'].str.lower() == 'call']['volume'].sum(),
                'put_volume': chain_df[chain_df['optionType'].str.lower() == 'put']['volume'].sum(),
            }
            return chain_analysis
        except Exception as e:
            print(f"      Error analyzing chain: {e}")
            return None

    def calculate_max_pain(self, chain_df):
        """Calculate max pain for option chain"""
        try:
            # Group by strike price
            strikes = chain_df['strikePrice'].unique()
            pain = []
            
            for strike in strikes:
                # Get call and put OI at this strike
                call_oi = chain_df[(chain_df['strikePrice'] == strike) & 
                                  (chain_df['optionType'].str.lower() == 'call')]['openInterest'].sum()
                put_oi = chain_df[(chain_df['strikePrice'] == strike) & 
                                 (chain_df['optionType'].str.lower() == 'put')]['openInterest'].sum()
                total_pain = (call_oi + put_oi) * abs(strike - chain_df['underlying'].iloc[0])
                pain.append(total_pain)
            
            if pain:
                min_pain_index = pain.index(min(pain))
                return strikes[min_pain_index]
            else:
                return None
        except Exception as e:
            print(f"      Error calculating max pain: {e}")
            return None

    def identify_combined_opportunities(self, futures_data, options_data, previous_futures, previous_options):
        """Identify combined trading opportunities"""
        print("    Identifying trading opportunities...")
        
        opportunities = []
        
        if futures_data is None or options_data is None:
            print("    ❌ Missing futures or options data")
            return opportunities
        
        # Get unique symbols from futures data
        futures_symbols = futures_data['symbol'].unique()
        
        for symbol in futures_symbols:
            try:
                # Get futures data for this symbol
                current_future = futures_data[futures_data['symbol'] == symbol].iloc[0]
                
                # Get options chain for this symbol
                symbol_options = options_data[options_data['symbol'] == symbol]
                
                if len(symbol_options) == 0:
                    continue
                
                # Analyze option chain
                chain_analysis = self.analyze_option_chain(symbol_options)
                if not chain_analysis:
                    continue
                
                # Get previous day data if available
                previous_future = None
                if previous_futures is not None:
                    prev_match = previous_futures[previous_futures['symbol'] == symbol]
                    if len(prev_match) > 0:
                        previous_future = prev_match.iloc[0]
                
                # Analyze opportunity
                opportunity = self.analyze_combined_opportunity(
                    current_future, chain_analysis, previous_future
                )
                
                if opportunity:
                    opportunities.append(opportunity)
                    
            except Exception as e:
                print(f"      Error analyzing {symbol}: {e}")
                continue
        
        print(f"    Identified {len(opportunities)} potential opportunities")
        return opportunities

    def analyze_combined_opportunity(self, current_future, chain_analysis, previous_future):
        """Analyze combined futures + options opportunity"""
        try:
            symbol = current_future['symbol']
            
            # Calculate metrics
            current_price = current_future['lastPrice']
            price_change_pct = 0
            oi_change_pct = 0
            volume_change_pct = 0
            
            if previous_future is not None:
                # Calculate percentage changes
                prev_price = previous_future['lastPrice']
                prev_oi = previous_future['openInterest']
                prev_volume = previous_future['volume']
                
                if prev_price > 0:
                    price_change_pct = ((current_price - prev_price) / prev_price) * 100
                if prev_oi > 0:
                    oi_change_pct = ((current_future['openInterest'] - prev_oi) / prev_oi) * 100
                if prev_volume > 0:
                    volume_change_pct = ((current_future['volume'] - prev_volume) / prev_volume) * 100
            
            # Analyze options signals
            call_oi = chain_analysis['call_oi']
            put_oi = chain_analysis['put_oi']
            call_volume = chain_analysis['call_volume']
            put_volume = chain_analysis['put_volume']
            max_pain = chain_analysis['max_pain']
            
            # Calculate OI ratio and trends
            total_oi = call_oi + put_oi
            oi_ratio = put_oi / call_oi if call_oi > 0 else 0
            call_oi_change = 0  # Placeholder for actual calculation
            put_oi_change = 0   # Placeholder for actual calculation
            
            # Determine setup and recommendation
            setup, recommendation, confidence, reason = self.evaluate_combined_signals(
                current_future, price_change_pct, oi_change_pct, volume_change_pct,
                call_oi, put_oi, oi_ratio, call_volume, put_volume, max_pain
            )
            
            if setup:
                # Determine data quality
                data_quality = self.determine_data_quality(previous_future)
                has_historical_data = previous_future is not None
                
                # Determine strike guidance
                strike_guidance = self.determine_strike_guidance(current_price, max_pain, recommendation)
                
                return {
                    'symbol': symbol,
                    'setup': setup,
                    'recommendation': recommendation,
                    'confidence': confidence,
                    'current_price': current_price,
                    'price_change_pct': price_change_pct,
                    'oi_change_pct': oi_change_pct,
                    'volume_change_pct': volume_change_pct,
                    'call_oi': call_oi,
                    'put_oi': put_oi,
                    'call_oi_change': call_oi_change,
                    'put_oi_change': put_oi_change,
                    'oi_ratio': oi_ratio,
                    'max_pain': max_pain,
                    'strike_guidance': strike_guidance,
                    'reason': reason,
                    'data_quality': data_quality,
                    'has_historical_data': has_historical_data
                }
            
            return None
            
        except Exception as e:
            print(f"      Error in opportunity analysis: {e}")
            return None

    def determine_data_quality(self, previous_future):
        """Determine data quality based on historical data availability"""
        if previous_future is None:
            return "LOW"
        else:
            # Check if we have sufficient previous data
            required_fields = ['lastPrice', 'openInterest', 'volume']
            if all(field in previous_future and pd.notna(previous_future[field]) for field in required_fields):
                return "HIGH"
            else:
                return "MEDIUM"

    def determine_strike_guidance(self, current_price, max_pain, recommendation):
        """Provide strike price guidance"""
        try:
            if max_pain and not pd.isna(max_pain):
                if 'CALL' in recommendation:
                    # For CALLs, suggest strikes above current price but below max pain for conservative
                    suggested_strike = min(current_price * 1.02, max_pain * 0.98)
                    return f"CE around {suggested_strike:.1f}"
                else:
                    # For PUTs, suggest strikes below current price but above max pain
                    suggested_strike = max(current_price * 0.98, max_pain * 1.02)
                    return f"PE around {suggested_strike:.1f}"
            else:
                # Fallback without max pain
                if 'CALL' in recommendation:
                    return f"CE {current_price * 1.02:.1f} - {current_price * 1.05:.1f}"
                else:
                    return f"PE {current_price * 0.95:.1f} - {current_price * 0.98:.1f}"
        except:
            return "Check nearby strikes"

    def evaluate_combined_signals(self, current_future, price_change_pct, oi_change_pct, volume_change_pct,
                                 call_oi, put_oi, oi_ratio, call_volume, put_volume, max_pain):
        """Evaluate combined signals from futures and options"""
        
        # Extract futures data
        current_price = current_future['lastPrice']
        current_oi = current_future['openInterest']
        current_volume = current_future['volume']
        
        # Bullish scenarios
        if (price_change_pct > 1.0 and oi_change_pct > 5.0 and 
            volume_change_pct > 10.0 and oi_ratio < 0.8):
            return ("Bullish Breakout", "BUY CALL", "High", 
                   "Price up with OI buildup & high volume, PUT-CALL ratio favorable")
        
        # Bearish scenarios  
        if (price_change_pct < -1.0 and oi_change_pct > 5.0 and 
            volume_change_pct > 10.0 and oi_ratio > 1.2):
            return ("Bearish Breakdown", "BUY PUT", "High",
                   "Price down with OI buildup & high volume, PUT-CALL ratio favorable")
        
        # Moderate confidence scenarios
        if price_change_pct > 0.5 and oi_change_pct > 2.0 and volume_change_pct > 5.0:
            return ("Moderate Bullish", "BUY CALL", "Medium",
                   "Moderate price rise with OI buildup")
        
        if price_change_pct < -0.5 and oi_change_pct > 2.0 and volume_change_pct > 5.0:
            return ("Moderate Bearish", "BUY PUT", "Medium",
                   "Moderate price decline with OI buildup")
        
        return None, None, None, None
