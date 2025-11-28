"""
Option buying strategy engine implementing various setups
"""

import pandas as pd
from collections import defaultdict

class OptionBuyingStrategy:
    def __init__(self):
        self.setup_names = {
            1: "Long Call - Trend + Long Build-up + Call Writing Unwinding",
            2: "Long Put - Short Build-up + Put Writing Unwinding", 
            3: "Breakout/Breakdown + Fresh OI + Volume Spike",
            4: "Max Pain + OI Walls Analysis"
        }
    
    def setup_1_long_call(self, futures_df, option_chain, underlying_prices):
        """Setup 1 – Long Call: Trend + Long Build-up + Call Writing Unwinding"""
        opportunities = []
        
        for _, future in futures_df.iterrows():
            symbol = future['underlying']
            fut_price = future['LastPric']
            fut_oi_change = future['ChngInOpnIntrst']
            price_change_pct = (future['LastPric'] - future['PrvsClsgPric']) / future['PrvsClsgPric'] * 100
            
            if symbol not in option_chain:
                continue
            
            # Futures Long build-up conditions
            long_buildup = (
                price_change_pct > 1.0 and
                fut_oi_change > 0 and
                abs(fut_oi_change) > future['OpnIntrst'] * 0.05
            )
            
            if not long_buildup:
                continue
            
            # Analyze option chain
            symbol_chain = option_chain[symbol]
            
            for expiry, strikes in symbol_chain.items():
                call_oi_decrease = 0
                put_oi_increase = 0
                atm_calls = []
                atm_puts = []
                
                for (strike, opt_type), data in strikes.items():
                    if data.get('is_ATM', False):
                        if opt_type == 'CE':
                            atm_calls.append(data)
                            if data['oi_change'] < 0:
                                call_oi_decrease += abs(data['oi_change'])
                        elif opt_type == 'PE':
                            atm_puts.append(data)
                            if data['oi_change'] > 0:
                                put_oi_increase += data['oi_change']
                
                if (call_oi_decrease > 0 and put_oi_increase > 0 and 
                    len(atm_calls) > 0 and len(atm_puts) > 0):
                    
                    opportunities.append({
                        'symbol': symbol,
                        'setup': 'Long Call',
                        'setup_id': 1,
                        'reason': 'Futures Long Build-up + Call Unwinding + Put Writing',
                        'future_price': fut_price,
                        'price_change_pct': price_change_pct,
                        'oi_change': fut_oi_change,
                        'call_oi_change': call_oi_decrease,
                        'put_oi_change': put_oi_increase,
                        'recommendation': 'BUY ATM CALL',
                        'strike_guidance': f"Around {fut_price:.2f}",
                        'expiry': expiry,
                        'confidence': 'High' if price_change_pct > 1.5 else 'Medium',
                        'volume_spike': future['TtlTradgVol'] > 1000000
                    })
                    break
        
        return opportunities

    def setup_2_long_put(self, futures_df, option_chain, underlying_prices):
        """Setup 2 – Long Put: Short Build-up + Put Writing Unwinding"""
        opportunities = []
        
        for _, future in futures_df.iterrows():
            symbol = future['underlying']
            fut_price = future['LastPric']
            fut_oi_change = future['ChngInOpnIntrst']
            price_change_pct = (future['LastPric'] - future['PrvsClsgPric']) / future['PrvsClsgPric'] * 100
            
            if symbol not in option_chain:
                continue
            
            # Futures Short build-up conditions
            short_buildup = (
                price_change_pct < -1.0 and
                fut_oi_change > 0 and
                abs(fut_oi_change) > future['OpnIntrst'] * 0.05
            )
            
            if not short_buildup:
                continue
            
            # Analyze option chain
            symbol_chain = option_chain[symbol]
            
            for expiry, strikes in symbol_chain.items():
                put_oi_decrease = 0
                call_oi_increase = 0
                atm_calls = []
                atm_puts = []
                
                for (strike, opt_type), data in strikes.items():
                    if data.get('is_ATM', False):
                        if opt_type == 'PE':
                            atm_puts.append(data)
                            if data['oi_change'] < 0:
                                put_oi_decrease += abs(data['oi_change'])
                        elif opt_type == 'CE':
                            atm_calls.append(data)
                            if data['oi_change'] > 0:
                                call_oi_increase += data['oi_change']
                
                if (put_oi_decrease > 0 and call_oi_increase > 0 and 
                    len(atm_calls) > 0 and len(atm_puts) > 0):
                    
                    opportunities.append({
                        'symbol': symbol,
                        'setup': 'Long Put',
                        'setup_id': 2,
                        'reason': 'Futures Short Build-up + Put Unwinding + Call Writing',
                        'future_price': fut_price,
                        'price_change_pct': price_change_pct,
                        'oi_change': fut_oi_change,
                        'call_oi_change': call_oi_increase,
                        'put_oi_change': put_oi_decrease,
                        'recommendation': 'BUY ATM PUT',
                        'strike_guidance': f"Around {fut_price:.2f}",
                        'expiry': expiry,
                        'confidence': 'High' if price_change_pct < -1.5 else 'Medium',
                        'volume_spike': future['TtlTradgVol'] > 1000000
                    })
                    break
        
        return opportunities

    def setup_3_breakout(self, futures_df, option_chain, underlying_prices):
        """Setup 3 – Breakout/Breakdown + Fresh OI + Volume Spike"""
        opportunities = []
        
        for _, future in futures_df.iterrows():
            symbol = future['underlying']
            fut_price = future['LastPric']
            prev_close = future['PrvsClsgPric']
            price_change_pct = (fut_price - prev_close) / prev_close * 100
            volume = future['TtlTradgVol']
            oi_change = future['ChngInOpnIntrst']
            
            if symbol not in option_chain:
                continue
            
            breakout_up = (
                price_change_pct > 1.5 and
                oi_change > 0 and
                volume > 1000000
            )
            
            breakdown_down = (
                price_change_pct < -1.5 and
                oi_change > 0 and
                volume > 1000000
            )
            
            if breakout_up:
                symbol_chain = option_chain[symbol]
                call_volume_spike = False
                
                for expiry, strikes in symbol_chain.items():
                    total_call_volume = sum(data['volume'] for (strike, opt_type), data in strikes.items() 
                                          if opt_type == 'CE' and data.get('is_ATM', False))
                    if total_call_volume > 10000:
                        call_volume_spike = True
                        break
                
                if call_volume_spike:
                    opportunities.append({
                        'symbol': symbol,
                        'setup': 'Breakout Call',
                        'setup_id': 3,
                        'reason': 'Price Breakout + Volume Spike + OI Increase + Call Volume Spike',
                        'future_price': fut_price,
                        'price_change_pct': price_change_pct,
                        'oi_change': oi_change,
                        'volume': volume,
                        'recommendation': 'BUY OTM CALL',
                        'strike_guidance': f"5-10% above {fut_price:.2f}",
                        'confidence': 'High',
                        'entry_type': 'Breakout'
                    })
            
            if breakdown_down:
                symbol_chain = option_chain[symbol]
                put_volume_spike = False
                
                for expiry, strikes in symbol_chain.items():
                    total_put_volume = sum(data['volume'] for (strike, opt_type), data in strikes.items() 
                                         if opt_type == 'PE' and data.get('is_ATM', False))
                    if total_put_volume > 10000:
                        put_volume_spike = True
                        break
                
                if put_volume_spike:
                    opportunities.append({
                        'symbol': symbol,
                        'setup': 'Breakdown Put',
                        'setup_id': 3,
                        'reason': 'Price Breakdown + Volume Spike + OI Increase + Put Volume Spike',
                        'future_price': fut_price,
                        'price_change_pct': price_change_pct,
                        'oi_change': oi_change,
                        'volume': volume,
                        'recommendation': 'BUY OTM PUT',
                        'strike_guidance': f"5-10% below {fut_price:.2f}",
                        'confidence': 'High',
                        'entry_type': 'Breakdown'
                    })
        
        return opportunities

    def analyze_max_pain(self, option_chain, underlying_prices):
        """Setup 4 – Max Pain + OI Walls Analysis"""
        opportunities = []
        
        for symbol, expiries in option_chain.items():
            current_price = underlying_prices.get(symbol)
            if not current_price:
                continue
            
            for expiry, strikes in expiries.items():
                strike_oi = {}
                
                for (strike, opt_type), data in strikes.items():
                    if strike not in strike_oi:
                        strike_oi[strike] = {'call_oi': 0, 'put_oi': 0}
                    
                    if opt_type == 'CE':
                        strike_oi[strike]['call_oi'] += data['oi']
                    else:
                        strike_oi[strike]['put_oi'] += data['oi']
                
                max_pain_strike = None
                min_net_oi = float('inf')
                
                for strike, ois in strike_oi.items():
                    net_oi = abs(ois['call_oi'] - ois['put_oi'])
                    if net_oi < min_net_oi:
                        min_net_oi = net_oi
                        max_pain_strike = strike
                
                if max_pain_strike:
                    if current_price > max_pain_strike * 1.02:
                        opportunities.append({
                            'symbol': symbol,
                            'setup': 'Max Pain Call',
                            'setup_id': 4,
                            'reason': f'Price above Max Pain ({max_pain_strike}) - Call favored',
                            'current_price': current_price,
                            'max_pain': max_pain_strike,
                            'recommendation': 'BUY CALL',
                            'strike_guidance': f"ATM to OTM, Max Pain: {max_pain_strike}",
                            'confidence': 'Medium',
                            'expiry': expiry
                        })
                    
                    elif current_price < max_pain_strike * 0.98:
                        opportunities.append({
                            'symbol': symbol,
                            'setup': 'Max Pain Put',
                            'setup_id': 4,
                            'reason': f'Price below Max Pain ({max_pain_strike}) - Put favored',
                            'current_price': current_price,
                            'max_pain': max_pain_strike,
                            'recommendation': 'BUY PUT',
                            'strike_guidance': f"ATM to OTM, Max Pain: {max_pain_strike}",
                            'confidence': 'Medium',
                            'expiry': expiry
                        })
        
        return opportunities

    def analyze_all_setups(self, futures_df, option_chain, underlying_prices):
        """Run all strategy setups"""
        all_opportunities = []
        
        all_opportunities.extend(self.setup_1_long_call(futures_df, option_chain, underlying_prices))
        all_opportunities.extend(self.setup_2_long_put(futures_df, option_chain, underlying_prices))
        all_opportunities.extend(self.setup_3_breakout(futures_df, option_chain, underlying_prices))
        all_opportunities.extend(self.analyze_max_pain(option_chain, underlying_prices))
        
        return pd.DataFrame(all_opportunities)
