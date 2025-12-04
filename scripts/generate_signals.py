#!/usr/bin/env python3
"""Load processed parquet(s), compute OI/price changes vs previous day and emit signals for setups described.


This is intentionally simple and readable; tune thresholds in config.yaml.
"""
import yaml
import glob
import pandas as pd
from pathlib import Path
from datetime import datetime
from scripts.utils import pct_change


ROOT = Path('.')
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'outputs' / 'signals'
OUT.mkdir(parents=True, exist_ok=True)
with open('config.yaml') as f:
cfg = yaml.safe_load(f)


TH = cfg['thresholds']
FILTERS = cfg['filters']




def load_all():
files = glob.glob(str(PROCESSED / '*.parquet'))
frames = []
for p in files:
df = pd.read_parquet(p)
# try to infer date from filename; else use processing time
date = Path(p).stem
df['_source_file'] = p
df['_date'] = date
frames.append(df)
if not frames:
raise SystemExit('No processed files found. Run parse_bhavcopy first.')
all_df = pd.concat(frames, ignore_index=True)
return all_df
def detect_setups(all_df):
# We'll group by symbol+expiry and compare to previous day snapshot per symbol
signals = []
grouped = all_df.groupby(['SYMBOL', 'EXPIRY_DT'])
for (sym, exp), g in grouped:
# separate futures row vs option rows by INSTRUMENT or OPTION_TYPE heuristics
fut_rows = g[g['INSTRUMENT'].str.contains('FUT', na=False) | g['OPTION_TYPE'].isna()]
opt_rows = g[g['OPTION_TYPE'].isin(['CE','PE'])]
if fut_rows.empty or opt_rows.empty:
continue
# aggregate futures metrics
fut = fut_rows.iloc[0]
# naive previous-day lookup: try to find in another file by date - but because user supplies files manually
# we'll do day-over-day only when two files exist for same symbol: pick last two snapshots
snapshots = fut_rows['_source_file'].unique()
# For simplicity assume processed files are daily and present previous file in processed dir
# We'll compute today's vs yesterday by comparing to any other processed file for same symbol/expiry if present
# Better approach: keep a small history store. For now we compare to any older file in processed dir.
# Build series for OI and LTP
try:
# FUT_LTP and FUT_OI expected in LTP and OPEN_INT
today_ltp = float(fut['LTP'])
today_oi = float(fut['OPEN_INT'])
except Exception:
continue
# attempt to find a prior snapshot for same symbol/expiry
prior = None
# naive search in directory for other parquet files - take one with different name
for p in Path('data/processed').glob('*.parquet'):
dfp = pd.read_parquet(p)
mm = dfp[(dfp['SYMBOL']==sym) & (dfp['EXPIRY_DT']==exp) & (dfp['INSTRUMENT'].str.contains('FUT', na=False))]
if not mm.empty:
prior = mm.iloc[0]
if p.name != Path(fut['_source_file']).name:
break
if prior is None:
# cannot compute day-over-day; skip price/OI based setups but still can compute option-chain snapshots
prior_ltp = None
prior_oi = None
else:
prior_ltp = prior['LTP']
prior_oi = prior['OPEN_INT']


# compute basic fut signals
fut_price_pct = pct_change(today_ltp, prior_ltp) if prior_ltp is not None else None
fut_oi_pct = pct_change(today_oi, prior_oi) if prior_oi is not None else None


# Option-chain: pick ATM strike (closest strike to fut LTP)
strikes = opt_rows['STRIKE_PRICE'].dropna().unique()
if len(strikes)==0:
continue
atm = min(strikes, key=lambda s: abs(s - today_ltp))
atm_calls = opt_rows[(opt_rows['STRIKE_PRICE']==atm) & (opt_rows['OPTION_TYPE']=='CE')]
atm_puts = opt_rows[(opt_rows['STRIKE_PRICE']==atm) & (opt_rows['OPTION_TYPE']=='PE')]
# compute OI changes at ATM if previous snapshot exists
atm_call_oi = atm_calls['OPEN_INT'].sum() if not atm_calls.empty else 0
atm_put_oi = atm_puts['OPEN_INT'].sum() if not atm_puts.empty else 0
# naive previous atm oi search
prior_atm_call_oi = None
prior_atm_put_oi = None
if prior is not None:
# search prior file for atm rows
# simple approach: read the prior file and get matching strike rows
pf = pd.read_parquet(prior['_source_file']) if '_source_file' in prior else None
if pf is not None:
prior_calls = pf[(pf['STRIKE_PRICE']==atm) & (pf['OPTION_TYPE']=='CE')]
prior_puts = pf[(pf['STRIKE_PRICE']==atm) & (pf['OPTION_TYPE']=='PE')]
prior_atm_call_oi = prior_calls['OPEN_INT'].sum() if not prior_calls.empty else 0
prior_atm_put_oi = prior_puts['OPEN_INT'].sum() if not prior_puts.empty else 0


call_oi_change_pct = pct_change(atm_call_oi, prior_atm_call_oi) if prior_atm_call_oi is not None else None
put_oi_change_pct = pct_change(atm_put_oi, prior_atm_put_oi) if prior_atm_put_oi is not None else None


# Decide setups
# Setup 1 — Long Call conditions (simple boolean tests)
call_setup = False
if fut_price_pct is not None and fut_oi_pct is not None and call_oi_change_pct is not None and put_oi_change_pct is not None:
if fut_price_pct > TH['fut_price_pct'] and fut_oi_pct > TH['fut_oi_pct']:
if call_oi_change_pct < TH['oi_unwinding_pct'] and put_oi_change_pct > TH['fut_oi_pct']:
