# src/detect_and_extract.py
from pathlib import Path
import zipfile
import time
import shutil


RAW = Path('data/raw')
HISTORY_RAW = Path('data/history/raw')
HISTORY_RAW.mkdir(parents=True, exist_ok=True)




def latest_zip_path():
zips = list(RAW.glob('*.zip'))
if not zips:
return None
latest = max(zips, key=lambda p: p.stat().st_mtime)
return latest
def extract_latest_zip():
z = latest_zip_path()
if z is None:
raise SystemExit('No zip file found in data/raw')
with zipfile.ZipFile(z, 'r') as zf:
names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
if not names:
raise SystemExit('No CSV found inside zip: %s' % z)
csvname = names[0]
ts = int(time.time())
out_name = HISTORY_RAW / f"{z.stem}_{ts}.csv"
with zf.open(csvname) as src, open(out_name, 'wb') as dst:
shutil.copyfileobj(src, dst)
return out_name




if __name__ == '__main__':
p = extract_latest_zip()
print('Extracted to', p)
