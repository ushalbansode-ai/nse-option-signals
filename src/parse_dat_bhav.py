import pandas as pd
import io
import zipfile

def parse_dat_bytes(raw_bytes):
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as z:
        fname = z.namelist()[0]
        df = pd.read_csv(z.open(fname))

    # Clean field names
    df.columns = df.columns.str.strip()

    return df
  
