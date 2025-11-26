import requests
import datetime
import zipfile
import io

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com"
}

URL_DAT = "https://archives.nseindia.com/content/fo/fo{date}bhav.DAT"
URL_ZIP = "https://archives.nseindia.com/content/fo/fo{date}bhav.csv.zip"


def is_weekend(d):
    return d.weekday() >= 5


def try_url(url):
    print(f"Trying: {url}")
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        print("✔ Found file")
        return r.content
    return None


def fetch_dat():
    today = datetime.datetime.now()

    # Look back 7 days
    for delta in range(0, 7):
        d = today - datetime.timedelta(days=delta)
        if is_weekend(d):
            print(f"Skipping weekend: {d.strftime('%d-%m-%Y')}")
            continue

        date_str = d.strftime("%d%m%Y")

        # Try DAT format
        dat_url = URL_DAT.format(date=date_str)
        dat_file = try_url(dat_url)
        if dat_file:
            return dat_file

        # Try ZIP CSV format
        zip_url = URL_ZIP.format(date=date_str)
        zip_file = try_url(zip_url)
        if zip_file:
            print("✔ ZIP file found. Extracting...")
            z = zipfile.ZipFile(io.BytesIO(zip_file))
            # There is exactly one CSV inside
            name = z.namelist()[0]
            return z.read(name)

    raise Exception("No DAT or ZIP bhavcopy available for last 7 days.")
            
