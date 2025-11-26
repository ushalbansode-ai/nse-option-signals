import requests
import datetime

def fetch_dat():
    today = datetime.datetime.now().strftime("%d%m%Y")
    url = f"https://archives.nseindia.com/content/fo/fo{today}bhav.DAT"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com"
    }

    print("Downloading:", url)
    resp = requests.get(url, headers=headers)

    if resp.status_code != 200:
        raise Exception(f"Failed to download DAT bhavcopy: {resp.status_code}")

    return resp.content
    
