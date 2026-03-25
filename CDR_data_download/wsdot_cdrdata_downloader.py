import os
import requests
from datetime import datetime, timedelta

BASE_URL = "https://data.wsdot.wa.gov/traffic/NW/FreewayData/5minute/DATA2025"
OUTPUT_DIR = r"I:\Modeling and Analysis Group\03_Data\TrafficData\WSDOT\Freeways\2025\Data2025"
TIMEOUT = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)

def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def download_file(url, filepath):
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT) as r:
            if r.status_code == 200:
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                print(f"Downloaded: {filepath}")
            else:
                print(f"Missing: {url} (status {r.status_code})")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

def main():
    start_date = datetime(2025, 10, 1)
    end_date = datetime(2025, 10, 31)

    for single_date in daterange(start_date, end_date):
        date_str = single_date.strftime("%Y%m%d")
        filename = f"{date_str}.DAT"
        url = f"{BASE_URL}/{filename}"
        filepath = os.path.join(OUTPUT_DIR, filename)

        if os.path.exists(filepath):
            print(f"Skip (exists): {filename}")
            continue

        download_file(url, filepath)

if __name__ == "__main__":
    main()