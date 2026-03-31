import os
import requests

## This tool is to download the route map and schedule for regular bus routes and BRT routes from SoundTransit website. The route numbers are based on the information on the website as of June 2024, and the URL pattern is based on the URL of one of the route maps/schedules. 
## The downloaded files will be saved in a specified directory. 

### configuration
## route numbers for regular bus routes from KC Metro website
ST_bus_numbers = [510, 515, 545, 554, 560, 570, 574, 577, 578, 586, 596]
## LRT routes
LRT_numbers = [1, 2, 't']

ST_bus_URL = "https://www.soundtransit.org/sites/default/files/documents/schedule-{num}.pdf"
LRT_URL = "https://www.soundtransit.org/sites/default/files/documents/schedule-link-{num}-line.pdf"
DOWNLOAD_DIR = r"I:\Modeling and Analysis Group\03_Data\Transit Data\ST\2026 Spring"

### end configuration

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def download_pdf(url, filename):
    try:
        with requests.get(url, headers = HEADERS, stream=True, timeout=10) as response:
            if response.status_code == 200:            
                print(f"Downloading {filename}")

                with open(filename, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # filter keep-alive chunks
                            f.write(chunk)
            else:
                print(f"Skipping (not found or not PDF): {url}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")


for i in ST_bus_numbers:  
    url = ST_bus_URL.format(num=i)
    filename = os.path.join(DOWNLOAD_DIR, f"ST-{i:03}.pdf")

    download_pdf(url, filename)

for i in LRT_numbers:
    url = LRT_URL.format(num=i)
    filename = os.path.join(DOWNLOAD_DIR, f"ST-{i}-Line.pdf")

    download_pdf(url, filename)    

print("Done!")

