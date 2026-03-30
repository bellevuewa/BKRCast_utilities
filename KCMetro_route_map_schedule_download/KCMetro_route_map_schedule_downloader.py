import os
import requests

## This tool is to download the route map and schedule for regular bus routes and BRT routes from KC Metro website. The route numbers are based on the information on the website as of June 2024, and the URL pattern is based on the URL of one of the route maps/schedules. 
## The downloaded files will be saved in a specified directory. 

### configuration
## route numbers for regular bus routes from KC Metro website
route_numbers_0 = [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 17, 21, 22, 24, 27, 28, 31, 32, 33, 36, 40, 43, 44, 45, 48, 49, 50, 56, 57, 60, 61, 62, 65, 67, 70, 75, 79, 90]
route_numbers_100 = [101, 102, 105, 106, 107, 111, 113, 118, 119, 124, 125, 128, 131, 132, 148, 150, 153, 156, 160, 161, 162, 165, 168, 177, 181, 182, 183, 184, 187, 193]
route_numbers_200 = [203, 204, 208, 212, 218, 222, 223, 224, 225, 226, 230, 231, 239, 240, 245, 249, 250, 255, 256, 269, 271]
route_numbers_300 = [303, 322, 331, 333, 345, 346, 348, 365, 372]
route_numbers_600 = [630, 631, 635]
route_numbers_700 = [773, 775]
route_numbers_800 = [893, 895]
route_numbers_900 = [901, 903, 906, 907, 914, 915, 917, 930, 931, 981, 982, 986, 987, 988, 989]

## BRT routes
BRT_numbers = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

Regular_bus_URL = "https://kingcounty.gov/en/-/media/king-county/depts/metro/schedules/pdf/03282026/rt-{num:03}.pdf"
BRT_URL = "https://kingcounty.gov/en/-/media/king-county/depts/metro/schedules/pdf/03282026/rt-{num}-Line.pdf"
DOWNLOAD_DIR = r"I:\Modeling and Analysis Group\03_Data\Transit Data\KCMetro\2026 Spring"

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


route_numbers = route_numbers_0 + route_numbers_100 + route_numbers_200 + route_numbers_300 + route_numbers_600 + route_numbers_700 + route_numbers_800 + route_numbers_900

for i in route_numbers:  
    url = Regular_bus_URL.format(num=i)
    filename = os.path.join(DOWNLOAD_DIR, f"rt-{i:03}.pdf")

    download_pdf(url, filename)

for i in BRT_numbers:
    url = BRT_URL.format(num=i)
    filename = os.path.join(DOWNLOAD_DIR, f"rt-{i}-Line.pdf")

    download_pdf(url, filename)    

print("Done!")

