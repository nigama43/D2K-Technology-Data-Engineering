import os
import requests
from retrying import retry
from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pandas as pd

# Define the URL of the TLC Trip Record Data page
url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Define the download directory
download_dir = "tlc_trip_data_2019"
csv_dir = "tlc_trip_data_2019_csv"

# Create the download directory if it does not exist
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# Create the CSV directory if it does not exist
if not os.path.exists(csv_dir):
    os.makedirs(csv_dir)

# Retry parameters
@retry(stop_max_attempt_number=5, wait_fixed=2000)
def download_file(url, dest):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an exception for HTTP errors
    with open(dest, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

# Fetch the webpage content
response = requests.get(url)
response.raise_for_status()
web_content = response.text

# Parse the webpage content
soup = BeautifulSoup(web_content, 'html.parser')

# Find all links to PARQUET files from 2019
links = soup.find_all('a', href=True)
parquet_links = [link['href'] for link in links if '2019' in link['href'] and link['href'].endswith('.parquet')]

# Debug: Print found PARQUET links
print(f"Found {len(parquet_links)} PARQUET links")

# Download each PARQUET file and convert to CSV
for parquet_link in parquet_links:
    # Construct the full URL for the PARQUET file
    if parquet_link.startswith('/'):
        parquet_link = 'https://www.nyc.gov' + parquet_link

    # Extract the file name from the URL
    file_name = os.path.join(download_dir, parquet_link.split('/')[-1])
    csv_file_name = os.path.join(csv_dir, parquet_link.split('/')[-1].replace('.parquet', '.csv'))

    print(f"Downloading {parquet_link} to {file_name}")
    try:
        # Download the PARQUET file
        download_file(parquet_link, file_name)
        print(f"Successfully downloaded {file_name}")

        # Convert the PARQUET file to CSV
        table = pq.read_table(file_name)
        df = table.to_pandas()
        df.to_csv(csv_file_name, index=False)
        print(f"Successfully converted {file_name} to {csv_file_name}")

    except Exception as e:
        print(f"Failed to download or convert {parquet_link}: {e}")

print("Download and conversion completed.")
