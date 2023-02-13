
# Import required libraries
import requests
import os
from google.cloud import storage

# Define the years for which data will be downloaded
years = [2019, 2020]
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Define the urls for downloading the data
taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month}.parquet"


# Function to download and store the data
def download_and_store(color, year):
    # Download the data
    for month in range(1, 13):
        if month < 10:
            month = "0"+str(month)
        else:
            month = str(month)
        response = requests.get(f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month}.parquet")
        if response.status_code != 200:
            raise Exception("Failed to download data for year {0}".format(year))
        # Store the data in a file
        with open(f"{color}_taxi_data_{year}-{month}.parquet", "wb") as file:
            file.write(response.content)
    # Upload the file to Google Cloud Storage
        client = storage.Client()
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(f"raw/{color}_taxi_data_{year}-{month}.parquet")
        blob.upload_from_filename(f"{color}_taxi_data_{year}-{month}.parquet", timeout=300)


# Loop over the years and download and store the data
for year in years:
    download_and_store("yellow", year)
    download_and_store("green", year)
