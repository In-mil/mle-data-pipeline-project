from google.cloud import storage
import requests
import os


BUCKET_NAME = "nyc-taxi-pipeline-bucket"

# URLs der Green Taxi Daten – erste 3 Monate 2025
URLS = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-03.parquet"
]

def upload_to_gcs(bucket_name, source_file, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"✅ Uploaded to GCS: {destination_blob}")

def download_and_upload():
    for url in URLS:
        file_name = url.split("/")[-1]
        print(f"⬇️ Downloading: {file_name}")
        r = requests.get(url)
        with open(file_name, "wb") as f:
            f.write(r.content)
        print(f"📦 Uploading to GCS...")
        upload_to_gcs(BUCKET_NAME, file_name, f"raw/{file_name}")
        os.remove(file_name)
        print(f"🧹 File deleted: {file_name}")

if __name__ == "__main__":
    download_and_upload()

#execute the script with: python upload_to_gcs.py