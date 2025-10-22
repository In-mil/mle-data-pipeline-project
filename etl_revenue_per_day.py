import pandas as pd
from google.cloud import storage
from io import BytesIO

# Dein Bucketname
BUCKET_NAME = "nyc-taxi-pipeline-bucket"

client = storage.Client()

def read_parquet_from_gcs(file_name):
    """Reads a Parquet file directly from GCS"""
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/{file_name}")
    data = blob.download_as_bytes()
    return pd.read_parquet(BytesIO(data))

def calculate_revenue_per_day():
    """Calculates the revenue per day from all three months"""
    all_data = []
    for month in ["01", "02", "03"]:
        file_name = f"green_tripdata_2025-{month}.parquet"
        print(f"📥 Load {file_name} from GCS ...")
        df = read_parquet_from_gcs(file_name)

        # Ensure the time column is correct
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])

        # Extract the date
        df["date"] = df["lpep_pickup_datetime"].dt.date

        # Calculate the sum per day
        revenue = df.groupby("date")["total_amount"].sum().reset_index()
        all_data.append(revenue)

    # Merge all months
    all_revenue = pd.concat(all_data)
    result = all_revenue.groupby("date")["total_amount"].sum().reset_index()

    # Save the result to a CSV file
    result.to_csv("revenue_per_day_2025.csv", index=False)
    print("✅ Saved as revenue_per_day_2025.csv")

if __name__ == "__main__":
    calculate_revenue_per_day()