# etl_revenue_per_day.py
import os
import pandas as pd
from google.cloud import storage


def _upload_to_gcs(bucket_name: str, local_path: str, dest_path: str) -> None:
    """Upload a local file to GCS at gs://{bucket_name}/{dest_path}."""
    client = storage.Client()  # uses ADC (gcloud auth application-default login)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded result to GCS: gs://{bucket_name}/{dest_path}")


def calculate_revenue_per_day() -> str:
    """
    Load 2025-01..03 green taxi parquet files from GCS,
    aggregate revenue per day, save CSV locally,
    then upload CSV to GCS under results/.
    Returns the GCS destination path of the CSV.
    """
    bucket = os.getenv("GCS_BUCKET", "nyc-taxi-pipeline-bucket")
    raw_prefix = "raw"
    result_prefix = "results"
    local_csv = "revenue_per_day_2025.csv"
    dest_csv = f"{result_prefix}/{local_csv}"

    # --- Load data from GCS (requires gcsfs) ---
    months = ["01", "02", "03"]
    dfs = []
    for m in months:
        path = f"gs://{bucket}/{raw_prefix}/green_tripdata_2025-{m}.parquet"
        print(f"📥 Load green_tripdata_2025-{m}.parquet from GCS ...")
        df = pd.read_parquet(path, storage_options={"token": "google_default"})
        dfs.append(df)

    data = pd.concat(dfs, ignore_index=True)

    # --- Basic cleaning / typing ---
    # Green Taxi has pickup column "lpep_pickup_datetime" and fare columns like "total_amount"
    data["lpep_pickup_datetime"] = pd.to_datetime(data["lpep_pickup_datetime"], errors="coerce")
    data["pickup_date"] = data["lpep_pickup_datetime"].dt.date

    # Ensure numeric
    for col in ["total_amount"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0)

    # --- Aggregate revenue per day ---
    rev = (
        data.groupby("pickup_date", dropna=False)["total_amount"]
        .sum()
        .reset_index()
        .rename(columns={"total_amount": "revenue"})
        .sort_values("pickup_date")
    )

    # --- Save locally ---
    rev.to_csv(local_csv, index=False)
    print(f"✅ Saved as {local_csv}")

    # --- Upload CSV to GCS ---
    _upload_to_gcs(bucket_name=bucket, local_path=local_csv, dest_path=dest_csv)

    return f"gs://{bucket}/{dest_csv}"