from prefect import flow, task
from upload_to_gcs import download_and_upload
from etl_revenue_per_day import calculate_revenue_per_day


@task
def upload_task():
    download_and_upload()


@task
def etl_task():
    calculate_revenue_per_day()


@flow
def main_flow():
    upload_task()
    etl_task()


if __name__ == "__main__":
    # Deployment direkt aus dem Flow heraus erstellen
    main_flow.deploy(
        name="nyc-taxi-flow",
        work_pool_name="cloud-pool",
        description="NYC Green Taxi ETL – revenue per day",
    )

    # Optional: lokalen Testlauf starten
    # main_flow()

#execute the script with: python pipeline_prefect.py