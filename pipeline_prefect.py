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
    main_flow()
