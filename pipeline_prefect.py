from prefect import flow, task
from upload_to_gcs import download_and_upload
from etl_revenue_per_day import calculate_revenue_per_day
from prefect.filesystems import GitHub  # ✅ neuer Import für Prefect 3.4


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
    # Richtige GitHub-Storage-Definition
    repo = GitHub(
        repository="in-mil/mle-data-pipeline-project",  # dein Repo-Name
        reference="main",  # Branch
        access_token=None  # nur nötig bei privaten Repos
    )

    # Deployment anlegen und in Prefect Cloud registrieren
    main_flow.deploy(
        name="nyc-taxi-flow",
        work_pool_name="cloud-pool",
        description="NYC Green Taxi ETL – revenue per day",
        storage=repo,
        image="prefecthq/prefect:3-latest"
    )