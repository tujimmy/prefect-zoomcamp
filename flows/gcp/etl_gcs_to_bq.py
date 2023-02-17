from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint

@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Extract from GCS"""
    """dtc_data_lake_root-welder-375217/data\yellow\yellow_tripdata_2021-01.parquet"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    df = pd.read_parquet(path)
    print(f"pre: missing pass count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing pass count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write to BQ"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    print("start")
    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="root-welder-375217",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    print("end")


@flow()
def etl_gcs_to_bq():
    """Main ETL Flow to load data into BigQuery"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()