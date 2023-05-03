from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(station) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"{station}.parquet"
    gcs_block = GcsBucket.load("de-zc-prefect-climate")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../")
    return Path(f"../{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    df = df.drop(columns=['PRCP_ATTRIBUTES', 'SNWD', 'SNWD_ATTRIBUTES', 'TMAX_ATTRIBUTES', 'TMIN_ATTRIBUTES', 'TAVG_ATTRIBUTES'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-zc-prefect-cred")

    df.to_gbq(
        destination_table="de_zc_project_stage.climate",
        project_id="titanium-vortex-385513",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=5_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    station = "RSM00024688"

    path = extract_from_gcs(station)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()