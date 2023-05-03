from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read station data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["DATE"] = pd.to_datetime(df["DATE"])
    df["ELEVATION"] = pd.to_numeric(df["ELEVATION"], downcast='integer')
    df["LATITUDE"] = pd.to_numeric(df["LATITUDE"], downcast='float')
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], downcast='float')
    df["TMAX"] = pd.to_numeric(df["TMAX"], downcast='float')
    df["TMIN"] = pd.to_numeric(df["TMIN"], downcast='float')
    df["TAVG"] = pd.to_numeric(df["TAVG"], downcast='float')
    df["PRCP"] = pd.to_numeric(df["PRCP"], downcast='integer')
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zc-prefect-climate")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    station = "RSM00024688" #This is the coldest place in the world called Ojmjakon, there will be a search later for all the stations of interest.
    dataset_file = f"{station}"
    dataset_url = f"https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{station}.csv"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()