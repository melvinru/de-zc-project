from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests


@task()
def fetch_station_list() -> dict:
    """Fetches and returns a dictionary of station details."""
    url = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/doc/ghcnd-stations.txt"
    response = requests.get(url)
    lines = response.text.split("\n")
    stations = {}

    for line in lines:
        if line:
            station_id = line[:11].strip()
            latitude = line[12:20].strip()
            longitude = line[21:30].strip()
            elevation = line[31:37].strip()
            stations[station_id] = {
                "latitude": latitude,
                "longitude": longitude,
                "elevation": elevation,
            }

    return stations


@task()
def generate_dataset_url(station: str) -> str:
    """Generates dataset URL"""
    return f"https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{station}.csv"


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read station data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["DATE"] = pd.to_datetime(df["DATE"])
    df["ELEVATION"] = pd.to_numeric(df["ELEVATION"], downcast="integer")
    df["LATITUDE"] = pd.to_numeric(df["LATITUDE"], downcast="float")
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], downcast="float")
    df["TMAX"] = pd.to_numeric(df["TMAX"], downcast="float")
    df["TMIN"] = pd.to_numeric(df["TMIN"], downcast="float")
    df["TAVG"] = pd.to_numeric(df["TAVG"], downcast="float")
    df["PRCP"] = pd.to_numeric(df["PRCP"], downcast="integer")
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


@task()
def etl_process(df, dataset_file):
    """Process the ETL steps."""
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)


@flow()
def etl_web_to_gcs(station: str = "RSM00024688") -> None:
    """The main ETL function."""
    stations = fetch_station_list()

    if station not in stations:
        print(
            f"Could not find station '{station}'. Please check the station ID and try again."
        )
        return

    dataset_file = station
    dataset_url = generate_dataset_url(station)
    df = fetch(dataset_url)
    etl_process(df, dataset_file)


if __name__ == "__main__":
    etl_web_to_gcs()
