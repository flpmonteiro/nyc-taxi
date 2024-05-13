import os
from pathlib import Path

import pandas as pd
import requests

from dagster import asset, AssetExecutionContext

# For resources
from dagster_duckdb import DuckDBResource
from dagster import EnvVar


# for partitions
from dagster import MonthlyPartitionsDefinition

# for jobs
from dagster import AssetSelection, define_asset_job

# for schedule
from dagster import ScheduleDefinition

from . import constants

# TODO: get from .env file
start_date = '2023-01-01'
end_date = '2023-12-31'

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)

monthly_job = define_asset_job(
    name='monthly_job',
    partitions_def=monthly_partition,
    selection=AssetSelection.all(),
)

trips_update_schedule = ScheduleDefinition(
    job=monthly_job,
    cron_schedule="0 0 5 * *", # at 00:00, every 5th of the month, every month, every year
)

@asset(
    partitions_def=monthly_partition,
)
def download_data(context: AssetExecutionContext):
    year_month = context.partition_key[:-3]
    color = 'yellow'
    url = constants.DOWNLOAD_URL.format(color,year_month)
    file_name = Path(url).name
    data_raw_path = Path('data/raw')
    full_path = data_raw_path / f'{year_month[:4]}' / f'{year_month[5:]}'
    full_path.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(full_path/file_name, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error: {e}")
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")


database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
)

@asset(
    deps=['download_data'],
    partitions_def=monthly_partition,
)
def taxi_trips(context: AssetExecutionContext, database:DuckDBResource) -> None:
    year_month = context.partition_key[:-3]

    query = f"""
        create table if not exists trips (
            vendor_id integer,
            pickup_zone_id integer,
            dropoff_zone_id integer,
            rate_code_id double,
            payment_type integer,
            dropoff_datetime timestamp,
            pickup_datetime timestamp,
            trip_distance double,
            passenger_count double,
            total_amount double,
            partition_date varchar
        );

        delete from trips where partition_date = '{month_to_fetch}';

        insert into trips
        select
            VendorID,
            PULocationID,
            DOLocationID,
            RatecodeID,
            payment_type,
            tpep_dropoff_datetime,
            tpep_pickup_datetime,
            trip_distance,
            passenger_count,
            total_amount,
            '{month_to_fetch}' as partition_date
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)
