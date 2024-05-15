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
end_date = '2024-01-01'

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
    data_raw_path.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(data_raw_path/file_name, 'wb') as file:
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
    color = 'yellow'
    year_month = context.partition_key[:-3]
    table_name = f'{color}_tripdata'

    query = f"""
        create table if not exists {table_name} (
            vendor_id integer,
            pickup_datetime timestamp,
            dropoff_datetime timestamp,
            passenger_count bigint,
            trip_distance double,
            ratecode_id bigint,
            store_and_fwd_flag varchar,
            pickup_location_id integer,
            dropoff_location_id integer,
            payment_type bigint,
            fare_amount double,
            extra double,
            mta_tax double,
            tip_amount double,
            tolls_amount double,
            improvement_surcharge double,
            total_amount double,
            congestion_surcharge double,
            airport_fee double,
            partition_date varchar
        );

        delete from {table_name} where partition_date = '{year_month}';

        insert into {table_name}
        select
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee,
            '{year_month}' as partition_date
        from '{constants.TRIPDATA_FILE_PATH.format(color, year_month)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)

@asset
def download_zones_csv() -> None:
    url = constants.TAXI_ZONES_URL
    file_name = Path(url).name
    data_raw_path = Path('data/raw')
    data_raw_path.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(data_raw_path/file_name, 'wb') as file:
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


@asset(
    deps=['download_zones_csv'],
)
def load_taxi_zones(database:DuckDBResource) -> None:

    query = f"""
        create table if not exists taxi_zones (
            location_id integer,
            borough varchar,
            zone varchar,
            service_zone varchar,
        );

        insert into taxi_zones
        select
            LocationID,
            Borough,
            Zone,
            service_zone
        from '{constants.TAXI_ZONES_FILE_PATH}';
    """

    with database.get_connection() as conn:
        conn.execute(query)