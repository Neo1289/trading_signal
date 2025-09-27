import pandas as pd
import requests
from google.cloud import bigquery
import os
import logging
from datetime import datetime
from pathlib import Path

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())

destination_table = "mvrv_score"

logger = logging.getLogger(__name__)

def fetch_mvrv() -> pd.DataFrame:
    url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"

    params = {
        'assets': 'btc',
        'metrics': 'CapMVRVCur',
        'start_time': '2020-01-01',
        'end_time': datetime.now().strftime('%Y-%m-%d'),
        'frequency': '1d'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame([
        {
            'time': point['time'],
            'mvrv': float(point['CapMVRVCur'])
        }
        for point in data['data']
    ])

    df['time'] = pd.to_datetime(df['time']).dt.date

    logger.info(f"imported {len(df)} rows of data from {url}")

    return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'time', 'type': 'DATE', 'description': 'The date of measurement'},
        {'name': 'mvrv', 'type': 'FLOAT64', 'description': 'decimal value of the mvrv daily'},
    ]

    return table_schema

def run_etl(credentials,dataset:str,mode:str) -> int:

    if mode == 'prod':

        project = "connection-123"
        client = bigquery.Client(credentials=credentials, project=project)
        table = fetch_mvrv()
        table_schema = schema()

        job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="time"
            ),
            destination_table_description="Daily value for the mvrv score"
        )

        table_ref = dataset + destination_table

        job = client.load_table_from_dataframe(
            table,
            table_ref,
            job_config=job_config
        )

        job.result()

        return 0

    else:
        print('test mode')
        table = fetch_mvrv()
        # Create subdirectory if it doesn't exist
        subdir = local_folder / fetch_mvrv.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return 0
