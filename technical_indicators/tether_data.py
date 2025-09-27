import pandas as pd
import requests
from google.cloud import bigquery
import logging
import os
from pathlib import Path

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())


destination_table = "tether_data"

logger = logging.getLogger(__name__)

def fetch_tether_data() -> pd.DataFrame:
    url = 'https://api.coingecko.com/api/v3/coins/tether/market_chart'
    params = {
        'vs_currency': 'usd',
        'days': '365',
        'interval': 'daily'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # Extract  market caps, and total volumes
    market_caps = data['market_caps']
    total_volumes = data['total_volumes']

    # Create DataFrames for each metric
    df_market_caps = pd.DataFrame(market_caps, columns=['timestamp', 'market_cap'])
    df_volumes = pd.DataFrame(total_volumes, columns=['timestamp', 'total_volume'])

    # Merge all data on timestamp
    df = df_market_caps.merge(df_volumes, on='timestamp')

    # Parse timestamps in UTC, normalize to midnight, then shift back one day
    ts_utc = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    dates_utc = ts_utc.dt.normalize() - pd.Timedelta(days=1)

    # Use the shifted date as the label for the "closing" day
    df['date'] = dates_utc.dt.date
    df = df.drop(columns=['timestamp']).sort_values('date').reset_index(drop=True)

    # Ensure one row per date (in case of any duplicates)
    df = df.groupby('date', as_index=False).last()
    df = df.rename(columns={'date': 'timestamp'})

    logger.info(f"imported {len(df)} rows of data from {url}")

    return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'DATE', 'description': 'The date of the price'},
        {'name': 'market_cap', 'type': 'FLOAT64', 'description': 'market cap for the daily timeframe'},
        {'name': 'total_volume', 'type': 'FLOAT64', 'description': 'total volume of transactions happened daily'}
    ]
    return table_schema


def run_etl(credentials,dataset:str,mode:str) -> int:

    if mode == 'prod':

        project = "connection-123"
        client = bigquery.Client(credentials=credentials, project=project)
        table = fetch_tether_data()
        table_schema = schema()

        job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            ),
            destination_table_description="market cap and daily volume for usdt"
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
        table = fetch_tether_data()
        # Create subdirectory if it doesn't exist
        subdir = local_folder / fetch_tether_data.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return 0
