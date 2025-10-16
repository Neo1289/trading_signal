import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import logging
import os
from pathlib import Path

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())

destination_table = "gspc"

logger = logging.getLogger(__name__)

def fetch_gspc(ticker:str = "^GSPC", period:str = "3y") -> pd.DataFrame:

    # Download S&P 500 data
    ticker = yf.Ticker(ticker)  # ^GSPC is the S&P 500 index symbol
    df = ticker.history(period=period)

    close_prices = df['Close']
    df = pd.DataFrame(close_prices)
    df = df.reset_index().rename(columns={'Date': 'date', 'Close': 'close'})

    return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'date', 'type': 'DATE', 'description': 'The date of the closing price'},
        {'name': 'close', 'type': 'FLOAT64', 'description': 'sp500 daily closing price'},

    ]
    return table_schema


def run_etl(credentials,dataset:str,mode:str) -> int:

    if mode == 'prod':

        project = "connection-123"
        client = bigquery.Client(credentials=credentials, project=project)
        table = fetch_gspc()
        table_schema = schema()

        job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date"
            ),
            destination_table_description="daily figures for the sp500 (last 20 years)"
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
        table = fetch_gspc()
        # Create subdirectory if it doesn't exist
        subdir = local_folder / fetch_gspc.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return 0
