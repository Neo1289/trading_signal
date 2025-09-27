import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import logging
from pathlib import Path
import os

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())


destination_table = "total_three_divided_btc"

logger = logging.getLogger(__name__)


def fetch_transactions(credentials) -> tuple[pd.DataFrame, int]:
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
        SELECT 
        status_timestamp,
        (data_quote_USD_total_market_cap - (b.market_cap + e.market_cap)) / b.market_cap AS total_three_divided_btc
        
        FROM
        connection-123.signals.cmc_data c
        LEFT JOIN connection-123.signals.bitcoin_price b
          ON b.timestamp = c.status_timestamp
        LEFT JOIN connection-123.signals.ethereum_price e
          ON e.timestamp = c.status_timestamp 
        ORDER BY 1
    """
    query_job = client.query(query)
    results = query_job.result()
    df_transactions_count = results.to_dataframe()

    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")

    return df_transactions_count, bytes_processed


def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'status_timestamp', 'type': 'STRING', 'description': 'The date of the transaction'},
        {'name': 'total_three_divided_btc', 'type': 'FLOAT64', 'description': 'total market cap excluding btc and eth divided btc'}
    ]
    return table_schema


def run_etl(credentials, dataset: str, mode: str) -> int:

    if mode == 'prod':

        table,bytes_processed = fetch_transactions(credentials)
        table_schema = schema()
        target_table = dataset + destination_table

        pandas_gbq.to_gbq(
            dataframe=table,
            destination_table=target_table,
            project_id="connection-123",
            table_schema=table_schema,
            credentials=credentials,
            if_exists="replace"
        )

        return bytes_processed

    else:
        print('test mode')
        table, bytes_processed = fetch_transactions(credentials)
        # Create subdirectory if it doesn't exist
        subdir = local_folder / fetch_transactions.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return bytes_processed
