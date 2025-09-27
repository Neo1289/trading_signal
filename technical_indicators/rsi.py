import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import logging
from pathlib import Path
import os

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())


destination_table = "btc_rsi"

logger = logging.getLogger(__name__)

def calculate_rsi(credentials) -> tuple[pd.DataFrame, int]:
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
    SELECT
    timestamp,
    price
    FROM connection-123.signals.bitcoin_price
    WHERE DATE(timestamp) < DATE(CURRENT_TIMESTAMP())
    ORDER BY 1
    """
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()

    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")

    # Calculate price changes
    df['price_change'] = df['price'].diff()

    # Separate gains and losses
    df['gain'] = df['price_change'].where(df['price_change'] > 0, 0)
    df['loss'] = (-df['price_change']).where(df['price_change'] < 0, 0)

    # Calculate 14-period average gains and losses using exponential moving average
    df['avg_gain'] = df['gain'].ewm(span=14, adjust=False).mean()
    df['avg_loss'] = df['loss'].ewm(span=14, adjust=False).mean()

    # Calculate RSI
    df['rs'] = df['avg_gain'] / df['avg_loss']
    df['rsi_14'] = 100 - (100 / (1 + df['rs']))

    # Return timestamp and RSI
    rsi_result = df[['timestamp', 'rsi_14']].copy()

    return rsi_result, bytes_processed

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'DATE', 'description': 'The date of the calculation rsi'},
        {'name': 'rsi', 'type': 'FLOAT64', 'description': 'rsi indicator 14 periods'}

    ]
    return table_schema

def run_etl(credentials, dataset: str, mode: str) -> int:

    if mode == 'prod':

        table, bytes_processed = calculate_rsi(credentials)
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
        table, bytes_processed = calculate_rsi(credentials)
        # Create subdirectory if it doesn't exist
        subdir = local_folder / calculate_rsi.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return bytes_processed
