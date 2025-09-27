import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import logging
from pathlib import Path
import os

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())


destination_table = "btc_bollinger_bands"

logger = logging.getLogger(__name__)

def calculate_bollinger_bands(credentials) -> tuple[pd.DataFrame, int]:
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
    SELECT 
        timestamp, 
        price 
    FROM 
        `connection-123.signals.bitcoin_price` 
    WHERE 
        DATE(timestamp) < DATE(CURRENT_TIMESTAMP())
    ORDER BY 
        timestamp ASC
    """

    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")

    # Calculate 20-period Simple Moving Average (Middle Band)
    df['middle_band'] = df['price'].rolling(window=20).mean()

    # Calculate 20-period Standard Deviation
    df['std_dev'] = df['price'].rolling(window=20).std()

    # Calculate Upper and Lower Bands (2 standard deviations)
    df['upper_band'] = df['middle_band'] + (2 * df['std_dev'])
    df['lower_band'] = df['middle_band'] - (2 * df['std_dev'])

    # Calculate Bollinger Band Width (optional indicator)
    df['bb_width'] = df['upper_band'] - df['lower_band']

    # Calculate %B (position within bands, optional)
    df['percent_b'] = (df['price'] - df['lower_band']) / (df['upper_band'] - df['lower_band'])

    # Return Bollinger Bands components with timestamp
    bollinger_result = df[['timestamp', 'price', 'middle_band', 'upper_band', 'lower_band', 'bb_width', 'percent_b']].copy()

    return bollinger_result, bytes_processed

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'DATE', 'description': 'The date of the calculation rsi'},
        {'name': 'price', 'type': 'FLOAT64', 'description': 'bitcoin closing price'},
        {'name': 'middle_band', 'type': 'FLOAT64', 'description': 'sma 20 periods for btc closing price'},
        {'name': 'upper_band', 'type': 'FLOAT64', 'description': 'Middle Band + (2 * 20-period Standard Deviation)'},
        {'name': 'lower_band', 'type': 'FLOAT64', 'description': 'Middle Band - (2 * 20-period Standard Deviation)'}
    ]
    return table_schema

def run_etl(credentials, dataset: str, mode: str) -> int:

    if mode == 'prod':

        table, bytes_processed = calculate_bollinger_bands(credentials)
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
        table, bytes_processed = calculate_bollinger_bands(credentials)
        # Create subdirectory if it doesn't exist
        subdir = local_folder / calculate_bollinger_bands.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return bytes_processed
