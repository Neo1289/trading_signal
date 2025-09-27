import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import logging
import os
from pathlib import Path

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())


destination_table = "btc_ema"

logger = logging.getLogger(__name__)

def calculate_ema(credentials,periods=[9,12, 26, 20, 50, 200]) -> tuple[pd.DataFrame,int]:
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
    ema = results.to_dataframe()
    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")

    # Calculate EMA for each period
    for period in periods:
        ema_column = f'ema_{period}'
        # Calculate EMA using the 'price' column
        ema[ema_column] = ema['price'].ewm(span=period, adjust=False).mean()
    
    return ema, bytes_processed

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'DATE', 'description': 'The date of the ema'},
        {'name': 'price', 'type': 'FLOAT64', 'description': 'Closing price for the given interval'},
        {'name': 'ema_9', 'type': 'FLOAT64', 'description': 'exponential moving average 9 periods'},
        {'name': 'ema_12', 'type': 'FLOAT64', 'description': 'exponential moving average 12 periods'},
        {'name': 'ema_26', 'type': 'FLOAT64', 'description': 'exponential moving average 26 periods'},
        {'name': 'ema_20', 'type': 'FLOAT64', 'description': 'exponential moving average 20 periods'},
        {'name': 'ema_50', 'type': 'FLOAT64', 'description': 'exponential moving average 50 periods'},
        {'name': 'ema_200', 'type': 'FLOAT64', 'description': 'exponential moving average 200 periods'}
    ]
    return table_schema

def run_etl(credentials,dataset:str,mode:str) -> int:

    if mode == 'prod':

        table,bytes_processed = calculate_ema(credentials)
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
        table,bytes_processed = calculate_ema(credentials,periods=[9,12, 26, 20, 50, 200])
        # Create subdirectory if it doesn't exist
        subdir = local_folder / calculate_ema.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")

        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return bytes_processed
