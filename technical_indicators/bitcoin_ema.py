import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account
import logging
from datetime import datetime, timedelta

destination_table = "btc_ema"

logger = logging.getLogger(__name__)

def calculate_ema(credentials,periods=[9,12, 26, 20, 50, 200]) -> pd.DataFrame:
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

def run_etl(credentials,dataset:str) -> None:
   
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