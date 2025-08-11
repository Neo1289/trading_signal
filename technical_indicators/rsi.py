import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account
import logging

destination_table = "btc_rsi"

logger = logging.getLogger(__name__)

def calculate_rsi(credentials) -> pd.DataFrame:
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

    return rsi_result

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'DATE', 'description': 'The date of the calculation rsi'},
        {'name': 'rsi', 'type': 'FLOAT64', 'description': 'rsi indicator 14 periods'}

    ]
    return table_schema

def run_etl(credentials, dataset: str) -> None:
    table = calculate_rsi(credentials)
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