import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account
import logging

destination_table = "btc_macd"

logger = logging.getLogger(__name__)


def calculate_macd(credentials) -> pd.DataFrame:
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
      SELECT
      timestamp,
      ema_9,
      ema_12,
      ema_26
    FROM connection-123.signals.btc_ema
    ORDER BY timestamp 
    """
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()

    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")
    # Calculate MACD line
    df['macd_line'] = df['ema_12'] - df['ema_26']
    # Calculate signal line (9-period EMA of MACD line)
    df['signal_line'] = df['macd_line'].ewm(span=9, adjust=False).mean()
    # Calculate histogram
    df['histogram'] = df['macd_line'] - df['signal_line']
    # Return only the MACD components
    macd_result = df[['timestamp', 'macd_line', 'signal_line', 'histogram']].copy()

    return macd_result

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'DATE', 'description': 'The date of the calculation (macd) and line'},
        {'name': 'macd_line', 'type': 'FLOAT64', 'description': 'macd line'},
        {'name': 'signal_line', 'type': 'FLOAT64', 'description': 'signal line'},
        {'name': 'histogram', 'type': 'FLOAT64', 'description': 'histogram line'},
    ]
    return table_schema


def run_etl(credentials, dataset: str) -> None:
    table = calculate_macd(credentials)
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