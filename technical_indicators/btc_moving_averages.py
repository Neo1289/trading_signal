import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account
import logging

destination_table = "btc_moving_averages"

logger = logging.getLogger(__name__)

def calculate_ma(credentials) -> pd.DataFrame:
   
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
        SELECT 
      timestamp AS date_,
      price,
      AVG(price) OVER (
        ORDER BY timestamp 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
      ) AS sma_10,
      AVG(price) OVER (
        ORDER BY timestamp 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
      ) AS sma_20,
      AVG(price) OVER (
        ORDER BY timestamp 
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
      ) AS sma_50
    FROM `connection-123.signals.bitcoin_price`
    WHERE timestamp < CURRENT_DATE()
    ORDER BY timestamp DESC
    """
    query_job = client.query(query)
    results = query_job.result()
    mas = results.to_dataframe()

    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")

    return mas, bytes_processed

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'date_', 'type': 'DATE', 'description': 'The date of the closing price'},
        {'name': 'price', 'type': 'FLOAT64', 'description': 'Closing price for the given interval'},
        {'name': 'sma_10', 'type': 'FLOAT64', 'description': 'simple moving average 10 periods'},
        {'name': 'sma_20', 'type': 'FLOAT64', 'description': 'simple moving average 20 periods'},
        {'name': 'sma_50', 'type': 'FLOAT64', 'description': 'simple moving average 50 periods'}
    ]
    return table_schema

def run_etl(credentials,dataset:str,mode:str) -> None:

    if mode == 'prod':

        table, bytes_processed = calculate_ma(credentials)
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
        print('no production mode')
