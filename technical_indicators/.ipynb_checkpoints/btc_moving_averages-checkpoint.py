import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account
import logging

destination_table = "btc_moving_averages"

logger = logging.getLogger(__name__)

def get_closing_prices(credentials) -> pd.DataFrame:
   
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
    WITH data_ AS (
        SELECT * FROM connection-123.signals.bitcoin_price ORDER BY timestamp)
        SELECT 
      timestamp,
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
    FROM data_
ORDER BY timestamp DESC
    """
    query_job = client.query(query)
    results = query_job.result()
    bitcoin_prices = results.to_dataframe()

    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")

    return bitcoin_prices