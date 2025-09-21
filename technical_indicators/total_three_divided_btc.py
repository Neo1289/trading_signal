import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account
import logging

destination_table = "total_three_divided_btc"

logger = logging.getLogger(__name__)


def fetch_transactions(credentials) -> pd.DataFrame:
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


def run_etl(credentials, dataset: str) -> None:
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

