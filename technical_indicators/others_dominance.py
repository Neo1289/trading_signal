import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account
import logging

destination_table = "others_dominance"

logger = logging.getLogger(__name__)

def fetch_data(credentials) -> pd.DataFrame:
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
        SELECT
        status_timestamp,
        data_eth_dominance,
        data_btc_dominance
        FROM connection-123.signals.cmc_data
    """
    query_job = client.query(query)
    results = query_job.result()
    dominance_data = results.to_dataframe()

    dominance_data['others_dominance'] = 100 - (dominance_data['data_eth_dominance'] + dominance_data['data_btc_dominance'])

    bytes_processed = query_job.total_bytes_processed
    logger.info(f"Query processed {bytes_processed:,} bytes ({bytes_processed / 1024 / 1024:.2f} MB)")

    return dominance_data, bytes_processed

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'status_timestamp', 'type': 'DATE', 'description': 'The date of the reading'},
        {'name': 'data_eth_dominance', 'type': 'FLOAT64', 'description': 'daily eth dominance'},
        {'name': 'data_btc_dominance', 'type': 'FLOAT64', 'description': 'daily btc dominance'},
        {'name': 'others_dominance', 'type': 'FLOAT64', 'description': 'ddaily others dominance'},
    ]
    return table_schema

def run_etl(credentials, dataset: str, mode: str) -> None:

    if mode == 'prod':

        table, bytes_processed = fetch_data(credentials)
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
