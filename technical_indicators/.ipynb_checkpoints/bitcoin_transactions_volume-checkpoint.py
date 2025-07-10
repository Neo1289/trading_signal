import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from typing import Any
from google.oauth2 import service_account

destination_table = "bitcoin_transactions"

def fetch_transactions(credentials) -> pd.DataFrame:
   
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = """
        SELECT 
          DATE(timestamp) as date_,
          SUM(transaction_count) as total_transactions
        FROM 
          `bigquery-public-data.crypto_bitcoin.blocks`
        WHERE 
          timestamp_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
        GROUP BY 
          1
        ORDER BY 
          1
    """
    query_job = client.query(query)
    results = query_job.result()
    df_transactions_count = results.to_dataframe()
    df_transactions_count['date_'] = df_transactions_count['date_'].astype(str)
    
    return df_transactions_count

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'date_', 'type': 'STRING', 'description': 'The date of the transaction'},
        {'name': 'total_transactions', 'type': 'INT64', 'description': 'Total number of daily bitcoin transactions'}
    ]
    return table_schema

def run_etl(credentials,dataset:str) -> None:
    table = fetch_transactions(credentials)
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

