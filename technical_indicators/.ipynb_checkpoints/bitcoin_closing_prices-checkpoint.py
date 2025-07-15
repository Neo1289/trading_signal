import pandas as pd
import requests
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import os
import logging
from google.oauth2 import service_account

destination_table = "bitcoin_price"

def fetch_bitcoin_price() -> pd.DataFrame:
    url = 'https://api.coingecko.com/api/v3/coins/bitcoin/ohlc'
    params = {
        'vs_currency': 'usd',
        'days': '365'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # OHLC data format: [timestamp, open, high, low, close]
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date.astype(str)
    
    # Keep only timestamp and closing price to match original structure
    df = df[['timestamp', 'close']].rename(columns={'close': 'price'})
    df = df.drop_duplicates(subset='timestamp', keep='first')
    return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'STRING', 'description': 'The date of the price'},
        {'name': 'price', 'type': 'FLOAT64', 'description': 'closing price'}
    ]
    return table_schema

def run_etl(credentials,dataset:str) -> None:
   
    table = fetch_bitcoin_price()
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
