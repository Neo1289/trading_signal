import pandas as pd
import requests
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import os
import logging
from google.oauth2 import service_account

destination_table = "bitcoin_price"

logger = logging.getLogger(__name__)

def fetch_bitcoin_price() -> pd.DataFrame:
    url = 'https://api.coingecko.com/api/v3/coins/bitcoin/market_chart'
    params = {
        'vs_currency': 'usd',
        'days': '365',
        'interval': 'daily'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Extract prices, market caps, and total volumes
    prices = data['prices']
    market_caps = data['market_caps']
    total_volumes = data['total_volumes']
    
    # Create DataFrames for each metric
    df_prices = pd.DataFrame(prices, columns=['timestamp', 'price'])
    df_market_caps = pd.DataFrame(market_caps, columns=['timestamp', 'market_cap'])
    df_volumes = pd.DataFrame(total_volumes, columns=['timestamp', 'total_volume'])
    
    # Merge all data on timestamp
    df = df_prices.merge(df_market_caps, on='timestamp').merge(df_volumes, on='timestamp')
    
    # Convert timestamp to date
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
    
    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)

    logger.info(f"imported {len(df)} rows of data from {url}")
    
    return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'timestamp', 'type': 'DATE', 'description': 'The date of the price'},
        {'name': 'price', 'type': 'FLOAT64', 'description': 'closing price'},
        {'name': 'market_cap', 'type': 'FLOAT64', 'description': 'market cap for the daily timeframe'},
        {'name': 'total_volume', 'type': 'FLOAT64', 'description': 'total volume of transactions happened daily'}
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
