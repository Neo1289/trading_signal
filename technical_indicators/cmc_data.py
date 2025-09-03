import json
import os
import requests
import pandas as pd
from datetime import datetime
import numpy as np
from typing import List, Dict, TypedDict, Optional, Any
import logging
from google.oauth2 import service_account
import pandas_gbq
from pathlib import Path
from google.cloud import bigquery

current_dir = Path(__file__).parent
api_key_path = current_dir.parent / "cmc.txt"
api_key_path_str = str(api_key_path.resolve())

with open(api_key_path_str, 'r') as f:
    api_key = f.read().strip()

destination_table = "cmc_data"

def fetch_cmc_data() -> dict:

    url = 'https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest'

    headers = {
        'X-CMC_PRO_API_KEY': api_key,
        'Accept': 'application/json'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    df = pd.json_normalize(response.json(), sep='_')

    df['status_timestamp'] = pd.to_datetime(df['status_timestamp'], utc=True).dt.date

    columns = [
        'status_timestamp', 'data_eth_dominance', 'data_btc_dominance',
        'data_defi_volume_24h', 'data_defi_market_cap', 'data_stablecoin_volume_24h',
        'data_stablecoin_market_cap', 'data_derivatives_volume_24h',
        'data_quote_USD_total_market_cap', 'data_quote_USD_total_volume_24h',
        'data_quote_USD_altcoin_volume_24h', 'data_quote_USD_altcoin_market_cap',
        'data_quote_USD_defi_market_cap'
    ]

    return df[columns]

def schema() -> list[dict]:

    table_schema = [
        {'name': 'status_timestamp', 'type': 'DATE',
         'description': 'date for the API response (e.g. 2025-09-03)'},
        {'name': 'data_eth_dominance', 'type': 'FLOAT64',
         'description': 'Ethereum market cap dominance as percent (e.g. 18.5 -> 18.5%)'},
        {'name': 'data_btc_dominance', 'type': 'FLOAT64',
         'description': 'Bitcoin market cap dominance as percent (e.g. 40.2 -> 40.2%)'},
        {'name': 'data_defi_volume_24h', 'type': 'FLOAT64',
         'description': '24h total DeFi trading volume in native units (converted to quote currency when applicable)'},
        {'name': 'data_defi_market_cap', 'type': 'FLOAT64',
         'description': 'Total DeFi market capitalization (in quote currency, e.g. USD)'},
        {'name': 'data_stablecoin_volume_24h', 'type': 'FLOAT64',
         'description': '24h stablecoin trading volume (in quote currency)'},
        {'name': 'data_stablecoin_market_cap', 'type': 'FLOAT64',
         'description': 'Total stablecoin market capitalization (in quote currency)'},
        {'name': 'data_derivatives_volume_24h', 'type': 'FLOAT64',
         'description': '24h derivatives volume (in quote currency)'},
        {'name': 'data_quote_USD_total_market_cap', 'type': 'FLOAT64',
         'description': 'Total cryptocurrency market capitalization quoted in USD'},
        {'name': 'data_quote_USD_total_volume_24h', 'type': 'FLOAT64',
         'description': 'Total 24h cryptocurrency trading volume quoted in USD'},
        {'name': 'data_quote_USD_altcoin_volume_24h', 'type': 'FLOAT64',
         'description': '24h trading volume for altcoins quoted in USD'},
        {'name': 'data_quote_USD_altcoin_market_cap', 'type': 'FLOAT64',
         'description': 'Altcoin total market capitalization quoted in USD'},
        {'name': 'data_quote_USD_defi_market_cap', 'type': 'FLOAT64',
         'description': 'DeFi total market capitalization quoted in USD'},
    ]
    return table_schema

def run_etl(credentials,dataset:str) -> None:
    project = "connection-123"
    client = bigquery.Client(credentials=credentials, project=project)
    table = fetch_cmc_data()
    table_schema = schema()

    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="status_timestamp"
        ),
        destination_table_description="Global snapshot of CoinMarketCap aggregated metrics (market caps, 24h volumes, dominance and sector-specific caps) quoted in USD. One row per API response"
    )

    table_ref = dataset + destination_table

    job = client.load_table_from_dataframe(
        table,
        table_ref,
        job_config=job_config
    )

    job.result()
