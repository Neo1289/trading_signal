import logging 
import os
from google.oauth2 import service_account
import pandas as pd
import requests
import pandas_gbq
from google.cloud import bigquery
from typing import Any
import time


logging.basicConfig(
    filename='logfile.txt',  
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dataset = "signals."

from technical_indicators import ( 
    bitcoin_transactions_volume,
    bitcoin_closing_prices,
    btc_moving_averages,
    bitcoin_ema,
    macd,
    rsi,
    bollinger_bands,
    ethereum_closing_prices,
    tether_data,
    cmc_data,
    mvrv_score
    )

def main() -> None:
    
    credentials = service_account.Credentials.from_service_account_file("connection-123-892e002c2def.json")
    
    jobs = [
        bitcoin_transactions_volume,
        bitcoin_closing_prices,
        btc_moving_averages,
        bitcoin_ema,
        macd,
        rsi,
        bollinger_bands,
        ethereum_closing_prices,
        tether_data,
        cmc_data,
        mvrv_score
    ]

    for job in jobs:
        job_name = job.__name__.title()
        logger.info(f"Starting ETL job: {job_name}")
    
        job.run_etl(credentials, dataset)

if __name__ == "__main__":
    main()

