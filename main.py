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
    mvrv_score,
    others_dominance,
    total_three_divided_btc
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
        mvrv_score,
        others_dominance,
        total_three_divided_btc
    ]

    total_bytes_processed = 0

    for job in jobs:
        job_name = job.__name__.title()
        logger.info(f"Starting ETL job: {job_name}")

        bytes_processed = job.run_etl(credentials, dataset)
        total_bytes_processed += bytes_processed

    logger.info(f"Total bytes processed across all jobs: {total_bytes_processed / 1024 / 1024:.2f} MB)")

if __name__ == "__main__":
    main()

