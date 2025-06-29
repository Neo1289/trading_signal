import logging 
import os
from google.oauth2 import service_account
import pandas as pd
import requests
import pandas_gbq
from google.cloud import bigquery
from typing import Any


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
   
    )

def main() -> None:
    credentials = service_account.Credentials.from_service_account_file("connection-123-892e002c2def.json")

    jobs = [
        bitcoin_transactions_volume,
        
    ]
    
    for job in jobs:
        job.run_etl(credentials,dataset)
        logger.info(job.run_etl.__doc__)
        logger.info(job.__file__)
        
if __name__ == "__main__":
    main()