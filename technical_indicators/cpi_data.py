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
api_key_path = current_dir.parent / "fred_api_key.txt"
api_key_path_str = str(api_key_path.resolve())

with open(api_key_path_str, 'r') as f:
    api_key = f.read().strip()

base_url = "https://api.stlouisfed.org/fred/"

destination_table = "cpi_data"

logger = logging.getLogger(__name__)

def fred_request(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if params is None:
        params = {}

    params['api_key'] = api_key
    params['file_type'] = 'json'

    url = base_url + endpoint
    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

def get_cpi_data(series_id: str = "CPILFESL") -> Optional[pd.DataFrame]:
    endpoint = "series/observations"
    params = {
        "series_id": series_id,
        "observation_start": "2023-01-01",
        "frequency": "m",
        "units": "pc1"
    }
    result = fred_request(endpoint, params)

    if result and 'observations' in result:
        print(f"Retrieved {len(result['observations'])} observations for {series_id}")

        # Convert to DataFrame
        df = pd.DataFrame(result['observations'])
        df = df[['date', 'value']]
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'])

        logger.info(f"imported {len(df)} rows of data from {series_id}")

        return df

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'date', 'type': 'DATE', 'description': 'The date of the measurement'},
        {'name': 'value', 'type': 'FLOAT64', 'description': 'the value of the CPI measured'}
    ]
    return table_schema

def run_etl(credentials,dataset:str) -> None:
    project = "connection-123"
    client = bigquery.Client(credentials=credentials, project=project)
    table = get_cpi_data()
    table_schema = schema()

    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        ),
        destination_table_description="CPI data monthly reading"
    )

    table_ref = dataset + destination_table

    job = client.load_table_from_dataframe(
        table,
        table_ref,
        job_config=job_config
    )

    job.result()

