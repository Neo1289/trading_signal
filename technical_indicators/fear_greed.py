import pandas as pd
import requests
import pandas_gbq
import os
import logging
from pathlib import Path
current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())

destination_table = "fear_greed"

logger = logging.getLogger(__name__)

def fetch_fear_greed_index(limit=365):
    url = f"https://api.alternative.me/fng/?limit={limit}&format=json"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

        # Convert to DataFrame
    df = pd.DataFrame(data['data'])

        # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s').dt.date
    df['value'] = df['value'].astype(int)

    classification_map = {
            'Extreme Fear': 1,
            'Fear': 2,
            'Neutral': 3,
            'Greed': 4,
            'Extreme Greed': 5
        }

    df['numeric_sentiment'] = df['value_classification'].map(classification_map)

        # Sort by date (newest first)
    df = df.sort_values('timestamp', ascending=False)

    return df[['value_classification','numeric_sentiment', 'timestamp']]

def schema() -> list[dict]:
    """
    create the schema for the bq table
    """
    table_schema = [
        {'name': 'value_classification', 'type': 'string', 'description': 'the categorical classification of the index'},
        {'name': 'numeric_sentiment', 'type': 'INT64', 'description': 'the numerical sentiment of the index'},
        {'name': 'timestamp', 'type': 'DATE', 'description': 'the date of the index reading'}
    ]
    return table_schema

def run_etl(credentials,dataset:str,mode:str) -> int:

    if mode == 'prod':

        table = fetch_fear_greed_index()
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

        return 0

    else:
        print('test mode')
        table = fetch_fear_greed_index()

        # Create subdirectory if it doesn't exist
        subdir = local_folder / fetch_fear_greed_index.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')

        return 0