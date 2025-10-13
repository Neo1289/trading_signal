import yfinance as yf
import pandas as pd
import functools
from datetime import datetime
import pandas_gbq
from pathlib import Path
import os

current_dir = Path(__file__).parent
local_folder = current_dir / "testing_area"
local_folder_string = str(local_folder.resolve())


destination_table = "bitcoin_fifty_week"

def get_fifty_weeks() -> pd.DataFrame:

    btc = yf.download('BTC-USD', period='5y',auto_adjust=False)

    weekly = btc['Close'].resample('W').last()

    sma50 = weekly.rolling(window=50).mean()

    df = pd.DataFrame(sma50).reset_index()

    df.columns = ['date', 'sma50_close']

    return df

def schema() -> list[dict]:

    table_schema = [
        {'name': 'date', 'type': 'DATE','description': 'date for end of the week'},
        {'name': 'sma50_close', 'type': 'FLOAT64',
         'description': '50 week rolling simple moving average, the closing week is sunday for bitcoin'},
    ]
    return table_schema

def run_etl(credentials, dataset: str, mode: str) -> int:

    if mode == 'prod':

        table = get_fifty_weeks()
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
        table = get_fifty_weeks()
        # Create subdirectory if it doesn't exist
        subdir = local_folder / get_fifty_weeks.__name__
        subdir.mkdir(parents=True, exist_ok=True)

        csv_filename = os.path.join(str(subdir), "data.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return 0
