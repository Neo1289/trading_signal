import yfinance as yf
import pandas as pd
import functools
from datetime import datetime
import pandas_gbq
from pathlib import Path
import os

current_dir = Path(__file__).parent
local_folder = current_dir.parent / "testing_area"
local_folder_string = str(local_folder.resolve())


destination_table = "bitcoin_fifty_week"

def current_week_func()-> tuple[int,int]:
    now = datetime.now()
    year, week, _ = now.isocalendar()

    return year, week

@functools.lru_cache(maxsize=1)
def fetch_transactions(week_year:tuple[int,int]) -> pd.DataFrame:
    print(f"Fetching fresh data for week {week_year}")

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

def run_etl(credentials, dataset: str, mode: str) -> None:

    if mode == 'prod':

        current_week = current_week_func()
        table = fetch_transactions(current_week)
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
        current_week = current_week_func()
        table = fetch_transactions(current_week)
        csv_filename = os.path.join(local_folder,"bitcoin_closing_prices_local.csv")
        table.to_csv(csv_filename, index=False)
        print(f'Data saved to {csv_filename}')
        return 0
