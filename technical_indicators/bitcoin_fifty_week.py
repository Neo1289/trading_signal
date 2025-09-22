import yfinance as yf
import pandas as pd
import functools
from datetime import datetime
from typing import Any

def current_week_func()-> tuple[int,int]:
    now = datetime.now()
    year, week, _ = now.isocalendar()

    return year, week

@functools.lru_cache(maxsize=1)
def fetch_transactions(week_year:tuple[int,int]) -> pd.DataFrame:
    print(f"Fetching fresh data for week {week_year}")
    btc = yf.download('BTC-USD', period='5y',auto_adjust=False)
    weekly = btc['Close'].resample('W').last()
    sma50 = weekly.rolling(50).mean()
    df = pd.DataFrame(sma50)

    return df

if __name__ == '__main__':
    current_week = current_week_func()
    data = fetch_transactions(current_week)

    print("Cache info:", fetch_transactions.cache_info())

    # Second call (should use cache)
    data = fetch_transactions(current_week)
    print("Cache info:", fetch_transactions.cache_info())