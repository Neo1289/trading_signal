import yfinance as yf
import pandas as pd

# Download S&P 500 data
ticker = yf.Ticker("^GSPC")  # ^GSPC is the S&P 500 index symbol
df = ticker.history(period="20y")

# Get just the close prices
close_prices = df['Close']
df = pd.DataFrame(close_prices)
df = df.reset_index().rename(columns={'Date': 'date'})


print(df.columns)
