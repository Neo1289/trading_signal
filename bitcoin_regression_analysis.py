import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from typing import Tuple, Dict, List



def fetch_all_indicators(credentials_path="connection-123-892e002c2def.json") -> pd.DataFrame:

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project="connection-123")

    query = """
    WITH base_data AS (
        SELECT
            p.timestamp as date_,
            p.price,
            p.market_cap,
            p.total_volume,
            ma.sma_10,
            ma.sma_20,
            ma.sma_50,
            ema.ema_9,
            ema.ema_12,
            ema.ema_26,
            ema.ema_20,
            ema.ema_50
        FROM `connection-123.signals.bitcoin_price` p
        LEFT JOIN `connection-123.signals.btc_moving_averages` ma ON p.timestamp = ma.date_
        LEFT JOIN `connection-123.signals.btc_ema` ema ON p.timestamp = ema.timestamp
        LEFT JOIN `connection-123.signals.btc_rsi` rsi ON p.timestamp = rsi.timestamp
        LEFT JOIN `connection-123.signals.btc_macd` macd ON p.timestamp = macd.timestamp
        LEFT JOIN `connection-123.signals.btc_bollinger_bands` bb ON p.timestamp = bb.timestamp
        WHERE p.timestamp BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        ORDER BY p.timestamp
    )
    SELECT *
    FROM base_data
    WHERE sma_10 IS NOT NULL  -- Ensure we have indicator data
    """

    query_job = client.query(query)
    df = query_job.result().to_dataframe()

    return df.dropna()

def btc_regression_model() -> Tuple[pd.DataFrame, sm.regression.linear_model.RegressionResultsWrapper, StandardScaler]:
    data = fetch_all_indicators()

    feature_cols = ['total_volume', 'sma_10', 'sma_20', 'sma_50',
                    'ema_9', 'ema_12', 'ema_26', 'ema_20', 'ema_50']

    feature_cols = [col for col in feature_cols if col in data.columns]

    X = data[feature_cols]
    y = data['price']

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_train = sm.add_constant(X_scaled)

    model = sm.OLS(y, X_train)
    results = model.fit()

    return data, results, scaler

def predict_next_days(results: sm.regression.linear_model.RegressionResultsWrapper,
                      data: pd.DataFrame,
                      scaler: StandardScaler,
                      days: int = 1
                      ) -> pd.DataFrame:

    feature_cols = ['total_volume', 'sma_10', 'sma_20', 'sma_50',
                    'ema_9', 'ema_12', 'ema_26', 'ema_20', 'ema_50']
    feature_cols = [col for col in feature_cols if col in data.columns]

    last_row = data[feature_cols].iloc[-1].values

    # Use the same scaler from training
    last_row_scaled = scaler.transform([last_row])
    X_pred = sm.add_constant(last_row_scaled)

    predicted_price = results.predict(X_pred)[0]

    last_date = pd.to_datetime(data['date_'].iloc[-1])
    next_date = last_date + pd.Timedelta(days=days)

    return pd.DataFrame({
        'date': [next_date],
        'predicted_price': [predicted_price]
    })

if __name__ == "__main__":
    data, results, scaler = btc_regression_model()
    future_predictions = predict_next_days(results, data, scaler)
    print("Next Day Bitcoin Price Predictions:")
    print(future_predictions[['date', 'predicted_price']].round(2))