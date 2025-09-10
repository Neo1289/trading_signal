import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import statsmodels.api as sm
from sklearn.model_selection import train_test_split


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

    return df


def btc_regression_model() -> tuple:
    data = fetch_all_indicators()
    X = data.drop(['price', 'date_'], axis=1)
    y = data['price']

    X_train = sm.add_constant(X)
    model = sm.OLS(y, X_train)
    results = model.fit()

    return data, results

def predict_next_days(results, days=5):
    data = fetch_all_indicators()

    latest_indicators = data.iloc[-1][['market_cap', 'total_volume', 'sma_10', 'sma_20', 'sma_50',
                                      'ema_9', 'ema_12', 'ema_26', 'ema_20', 'ema_50']].values

    predictions = []
    current_indicators = latest_indicators.copy()

    for day in range(1, days + 1):
        X_pred = np.insert(current_indicators, 0, 1)
        predicted_price = results.predict(X_pred)[0]
        predictions.append({
            'day': day,
            'predicted_price': predicted_price,
            'date': pd.Timestamp.now() + pd.Timedelta(days=day)
        })

        current_indicators[2:5] = current_indicators[2:5] * 0.95 + predicted_price * 0.05
        current_indicators[5:] = current_indicators[5:] * 0.9 + predicted_price * 0.1

    return pd.DataFrame(predictions)

if __name__ == "__main__":
    data, results = btc_regression_model()
    future_predictions = predict_next_days(results, days=5)
    print("Next 5 Days Bitcoin Price Predictions:")
    print(future_predictions[['date', 'predicted_price']].round(2))