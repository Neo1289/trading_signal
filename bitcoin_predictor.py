import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, classification_report
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class BitcoinPredictor:
    def __init__(self, credentials_path: str):
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(credentials=self.credentials, project="connection-123")
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.scaler = StandardScaler()

    def fetch_all_indicators(self) -> pd.DataFrame:
        """Fetch and combine all technical indicators from BigQuery"""

        # Main query joining all indicator tables
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
                ema.ema_50,
                ema.ema_200,
                rsi.rsi_14,
                macd.macd_line,
                macd.signal_line,
                macd.histogram,
                bb.middle_band,
                bb.upper_band,
                bb.lower_band,
                bb.bb_width,
                bb.percent_b
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

        query_job = self.client.query(query)
        df = query_job.result().to_dataframe()

        logger.info(f"Fetched {len(df)} rows of combined indicator data")
        return df

    def create_features(self, df: pd.DataFrame, include_target: bool = True) -> pd.DataFrame:
        """Create additional features and optionally the target variable.
        When include_target=False, do not drop the last 3 days so that prediction uses the latest date.
        """

        # Sort by date to ensure proper order
        df = df.sort_values('date_').reset_index(drop=True)

        # Price momentum features
        df['price_change_1d'] = df['price'].pct_change(1)
        df['price_change_3d'] = df['price'].pct_change(3)
        df['price_change_7d'] = df['price'].pct_change(7)

        # Volume features
        df['volume_ma_7'] = df['total_volume'].rolling(7).mean()
        df['volume_ratio'] = df['total_volume'] / df['volume_ma_7']

        # Moving average ratios
        df['price_sma10_ratio'] = df['price'] / df['sma_10']
        df['price_sma20_ratio'] = df['price'] / df['sma_20']
        df['price_sma50_ratio'] = df['price'] / df['sma_50']
        df['sma10_sma20_ratio'] = df['sma_10'] / df['sma_20']

        # EMA features
        df['ema12_ema26_diff'] = df['ema_12'] - df['ema_26']
        df['price_ema20_ratio'] = df['price'] / df['ema_20']

        # Bollinger Bands position
        df['bb_position'] = (df['price'] - df['lower_band']) / (df['upper_band'] - df['lower_band'])

        # MACD momentum
        df['macd_momentum'] = df['macd_line'] - df['signal_line']

        if include_target:
            # Target: 1 if price goes up in next 3 days, 0 if down
            df['future_price'] = df['price'].shift(-3)
            df['target'] = (df['future_price'] > df['price']).astype(int)
            # Remove rows with NaN values (includes last 3 rows without future_price)
            df = df.dropna().reset_index(drop=True)
        else:
            # For inference, keep the latest rows; drop only rows that are unusable for features
            feature_columns = [
                'price_change_1d', 'price_change_3d', 'price_change_7d',
                'volume_ratio', 'price_sma10_ratio', 'price_sma20_ratio',
                'price_sma50_ratio', 'sma10_sma20_ratio', 'ema12_ema26_diff',
                'price_ema20_ratio', 'bb_position', 'bb_width', 'percent_b',
                'rsi_14', 'macd_momentum', 'histogram'
            ]
            df = df.dropna(subset=feature_columns).reset_index(drop=True)

        return df

    def prepare_features(self, df: pd.DataFrame) -> tuple:
        """Select and prepare features for training or inference. Returns (X, y, feature_columns); y is None if not present."""

        feature_columns = [
            'price_change_1d', 'price_change_3d', 'price_change_7d',
            'volume_ratio', 'price_sma10_ratio', 'price_sma20_ratio',
            'price_sma50_ratio', 'sma10_sma20_ratio', 'ema12_ema26_diff',
            'price_ema20_ratio', 'bb_position', 'bb_width', 'percent_b',
            'rsi_14', 'macd_momentum', 'histogram'
        ]

        X = df[feature_columns].copy()
        y = df['target'].copy() if 'target' in df.columns else None

        # Handle any remaining NaN values
        X = X.fillna(X.mean())

        return X, y, feature_columns

    def train_model(self) -> dict:
        """Train the prediction model"""

        # Fetch and prepare data
        df = self.fetch_all_indicators()
        df = self.create_features(df, include_target=True)
        X, y, feature_columns = self.prepare_features(df)

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Train model
        self.model.fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = self.model.predict(X_test_scaled)
        accuracy = accuracy_score(y_test, y_pred)

        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_columns,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)

        results = {
            'accuracy': accuracy,
            'feature_importance': feature_importance,
            'classification_report': classification_report(y_test, y_pred)
        }

        logger.info(f"Model trained with accuracy: {accuracy:.4f}")
        return results

    def predict_direction(self) -> dict:
        """Predict Bitcoin direction for next 3 days using the latest available date."""

        # Get latest engineered features without target to avoid dropping the last 3 days
        df_raw = self.fetch_all_indicators()
        df_feat = self.create_features(df_raw, include_target=False)
        X_all, _, feature_columns = self.prepare_features(df_feat)

        # Scale and predict for all rows, then take the latest
        X_all_scaled = self.scaler.transform(X_all)
        preds = self.model.predict(X_all_scaled)
        probs = self.model.predict_proba(X_all_scaled)

        latest_idx = X_all.index[-1]
        prediction = preds[-1]
        probability = probs[-1]

        direction = "UP" if prediction == 1 else "DOWN"
        confidence = float(max(probability))

        latest_row = df_feat.loc[latest_idx]
        return {
            'direction': direction,
            'confidence': confidence,
            'probability_up': float(probability[1]),
            'probability_down': float(probability[0]),
            'current_price': float(latest_row['price']),
            'date': latest_row['date_']
        }

def main():
    """Main function to run Bitcoin prediction"""

    # Initialize predictor
    credentials_path = "connection-123-892e002c2def.json"
    predictor = BitcoinPredictor(credentials_path)

    # Train model
    print("Training Bitcoin direction prediction model...")
    results = predictor.train_model()

    print(f"\nModel Performance:")
    print(f"Accuracy: {results['accuracy']:.4f}")
    print(f"\nTop 5 Important Features:")
    print(results['feature_importance'].head())

    # Make prediction
    print("\nPredicting Bitcoin direction for next 3 days...")
    prediction = predictor.predict_direction()

    print(f"\nPrediction Results:")
    print(f"Current Price: ${prediction['current_price']:,.2f}")
    print(f"Prediction Date: {prediction['date']}")
    print(f"Direction (3 days): {prediction['direction']}")
    print(f"Confidence: {prediction['confidence']:.4f}")
    print(f"Probability UP: {prediction['probability_up']:.4f}")
    print(f"Probability DOWN: {prediction['probability_down']:.4f}")

if __name__ == "__main__":
    main()
