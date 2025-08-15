import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import warnings
warnings.filterwarnings('ignore')

# Configure logging to write to logfile.txt
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logfile.txt'),
        logging.StreamHandler()  # Also keep console output
    ]
)
logger = logging.getLogger(__name__)

class BitcoinPredictor:
    def __init__(self, credentials_path: str):
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(credentials=self.credentials, project="connection-123")

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

    def analyze_individual_indicators(self) -> dict:
        """Analyze each technical indicator individually and provide interpretations"""

        # Get the latest data
        df = self.fetch_all_indicators()
        if df.empty:
            return {"error": "No data available for analysis"}

        # Get the most recent row
        latest = df.iloc[-1]
        current_price = latest['price']

        analysis = {
            'date': latest['date_'],
            'current_price': current_price,
            'indicators': {},
            'summary': ''
        }

        # 1. Moving Averages Analysis
        sma_signals = []
        if current_price > latest['sma_10']:
            sma_signals.append("BULLISH (above SMA-10)")
        else:
            sma_signals.append("BEARISH (below SMA-10)")

        if current_price > latest['sma_20']:
            sma_signals.append("BULLISH (above SMA-20)")
        else:
            sma_signals.append("BEARISH (below SMA-20)")

        if current_price > latest['sma_50']:
            sma_signals.append("BULLISH (above SMA-50)")
        else:
            sma_signals.append("BEARISH (below SMA-50)")

        analysis['indicators']['Moving Averages'] = {
            'interpretation': f"SMA-10: ${latest['sma_10']:,.2f}, SMA-20: ${latest['sma_20']:,.2f}, SMA-50: ${latest['sma_50']:,.2f}",
            'signal': "; ".join(sma_signals)
        }

        # 2. EMA Analysis
        ema_signals = []
        if current_price > latest['ema_20']:
            ema_signals.append("BULLISH (above EMA-20)")
        else:
            ema_signals.append("BEARISH (below EMA-20)")

        if latest['ema_12'] > latest['ema_26']:
            ema_signals.append("BULLISH (EMA-12 > EMA-26)")
        else:
            ema_signals.append("BEARISH (EMA-12 < EMA-26)")

        analysis['indicators']['Exponential Moving Averages'] = {
            'interpretation': f"EMA-12: ${latest['ema_12']:,.2f}, EMA-26: ${latest['ema_26']:,.2f}, EMA-20: ${latest['ema_20']:,.2f}",
            'signal': "; ".join(ema_signals)
        }

        # 3. RSI Analysis
        rsi = latest['rsi_14']
        if rsi > 70:
            rsi_signal = "BEARISH (Overbought)"
        elif rsi < 30:
            rsi_signal = "BULLISH (Oversold)"
        elif rsi > 50:
            rsi_signal = "BULLISH (Above midline)"
        else:
            rsi_signal = "BEARISH (Below midline)"

        analysis['indicators']['RSI'] = {
            'interpretation': f"RSI-14: {rsi:.1f}",
            'signal': rsi_signal
        }

        # 4. MACD Analysis
        macd_line = latest['macd_line']
        signal_line = latest['signal_line']
        histogram = latest['histogram']

        macd_signals = []
        if macd_line > signal_line:
            macd_signals.append("BULLISH (MACD above signal)")
        else:
            macd_signals.append("BEARISH (MACD below signal)")

        if histogram > 0:
            macd_signals.append("BULLISH (Positive histogram)")
        else:
            macd_signals.append("BEARISH (Negative histogram)")

        analysis['indicators']['MACD'] = {
            'interpretation': f"MACD: {macd_line:.2f}, Signal: {signal_line:.2f}, Histogram: {histogram:.2f}",
            'signal': "; ".join(macd_signals)
        }

        # 5. Bollinger Bands Analysis
        bb_position = latest['percent_b']
        upper_band = latest['upper_band']
        lower_band = latest['lower_band']

        if bb_position > 0.8:
            bb_signal = "BEARISH (Near upper band - overbought)"
        elif bb_position < 0.2:
            bb_signal = "BULLISH (Near lower band - oversold)"
        elif bb_position > 0.5:
            bb_signal = "BULLISH (Above middle band)"
        else:
            bb_signal = "BEARISH (Below middle band)"

        analysis['indicators']['Bollinger Bands'] = {
            'interpretation': f"Upper: ${upper_band:,.2f}, Lower: ${lower_band:,.2f}, Position: {bb_position:.2f}",
            'signal': bb_signal
        }

        # 6. Volume Analysis
        volume_ma_7 = df['total_volume'].tail(7).mean()
        if latest['total_volume'] > volume_ma_7:
            volume_signal = "BULLISH (Above average volume)"
        else:
            volume_signal = "BEARISH (Below average volume)"

        analysis['indicators']['Volume'] = {
            'interpretation': f"Current: {latest['total_volume']:,.0f}, 7-day avg: {volume_ma_7:,.0f}",
            'signal': volume_signal
        }

        # Generate Summary
        bullish_count = sum(1 for indicator in analysis['indicators'].values()
                           if 'BULLISH' in indicator['signal'])
        bearish_count = sum(1 for indicator in analysis['indicators'].values()
                           if 'BEARISH' in indicator['signal'])

        total_signals = bullish_count + bearish_count
        bullish_percentage = (bullish_count / total_signals) * 100 if total_signals > 0 else 0

        if bullish_percentage >= 70:
            overall_sentiment = "STRONGLY BULLISH"
        elif bullish_percentage >= 60:
            overall_sentiment = "BULLISH"
        elif bullish_percentage >= 40:
            overall_sentiment = "NEUTRAL"
        elif bullish_percentage >= 30:
            overall_sentiment = "BEARISH"
        else:
            overall_sentiment = "STRONGLY BEARISH"

        analysis['summary'] = (f"Technical analysis shows {bullish_count} bullish and {bearish_count} bearish signals "
                              f"({bullish_percentage:.0f}% bullish). Overall sentiment: {overall_sentiment}")

        # Log the analysis
        logger.info(f"Technical Analysis - {analysis['summary']}")

        return analysis

    def generate_indicator_report(self) -> None:
        """Generate and display a comprehensive technical indicator report"""

        print("Analyzing Bitcoin Technical Indicators...")
        print("=" * 80)

        analysis = self.analyze_individual_indicators()

        if 'error' in analysis:
            print(f"Error: {analysis['error']}")
            return

        print(f"Bitcoin Technical Analysis Report")
        print(f"Date: {analysis['date']}")
        print(f"Current Price: ${analysis['current_price']:,.2f}")
        print("=" * 80)

        for indicator_name, indicator_data in analysis['indicators'].items():
            print(f"\n{indicator_name.upper()}:")
            print(f"  Values: {indicator_data['interpretation']}")
            print(f"  Signal: {indicator_data['signal']}")

        print("\n" + "=" * 80)
        print(f"SUMMARY: {analysis['summary']}")
        print("=" * 80)

def main():
    """Main function to run Bitcoin technical indicator analysis"""

    # Initialize predictor
    credentials_path = "connection-123-892e002c2def.json"
    predictor = BitcoinPredictor(credentials_path)

    # Generate comprehensive technical indicator report
    predictor.generate_indicator_report()

if __name__ == "__main__":
    main()
