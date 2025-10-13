import logging
from google.oauth2 import service_account
from typing import Any
import time
import os

logging.basicConfig(
    filename='logfile.txt',  
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dataset = "signals."

from technical_indicators import ( 
    bitcoin_transactions_volume,
    bitcoin_closing_prices,
    btc_moving_averages,
    bitcoin_ema,
    macd,
    rsi,
    bollinger_bands,
    ethereum_closing_prices,
    tether_data,
    cmc_data,
    mvrv_score,
    others_dominance,
    total_three_divided_btc,
    fear_greed,
    sp_500
    )


def validate_job_result(job_name: str, result: Any) -> bool:
    """Validate that job returns expected data types and values."""

    # Check if result exists
    if result is None:
        logger.error(f"{job_name}: Job returned None")
        return False

    # Check if result is numeric (bytes processed should be int/float)
    if not isinstance(result, (int, float)):
        logger.error(f"{job_name}: Expected numeric result, got {type(result)}")
        return False

    # Check if result is non-negative (bytes processed can't be negative)
    if result < 0:
        logger.error(f"{job_name}: Expected non-negative result, got {result}")
        return False

    # Check if result is reasonable
    if result > 1e6: ### 1 megabyte
        logger.warning(f"{job_name}: Unusually large bytes processed: {result}")

    logger.debug(f"{job_name}: Validation passed - {result} bytes processed")
    return True

def is_running_locally():
    if os.getenv('COMPUTERNAME') is not None:
        return 'local'
    else:
        return 'prod'

def main() -> None:
    
    credentials = service_account.Credentials.from_service_account_file("connection-123-892e002c2def.json")

    mode = is_running_locally()

    jobs = [
        bitcoin_transactions_volume,
        bitcoin_closing_prices,
        btc_moving_averages,
        bitcoin_ema,
        macd,
        rsi,
        bollinger_bands,
        ethereum_closing_prices,
        tether_data,
        cmc_data,
        mvrv_score,
        others_dominance,
        total_three_divided_btc,
        fear_greed,
        sp_500
    ]

    total_bytes_processed = 0
    total_execution_time = 0

    for job in jobs:
        try:
            job_name = job.__name__.title()
            logger.info(f"Starting ETL job: {job_name}")

            start_time = time.time()

            bytes_processed = job.run_etl(credentials, dataset, mode)

            # Validate the result
            if validate_job_result(job_name, bytes_processed):
                total_bytes_processed += bytes_processed

                end_time = time.time()
                execution_time = end_time - start_time
                total_execution_time += execution_time

                logger.info(f"Completed {job_name} in {execution_time:.2f} seconds")

            else:
                logger.error(f"Failed validation for {job_name}")

        except ValueError as e:
            logger.warning(f"Invalid value: {e}")
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
        except Exception as e:
            logger.critical(f"Unexpected error: {e}", exc_info=True)

    logger.info(f"Total bytes processed across all jobs: {total_bytes_processed / 1024 / 1024:.2f} MB)")
    logger.info(f"Total execution time: {total_execution_time:.2f} seconds ({total_execution_time / 60:.2f} minutes)")

if __name__ == "__main__":
    main()

