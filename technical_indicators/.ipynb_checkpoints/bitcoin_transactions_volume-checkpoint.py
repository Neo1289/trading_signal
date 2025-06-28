SELECT 
  DATE(timestamp) as transaction_date,
  SUM(transaction_count) as total_transactions
FROM 
  `bigquery-public-data.crypto_bitcoin.blocks`
WHERE 
  timestamp_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 MONTH)
GROUP BY 
  transaction_date
ORDER BY 
  transaction_date DESC