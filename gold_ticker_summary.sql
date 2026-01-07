{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_unified_trades') }}
)

SELECT
    ticker,
    source_broker,
    account_name,
    
    -- Trade Stats
    count(*) as total_trades,
    min(activity_date) as first_trade_date,
    max(activity_date) as last_trade_date,
    
    -- Cash Flow Analysis
    -- Logic: 'Amount' is typically negative for Buys (Cash Out) and positive for Sells (Cash In).
    -- Summing them gives "Net Cash Flow".
    round(sum(amount), 2) as net_cash_flow,
    
    -- Breakdown
    round(sum(CASE WHEN amount < 0 THEN amount ELSE 0 END), 2) as total_cash_invested,
    round(sum(CASE WHEN amount > 0 THEN amount ELSE 0 END), 2) as total_cash_returned

FROM silver_data
GROUP BY ticker, source_broker, account_name
ORDER BY net_cash_flow DESC