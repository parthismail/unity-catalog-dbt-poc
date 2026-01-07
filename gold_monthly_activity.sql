{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_unified_trades') }}
)

SELECT
    -- Truncate date to the first of the month (e.g., 2025-12-01)
    date_trunc('month', activity_date) as trade_month,
    source_broker,
    
    -- Monthly Stats
    count(DISTINCT ticker) as unique_tickers_traded,
    count(*) as total_transactions,
    round(sum(amount), 2) as monthly_net_cash_flow

FROM silver_data
GROUP BY 1, 2
ORDER BY 1 DESC, 2