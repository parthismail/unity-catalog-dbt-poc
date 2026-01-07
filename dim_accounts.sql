{{ config(
    materialized='table',
    file_format='delta'
) }}

SELECT DISTINCT
    -- Generates a unique ID based on the name (e.g., 'Dad' -> 'a3f9...')
    md5(account_name) as account_id,
    account_name,
    source_broker,
    current_timestamp() as first_seen_at

FROM {{ ref('silver_unified_trades') }}
WHERE account_name IS NOT NULL