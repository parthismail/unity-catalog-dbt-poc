{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH raw_bronze AS (
    SELECT * FROM dbw_unity_learning.bronze_schema.bronze_stock_transactions
),

metadata_parsing AS (
    SELECT *,
        split_part(file_path, '/', -1) as filename,
        
        -- PRE-CALCULATE: Split the date string into an array safely
        -- This step effectively standardizes both columns into one list of parts
        split(trim(COALESCE(`Activity Date`, Date)), '/') as date_parts,
        
        CASE 
            WHEN lower(split_part(file_path, '/', -1)) LIKE 'sch_%' THEN 'Schwab'
            WHEN lower(split_part(file_path, '/', -1)) LIKE 'r_%' THEN 'Robinhood'
            ELSE 'Unknown'
        END as source_broker,

        regexp_extract(split_part(file_path, '/', -1), '_([^_.]+)\\.csv', 1) as account_tag
    FROM raw_bronze
)

SELECT
    -- 1. UNIFIED DATE (TEXT RECONSTRUCTION STRATEGY)
    -- This logic CANNOT crash because it uses only string functions.
    try_cast(
        CASE 
            -- Case A: It's a Slash Date (M/D/Y) -> We found 3 parts
            WHEN size(date_parts) = 3 THEN 
                concat_ws('-', 
                    date_parts[2],               -- Year
                    lpad(date_parts[0], 2, '0'), -- Month (forces '4' to '04')
                    lpad(date_parts[1], 2, '0')  -- Day (forces '2' to '02')
                )
            
            -- Case B: Fallback (Standard YYYY-MM-DD or garbage)
            ELSE COALESCE(`Activity Date`, Date)
        END 
    as date) as activity_date,

    -- 2. UNIFIED TICKER
    split_part(Symbol, ' ', 1) as ticker,

    -- 3. UNIFIED TRANS CODE
    Action as trans_code,

    -- 4. CLEAN NUMBERS
    try_cast(regexp_replace(cast(Quantity as string), '[$,]', '') as double) as quantity,
    try_cast(regexp_replace(cast(Price as string), '[$,]', '') as double) as price,
    try_cast(regexp_replace(cast(Amount as string), '[$,()]', '') as double) as amount,

    -- 5. METADATA
    Description as description,
    source_broker,
    COALESCE(account_tag, 'Primary') as account_name

FROM metadata_parsing
-- Filter: Only keep rows where the manual reconstruction worked
WHERE try_cast(
    CASE 
        WHEN size(date_parts) = 3 THEN 
            concat_ws('-', date_parts[2], lpad(date_parts[0], 2, '0'), lpad(date_parts[1], 2, '0'))
        ELSE COALESCE(`Activity Date`, Date)
    END 
as date) IS NOT NULL