{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH source_data AS (
    SELECT *
    FROM dbw_unity_learning.bronze_schema.bronze_match_results
)

SELECT
    date as match_date,
    match_type,        -- Added this column
    team_one,
    team_two,
    result,
    score_summary,
    
    -- Winning Team Logic (Handles "Won by" and "Winner:")
    CASE 
        WHEN result LIKE '%won by%' THEN split(result, ' won')[0]
        WHEN result LIKE '%Winner:%' THEN trim(split(result, 'Winner:')[1])
        ELSE 'Draw/No Result' 
    END as winning_team,

    -- Win Type Logic (Handles Runs, Wickets, and Abandoned)
    CASE
        WHEN result LIKE '%Runs%' THEN 'Batting First Win'
        WHEN result LIKE '%Wickets%' THEN 'Chasing Win'
        WHEN result LIKE '%Abandoned%' THEN 'Abandoned/Walkover'
        ELSE 'Other'
    END as win_type

FROM source_data
WHERE match_type = 'League'
