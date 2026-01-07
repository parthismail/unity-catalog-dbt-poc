{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH team_participation AS (
    -- Get every time a team played as "Team One"
    SELECT 
        team_one as team_name,
        winning_team,
        win_type,
        match_type
    FROM {{ ref('silver_match_analysis') }}
    
    UNION ALL
    
    -- Get every time a team played as "Team Two"
    SELECT 
        team_two as team_name,
        winning_team,
        win_type,
        match_type
    FROM {{ ref('silver_match_analysis') }}
),

calc_stats AS (
    SELECT
        team_name,
        match_type,
        count(*) as total_matches,
        
        -- Overall Stats
        sum(CASE WHEN team_name = winning_team THEN 1 ELSE 0 END) as total_wins,
        
        -- Batting First Analysis
        -- If I won and the win type was 'Batting First', then I defended successfully
        sum(CASE WHEN team_name = winning_team AND win_type = 'Batting First Win' THEN 1 ELSE 0 END) as wins_batting_first,
        
        -- Chasing Analysis
        -- If I won and the win type was 'Chasing', then I chased successfully
        sum(CASE WHEN team_name = winning_team AND win_type = 'Chasing Win' THEN 1 ELSE 0 END) as wins_chasing

    FROM team_participation
    GROUP BY team_name, match_type
)

SELECT
    team_name,
    match_type,
    total_matches,
    total_wins,
    (total_matches - total_wins) as total_losses,
    
    -- Calculate Percentages (Cast to double to handle decimals)
    round((total_wins / total_matches) * 100, 2) as win_percentage,
    
    -- Detailed Metrics
    wins_batting_first,
    wins_chasing

FROM calc_stats
ORDER BY win_percentage DESC
