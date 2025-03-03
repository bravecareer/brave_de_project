/*
    Test Name: Valid User IDs Test
    Description: Validates that all user_ids in fact tables exist in the user dimension table
    Note: This test ignores 'UNKNOWN' user_ids as they are handled in the staging layer
*/

-- Test user_ids in user journey data that are related to campaigns
WITH user_journey_campaign_data AS (
    SELECT DISTINCT uj.user_id
    FROM {{ ref('stg_user_journey') }} uj
    WHERE uj.mkt_campaign IS NOT NULL
    AND uj.mkt_campaign != 'UNKNOWN'
    AND uj.user_id IS NOT NULL
    AND uj.user_id != 'UNKNOWN'
)

SELECT
  u.user_id as invalid_user_id,
  'user_journey_campaign_data' as source_table
FROM user_journey_campaign_data u
LEFT JOIN {{ source('de_project', 'user_data') }} ud
ON u.user_id = ud.user_id
WHERE ud.user_id IS NULL
