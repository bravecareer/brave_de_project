/*
    Test Name: Valid User IDs Test
    Description: Validates that all user_ids in stg_user_journey_tf exist in stg_user_data_tf
    Note: This test ignores 'UNKNOWN' user_ids
*/

-- Find user_ids in stg_user_journey_tf that don't exist in stg_user_data_tf
WITH missing_user_ids AS (
    SELECT DISTINCT
      uj.user_id as invalid_user_id,
      'stg_user_journey_tf' as source_table
    FROM {{ ref('stg_user_journey_tf') }} uj
    LEFT JOIN {{ ref('stg_user_data_tf') }} ud
    ON uj.user_id = ud.user_id
    WHERE ud.user_id IS NULL
      AND uj.user_id IS NOT NULL
      AND uj.user_id != 'UNKNOWN'
)

-- Return results
SELECT * FROM missing_user_ids
