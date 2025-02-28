/*
    Test Name: Valid User IDs Test
    Description: Validates that all user_ids in fact tables exist in the user dimension table
*/

-- Ensure all user_ids in the model exist in the valid_users source
SELECT
  f.user_id as invalid_user_id
FROM {{ ref('fact_user_behavior_new') }} f
LEFT JOIN {{ source('de_project', 'user_data') }} u
ON f.user_id = u.user_id
WHERE u.user_id IS NULL
  AND f.user_id IS NOT NULL
