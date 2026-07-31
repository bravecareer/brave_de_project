-- Ensure all user_ids in the model exist in the valid_users source
SELECT 
    f.user_id
FROM {{ ref('fact_user_engagement_kl') }} f
LEFT JOIN {{ source('de_project', 'user_journey') }} uj 
    ON f.user_id = uj.user_id
WHERE f.user_id IS NOT NULL 
  AND uj.user_id IS NULL
