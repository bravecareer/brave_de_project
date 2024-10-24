-- Ensure all user_ids in the model exist in the valid_users source
SELECT
  fc.user_id
FROM {{ ref('fact_campaign_effectiveness_PS') }} fc
LEFT JOIN {{ source('de_project', 'user_data') }} u
ON fc.user_id = u.user_id
WHERE u.user_id IS NULL
