-- Ensure all user_ids in the model exist in the valid_users source
SELECT
  u.user_id
FROM {{ ref('fact_user_engagement_sp') }}
LEFT JOIN {{ source('de_project', 'user_data') }} u
ON {{ ref('fact_user_engagement_sp') }}.user_id = u.user_id
WHERE u.user_id IS NULL
