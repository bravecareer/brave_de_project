-- Ensure all user_ids in the user journey staging model exist in the valid users source
SELECT
  u.user_id
FROM {{ ref('stg_user_journey_bl') }}
LEFT JOIN {{ source('de_project', 'user_data') }} u
ON {{ ref('stg_user_journey_bl') }}.user_id = u.user_id
WHERE u.user_id IS NULL