-- Ensure no NULL values in critical columns of user journey staging
SELECT
  user_id,
  product_id,
  search_event_id,
  timestamp
FROM {{ ref('stg_user_journey_bl') }}
WHERE user_id IS NULL
   OR product_id IS NULL
   OR search_event_id IS NULL
   OR timestamp IS NULL