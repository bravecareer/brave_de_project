-- Ensure no NULL values in critical columns
SELECT
  user_id,
  product_id,
  search_event_id,
  timestamp
FROM {{ ref('fact_user_engagement_arsen_c') }}
WHERE user_id IS NULL
   OR product_id IS NULL
   OR search_event_id IS NULL
   OR timestamp IS NULL
