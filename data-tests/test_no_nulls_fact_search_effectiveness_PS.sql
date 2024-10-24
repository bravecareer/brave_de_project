-- Test for no NULL values in critical columns for fact_search_effectiveness_PS
SELECT
  user_id,
  search_event_id,
  product_id
FROM {{ ref('fact_search_effectiveness_PS') }}
WHERE user_id IS NULL
  OR search_event_id IS NULL
  OR product_id IS NULL
