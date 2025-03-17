-- Ensure all product_ids in the user journey staging model exist in the valid products source
SELECT
  p.product_id
FROM {{ ref('stg_user_journey_bl') }}
LEFT JOIN {{ source('de_project', 'product_data') }} p
ON {{ ref('stg_user_journey_bl') }}.product_id = p.product_id
WHERE p.product_id IS NULL