-- Ensure all product_ids in the model exist in the valid_products source
SELECT
  p.product_id
FROM {{ ref('fact_user_behavior_new') }} f
LEFT JOIN {{ source('de_project', 'product_data') }} p
ON f.product_id = p.product_id
WHERE p.product_id IS NULL
