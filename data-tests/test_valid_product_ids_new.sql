/*
    Test Name: Valid Product IDs Test
    Description: Validates that all product_ids in fact tables exist in the product dimension table
    Note: This test ignores 'UNKNOWN' product_ids as they are handled in the staging layer
*/

-- Ensure all product_ids in the model exist in the valid_products source
SELECT
  f.product_id as invalid_product_id
FROM {{ ref('fact_user_behavior_new') }} f
LEFT JOIN {{ source('de_project', 'product_data') }} p
ON f.product_id = p.product_id
WHERE p.product_id IS NULL
  AND f.product_id IS NOT NULL
  AND f.product_id != 'UNKNOWN'
