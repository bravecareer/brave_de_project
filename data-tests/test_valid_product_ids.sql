-- Ensure all product_ids in the fact table exist in the products source
-- Business Value: Maintain referential integrity

SELECT 
    f.product_id
FROM {{ ref('fact_user_transaction_kl') }} f
LEFT JOIN {{ source('de_project', 'product_data') }} p
    ON f.product_id = p.product_id
WHERE f.product_id IS NOT NULL  -- Exclude actual nulls
  AND p.product_id IS NULL      -- Find missing IDs
