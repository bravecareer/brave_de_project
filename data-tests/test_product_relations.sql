-- Test: Check for NULL values in critical columns and missing products in the fact table
-- This test identifies:
-- 1. Critical columns (user_id, product_id, search_event_id, timestamp) with NULL values in the fact table (fact_user_transactions_gs).
-- 2. Products in the fact table (fact_user_transactions_gs) that do not exist in the product dimension table (dim_product_data_gs).

-- Check for NULL values in critical columns
SELECT
  user_id,
  product_id,
  search_event_id,
  timestamp
FROM {{ ref('fact_user_transactions_gs') }}
WHERE user_id IS NULL
   OR product_id IS NULL
   OR search_event_id IS NULL
   OR timestamp IS NULL

UNION ALL

-- Missing products in the fact table
SELECT 
    NULL AS user_id,       -- Placeholder for user_id
    f.product_id,          -- Select product_id from the fact table
    NULL AS search_event_id, -- Placeholder for search_event_id
    NULL AS timestamp      -- Placeholder for timestamp
FROM {{ ref('fact_user_transactions_gs') }} f  -- Reference the fact table
LEFT JOIN {{ ref('dim_product_data_gs') }} p  -- Perform a LEFT JOIN with the product dimension table
    ON f.product_id = p.product_id  -- Match on product_id
WHERE p.product_id IS NULL  -- Return products in the fact table with no corresponding product in the dimension table
