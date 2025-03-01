-- Test: Check for missing products in both fact and dimension tables
-- This test identifies:
-- 1. Products in the fact table (fact_user_transactions_gs) that do not exist in the dimension table (dim_product_data_gs).
-- 2. Products in the dimension table (dim_product_data_gs) that have no transactions in the fact table (fact_user_transactions_gs).

-- Missing products in the product dimension
SELECT 
    f.product_id  -- Select product ID from the fact table
FROM {{ ref('fact_user_transactions_gs') }} f  -- Reference the fact table
LEFT JOIN {{ ref('dim_product_data_gs') }} p  -- Perform a LEFT JOIN with the product dimension table
    ON f.product_id = p.product_id  -- Match on product_id
WHERE p.product_id IS NULL  -- Return products in the fact table with no corresponding product in the dimension table

UNION ALL

-- Missing products in the fact table
SELECT 
    p.product_id  -- Select product ID from the product dimension table
FROM {{ ref('dim_product_data_gs') }} p  -- Reference the product dimension table
LEFT JOIN {{ ref('fact_user_transactions_gs') }} f  -- Perform a LEFT JOIN with the fact table
    ON p.product_id = f.product_id  -- Match on product_id
WHERE f.product_id IS NULL  -- Return products in the dimension table with no corresponding transaction in the fact table
