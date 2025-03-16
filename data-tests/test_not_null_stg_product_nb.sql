-- Test for NOT NULL constraints
SELECT * FROM {{ ref('stg_product') }}  
WHERE product_id IS NULL OR price IS NULL;
