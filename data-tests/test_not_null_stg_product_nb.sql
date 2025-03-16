-- Test for NOT NULL constraints
SELECT * FROM {{ ref('stg_product_nb') }}  
WHERE product_id IS NULL OR price IS NULL;
