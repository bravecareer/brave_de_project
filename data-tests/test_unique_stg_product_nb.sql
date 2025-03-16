-- Test for unique product_id
SELECT product_id, COUNT(*) 
FROM {{ ref('stg_product') }}  
GROUP BY product_id 
HAVING COUNT(*) > 1;


