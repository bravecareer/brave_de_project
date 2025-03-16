-- Test to ensure price and weight are non-negative
SELECT * FROM {{ ref('stg_product_nb') }}  
WHERE price < 0 OR weight_grams < 0;