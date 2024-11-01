{{ config(
   materialized='table',
   unique_key='product_id'
) }}

SELECT 
   product_id,
   product_name,
   product_category,
   price,
   rating
FROM {{ ref('stg_product_data') }}
WHERE product_id IS NOT NULL
