{{ config(
   materialized='view'
) }}

WITH product_performance AS (
   SELECT
       f.product_id,
       p.rating,
       p.product_name,
       COUNT(f.product_id) AS total_transactions -- Counting transactions per product
   FROM {{ ref('fact_user_transactions_gs') }} f
   LEFT JOIN {{ ref('dim_product_data_gs') }} p 
     ON f.product_id = p.product_id
   GROUP BY f.product_id, p.product_name,p.rating
)

SELECT * FROM product_performance
