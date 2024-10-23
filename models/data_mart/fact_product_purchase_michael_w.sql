{{ config(
   materialized='incremental',
   unique_key=['product_id']
) }}

WITH user_journey AS (
   SELECT
       uj.product_id
   FROM brave_database.de_project.user_journey uj
   WHERE uj.has_purchase = TRUE
),

product_purchase_data AS (
   SELECT
       uj.product_id,

       p.product_category,
       p.price,
       p.product_color,
       CAST(SUBSTR(p.warranty_period, 1, 2) AS INTEGER) AS warranty_period_in_month,
       p.rating,
       p.weight_grams,
       p.discount_percentage,
       count(*) as quantity_sold,
       sum(p.price) as total_sales_amount
   FROM user_journey uj
   INNER JOIN brave_database.de_project.product_data p
     ON uj.product_id = p.product_id
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT * FROM product_purchase_data