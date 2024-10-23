{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}


WITH product_data AS (
   SELECT
       p.product_id,
       p.product_name,
       p.product_category,
       p.price,
       p.product_color,
       p.manufacturing_date,
       p.expiration_date,
       CAST(SUBSTR(p.warranty_period, 1, 2) AS INTEGER) AS warranty_period_in_month,
       p.rating,
       p.weight_grams,
       p.discount_percentage
   FROM {{ source('de_project', 'product_data') }} p
   WHERE p.product_id IS NOT NULL
)


SELECT * FROM product_data