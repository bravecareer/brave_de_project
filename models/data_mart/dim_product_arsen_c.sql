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
      p.supplier_id, --might change in the future, but still is a dimension
      p.product_color,
      p.manufacturing_date,
      p.expiration_date,
      p.warranty_period,
      --p.quantity_in_stock,
      p.rating,
      --p.sales_volume,
      p.weight_grams,
      p.discount_percentage
   FROM {{ source('de_project', 'product_data') }} p
   WHERE p.product_id IS NOT NULL
)

SELECT * FROM product_data