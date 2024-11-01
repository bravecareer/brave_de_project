{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}

WITH product_cleaned AS (
   SELECT
       product_id,
       product_name,
       product_category,
       price,
       supplier_id,
       product_color,
       manufacturing_date,
       expiration_date,
       warranty_period,
       quantity_in_stock,
       rating,
       sales_volume,
       weight_grams,
       discount_percentage
   FROM {{ source('de_project', 'product_data') }}
   WHERE product_id IS NOT NULL
     AND quantity_in_stock IS NOT NULL
)

SELECT * FROM product_cleaned
{% if is_incremental() %}
    WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}
