{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}

-- Get all product data without date filtering
WITH product_data AS (
   SELECT
       p.product_id,
       p.product_name,
       p.product_category,
       p.price,
       p.product_color,
       p.manufacturing_date,
       p.expiration_date,
       p.warranty_period,
       p.rating,
       p.weight_grams,
       p.discount_percentage
   FROM {{ ref('stg_product_data_tf') }} p
   {% if is_incremental() %}
   -- Only process new or changed products in incremental runs
   WHERE p.manufacturing_date >= CURRENT_DATE() - 5
   {% endif %}
)

SELECT * FROM product_data