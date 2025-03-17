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
       p.warranty_period,
       p.rating,
       p.weight_grams,
       p.discount_percentage
   FROM {{ source('de_project', 'product_data') }} p
)

SELECT * FROM product_data
{% if is_incremental() %}
-- On incremental runs, only process new products allowing a few days for late-arriving facts
WHERE manufacturing_date >= (SELECT DATEADD(day, -3, max(manufacturing_date)) from {{ this }})
{% endif %}