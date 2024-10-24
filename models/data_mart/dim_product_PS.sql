{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}

WITH product_enriched AS (
   SELECT
       p.product_id,
       p.product_name,
       p.product_category,
       p.price,
       p.supplier_id,
       p.product_color,
       p.manufacturing_date,
       p.expiration_date,
       p.warranty_period,
       p.rating,
       p.weight_grams,
       p.discount_percentage
   FROM {{ ref('stg_product_data_PS') }} p
   WHERE p.product_id IS NOT NULL
)

SELECT * FROM product_enriched

{% if is_incremental() %}
    WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}

