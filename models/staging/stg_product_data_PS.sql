{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}

WITH product_cleaned AS (
   SELECT
       CAST(p.PRODUCT_ID AS VARCHAR) AS product_id,  -- Cast to VARCHAR
       p.PRODUCT_NAME AS product_name,
       p.PRODUCT_CATEGORY AS product_category,
       {{ cast_as_float('p.PRICE') }} AS price,
       p.SUPPLIER_ID AS supplier_id,
       p.PRODUCT_COLOR AS product_color,
       {{ try_to_timestamp('p.MANUFACTURING_DATE') }} AS manufacturing_date,
       {{ try_to_timestamp('p.EXPIRATION_DATE') }} AS expiration_date,
       p.WARRANTY_PERIOD AS warranty_period,
       p.QUANTITY_IN_STOCK AS quantity_in_stock,
       {{ round_to_decimal('p.RATING', 1) }} AS rating,
       p.SALES_VOLUME AS sales_volume,
       {{ round_to_decimal('p.WEIGHT_GRAMS', 2) }} AS weight_grams,
       {{ round_to_decimal('p.DISCOUNT_PERCENTAGE', 2) }} AS discount_percentage
   FROM {{ source('de_project', 'product_data') }} p
   WHERE p.PRODUCT_ID IS NOT NULL
     AND p.QUANTITY_IN_STOCK IS NOT NULL
)

SELECT *
FROM product_cleaned
{% if is_incremental() %}
    -- Ensure only new or updated rows are added
    WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}
