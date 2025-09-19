{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

WITH product_data AS (
  SELECT
      product_id,
      REGEXP_REPLACE(product_name, '\\s*\\(\\d+\\)$')      AS product_name, -- remove trailing (n)
      product_category,
      CAST(price AS DECIMAL(18,2))                         AS price,
      product_color,
      COALESCE((manufacturing_date), CURRENT_DATE)     AS manufacturing_date,
      TRY_TO_DATE(expiration_date)                         AS expiration_date,
      warranty_period,
      CAST(rating AS DECIMAL(18,1))                          AS rating,
      weight_grams,
      discount_percentage,
      sales_volume,
      supplier_id,
      quantity_in_stock
  FROM {{ source('de_project','product_data') }}
  {% if is_incremental() %}
    -- Process only recent rows on incremental runs (2-day sliding window)
    WHERE DATE(manufacturing_date) >= CURRENT_DATE() - 2
  {% endif %}
),

cleaned_product AS (
  SELECT *
  FROM product_data
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY manufacturing_date DESC NULLS LAST) = 1
)

SELECT * , '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at
FROM cleaned_product
