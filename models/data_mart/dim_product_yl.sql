{{ config(
   materialized='incremental',
   unique_key='product_id',
   incremental_strategy='merge'
) }}


WITH product_data AS (
   SELECT
       product_id,
       product_name,
       product_category,
       price,
       product_color,
       manufacturing_date,
       expiration_date,
       warranty_period,
       rating,
       weight_grams,
       discount_percentage
   FROM {{ ref('stg_product_data_yl') }} 
   {% if is_incremental() %}
    -- Process only recent rows on incremental runs (2-day sliding window)
    WHERE DATE(manufacturing_date) >= CURRENT_DATE() - 2
  {% endif %}
)


SELECT *, '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at FROM product_data