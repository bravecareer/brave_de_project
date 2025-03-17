{{ config(materialized='table', schema='PROJECT_TEST', cluster_by=['product_id']) }}

WITH raw AS (
    SELECT
        product_id,
        INITCAP(LOWER(product_name)) AS product_name,
        INITCAP(LOWER(product_category)) AS product_category,
        price,
        rating,
        sales_volume,
        quantity_in_stock,
        discount_percentage,
        weight_grams,
        -- These columns come directly from the staging table:
        discounted_price,
        warranty_period,
        warranty_months,
        product_age_days,
        days_to_expiration,
        is_expired,
        load_timestamp
    FROM {{ ref('stg_product_data_sae') }}
    WHERE product_id IS NOT NULL
      AND product_name IS NOT NULL
),

transformed AS (
    SELECT
         product_id,
         product_name,
         product_category,
         price,
         rating,
         sales_volume,
         quantity_in_stock,
         discount_percentage,
         weight_grams,
         -- Recalculate discounted price (or you can use the staged column if it’s already accurate)
         ROUND(price * (1 - discount_percentage / 100.0), 2) AS discounted_price,
         -- Price tier: adjust thresholds based on your business
         CASE 
            WHEN price < 50 THEN 'Low'
            WHEN price >= 50 AND price < 150 THEN 'Medium'
            ELSE 'High'
         END AS price_tier,
         -- Trending score: combining rating and sales volume with a logarithmic scale
         ROUND(rating * LOG(10, sales_volume + 1), 2) AS trending_score,
         -- Categorize weight into buckets
         CASE
            WHEN weight_grams IS NULL THEN NULL
            WHEN weight_grams < 500 THEN 'Light'
            WHEN weight_grams >= 500 AND weight_grams < 1500 THEN 'Medium'
            ELSE 'Heavy'
         END AS weight_category,
         warranty_period,
         warranty_months,
         product_age_days,
         days_to_expiration,
         is_expired,
         load_timestamp
    FROM raw
)

SELECT *
FROM transformed
