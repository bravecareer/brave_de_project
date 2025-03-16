{{ config(materialized='table', schema='PROJECT_TEST', cluster_by=['product_id']) }}

WITH raw_data AS (
    SELECT 
         PRODUCT_ID::VARCHAR(255) AS product_id,
         PRODUCT_NAME::VARCHAR(255) AS product_name,
         PRODUCT_CATEGORY::VARCHAR(255) AS product_category,
         PRICE::NUMBER(38,2) AS price,
         SUPPLIER_ID::VARCHAR(255) AS supplier_id,
         PRODUCT_COLOR::VARCHAR(255) AS product_color,
         MANUFACTURING_DATE::DATE AS manufacturing_date,
         EXPIRATION_DATE::DATE AS expiration_date,
         WARRANTY_PERIOD::VARCHAR(255) AS warranty_period,
         QUANTITY_IN_STOCK::NUMBER(38,0) AS quantity_in_stock,
         RATING::NUMBER(38,2) AS rating,
         SALES_VOLUME::NUMBER(38,0) AS sales_volume,
         WEIGHT_GRAMS::NUMBER(38,0) AS weight_grams,
         DISCOUNT_PERCENTAGE::NUMBER(38,0) AS discount_percentage
    FROM {{ source('de_project', 'product_data') }}
    WHERE PRODUCT_ID IS NOT NULL
      AND PRODUCT_NAME IS NOT NULL
      AND PRODUCT_CATEGORY IS NOT NULL
      AND PRICE IS NOT NULL
      AND WEIGHT_GRAMS IS NOT NULL
      AND WEIGHT_GRAMS >= 0
),

cleaned AS (
    SELECT 
         TRIM(product_id) AS product_id,
         INITCAP(LOWER(TRIM(product_name))) AS product_name,
         INITCAP(LOWER(TRIM(product_category))) AS product_category,
         price,
         TRIM(supplier_id) AS supplier_id,
         INITCAP(LOWER(TRIM(product_color))) AS product_color,
         manufacturing_date,
         expiration_date,
         TRIM(warranty_period) AS warranty_period,
         -- Extract warranty months if the warranty period is formatted like "22 months"
         TRY_CAST(REGEXP_SUBSTR(warranty_period, '\\d+') AS INTEGER) AS warranty_months,
         quantity_in_stock,
         ROUND(rating, 2) AS rating,
         sales_volume,
         weight_grams,
         discount_percentage,
         -- Calculated fields:
         ROUND(price * (1 - discount_percentage / 100.0), 2) AS discounted_price,
         DATEDIFF('day', manufacturing_date, CURRENT_DATE()) AS product_age_days,
         DATEDIFF('day', CURRENT_DATE(), expiration_date) AS days_to_expiration,
         CASE 
           WHEN expiration_date < CURRENT_DATE() THEN TRUE
           ELSE FALSE 
         END AS is_expired,
         CURRENT_TIMESTAMP() AS load_timestamp
    FROM raw_data
    WHERE price > 0
      AND quantity_in_stock >= 0
      AND sales_volume >= 0
      AND discount_percentage BETWEEN 0 AND 100
      AND rating BETWEEN 0 AND 5
      AND manufacturing_date < expiration_date
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY load_timestamp DESC) AS rn
    FROM cleaned
)

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
    warranty_months,
    quantity_in_stock,
    rating,
    sales_volume,
    weight_grams,
    discount_percentage,
    discounted_price,
    product_age_days,
    days_to_expiration,
    is_expired,
    load_timestamp
FROM deduped
WHERE rn = 1
