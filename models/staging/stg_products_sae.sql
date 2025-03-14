{{ config(materialized='table', schema='PROJECT_TEST') }}

WITH raw_data AS (
    SELECT 
        sku::VARCHAR(255) AS sku,
        name::VARCHAR(255) AS name,
        type::VARCHAR(255) AS type,
        price::NUMBER AS price,
        description::VARCHAR(16777216) AS description
    FROM {{ ref('raw_products') }}
    WHERE sku IS NOT NULL
      AND name IS NOT NULL
      AND type IS NOT NULL
      AND price IS NOT NULL
      AND description IS NOT NULL
),

cleaned AS (
    SELECT 
        UPPER(TRIM(sku)) AS sku,
        INITCAP(LOWER(TRIM(name))) AS product_name,
        INITCAP(LOWER(TRIM(type))) AS product_type,
        price,
        TRIM(description) AS description
    FROM raw_data
    WHERE price > 0
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY sku ORDER BY price) AS rn
    FROM cleaned
)

SELECT 
    sku,
    product_name,
    product_type,
    price / 100 AS price_in_dollars,
    description
FROM deduped
WHERE rn = 1
