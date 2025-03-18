-- dim_product.sql: Creates a table of recent products, unique on product_id
{{ config(
    materialized='table',
    unique_key='product_id'
)}}

-- Define date threshold for recent manufacturing (last 5 days)
WITH date_threshold AS (
    SELECT DATEADD(day, -5, CURRENT_DATE()) AS recent_date
),

-- Select recent products with non-expired status and valid product_id
recent_products AS (
    SELECT
        product_id,              -- Unique product identifier
        weight_grams,            -- Product weight in grams
        discount_percentage,     -- Discount percentage applied
        rating,                  -- Customer or quality rating
        warranty_period,         -- Warranty duration
        price,                   -- Product price
        product_name,            -- Name of the product
        product_category,        -- Broad product category
        product_sub_category,    -- Specific product sub-category
        manufacturing_date       -- Product manufacturing date
    FROM {{ source('de_project', 'product_data') }}
    WHERE manufacturing_date >= (SELECT recent_date FROM date_threshold)
      AND expiration_date IS NULL   -- Exclude products already expired
      AND product_id IS NOT NULL    -- Ensure product_id is always available
)

-- Final explicit selection from cleaned recent_products CTE
SELECT
    product_id,
    weight_grams,
    discount_percentage,
    rating,
    warranty_period,
    price,
    product_name,
    product_category,
    product_sub_category,
    manufacturing_date
FROM recent_products
