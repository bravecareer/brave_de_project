{{ config(materialized='table', schema='PROJECT_TEST') }}

WITH raw_data AS (
    SELECT 
        id::VARCHAR(36) AS id,
        order_id::VARCHAR(36) AS order_id,
        sku::VARCHAR(255) AS sku
    FROM {{ ref('raw_items') }}
    WHERE id IS NOT NULL
      AND order_id IS NOT NULL
      AND sku IS NOT NULL
),

cleaned AS (
    SELECT
        REPLACE(REPLACE(id, '-', ''), ' ', '') AS item_id,
        
        REPLACE(REPLACE(order_id, '-', ''), ' ', '') AS order_id,
        
        UPPER(TRIM(sku)) AS sku
    FROM raw_data
)

SELECT 
    item_id,
    order_id,
    sku
FROM cleaned
