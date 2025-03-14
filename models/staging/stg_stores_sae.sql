{{ config(materialized='table', schema='PROJECT_TEST') }}

WITH raw_data AS (
    SELECT 
        id::VARCHAR(36) AS id,
        name::VARCHAR(255) AS name,
        opened_at::TIMESTAMP_NTZ AS opened_at,
        tax_rate::FLOAT AS tax_rate
    FROM {{ ref('raw_stores') }}
    WHERE id IS NOT NULL
      AND name IS NOT NULL
      AND opened_at IS NOT NULL
      AND tax_rate IS NOT NULL
      AND tax_rate >= 0
      AND opened_at < CURRENT_TIMESTAMP()
),

cleaned AS (
    SELECT 
        -- Standardize the store ID by removing hyphens and spaces
        REPLACE(REPLACE(id, '-', ''), ' ', '') AS store_id,
        -- Clean the store name: trim and convert to title case
        INITCAP(LOWER(TRIM(name))) AS store_name,
        -- Format the opened_at timestamp to a consistent string format
        TO_CHAR(opened_at, 'YYYY-MM-DD HH24:MI:SS') AS opened_at,
        -- Round the tax_rate to 4 decimal places
        ROUND(tax_rate, 4) AS tax_rate
    FROM raw_data
)

SELECT 
    store_id,
    store_name,
    opened_at,
    tax_rate
FROM cleaned
