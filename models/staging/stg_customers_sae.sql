{{ config(materialized='table', schema='PROJECT_TEST') }}

WITH raw_data AS (
    SELECT 
        id::VARCHAR(36) AS id,
        name::VARCHAR(255) AS name
    FROM {{ ref('raw_customers') }}
    WHERE id IS NOT NULL
      AND name IS NOT NULL
),

cleaned AS (
    SELECT
        REPLACE(REPLACE(id, '-', ''), ' ', '') AS customer_id,
        LOWER(TRIM(
            CASE 
                WHEN REGEXP_LIKE(name, '^(mr|mrs|dr)s?\\.?\\s+', 'i') 
                    THEN REGEXP_REPLACE(name, '^(mr|mrs|dr)s?\\.?\\s+', '', 1, 1, 'i')
                ELSE name
            END
        )) AS customer_name
    FROM raw_data
)

SELECT 
    customer_id,
    INITCAP(customer_name) AS customer_name
FROM cleaned
WHERE LENGTH(customer_name) > 2