{{ config(materialized='table', schema='PROJECT_TEST') }}

WITH raw_data AS (
    SELECT
         id::VARCHAR(255) AS id,
         name::VARCHAR(255) AS name,
         cost::NUMBER AS cost,
         perishable::BOOLEAN AS perishable,
         sku::VARCHAR(255) AS sku
    FROM {{ ref('raw_supplies') }}
    WHERE id IS NOT NULL
      AND name IS NOT NULL
      AND cost IS NOT NULL
      AND perishable IS NOT NULL
      AND sku IS NOT NULL
      AND cost > 0
),

cleaned AS (
    SELECT
         UPPER(TRIM(id)) AS supply_id,
         INITCAP(LOWER(TRIM(name))) AS supply_name,
         ROUND(cost, 2) AS cost,
         perishable,
         UPPER(TRIM(sku)) AS sku
    FROM raw_data
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY supply_id, sku ORDER BY cost) AS rn
    FROM cleaned
)

SELECT 
    supply_id,
    supply_name,
    cost,
    perishable,
    sku
FROM deduped
WHERE rn = 1
