/*
    Test Name: Valid Product IDs Test
    Description: Validates that all product_ids in fact tables exist in the product dimension table
    Note: This test ignores 'UNKNOWN' product_ids as they are handled in the staging layer
*/

-- Test product_ids in fact_product_performance
WITH product_performance_ids AS (
    SELECT DISTINCT
      f.product_id as invalid_product_id,
      'fact_product_performance' as source_table
    FROM {{ ref('fact_product_performance') }} f
    LEFT JOIN {{ source('de_project', 'product_data') }} p
    ON f.product_id = p.product_id
    WHERE p.product_id IS NULL
      AND f.product_id IS NOT NULL
      AND f.product_id != 'UNKNOWN'
),

-- Test product_ids in fact_inventory_metrics_new
inventory_metrics_ids AS (
    SELECT DISTINCT
      f.product_id as invalid_product_id,
      'fact_inventory_metrics_new' as source_table
    FROM {{ ref('fact_inventory_metrics_new') }} f
    LEFT JOIN {{ source('de_project', 'product_data') }} p
    ON f.product_id = p.product_id
    WHERE p.product_id IS NULL
      AND f.product_id IS NOT NULL
      AND f.product_id != 'UNKNOWN'
)

-- Combine all results
SELECT * FROM product_performance_ids
UNION ALL
SELECT * FROM inventory_metrics_ids
