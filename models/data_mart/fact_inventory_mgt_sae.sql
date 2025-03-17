{{ config(
    materialized='incremental',
    schema='PROJECT_TEST',
    unique_key=['inventory_id', 'as_of_date'],
    cluster_by=['warehouse_id', 'product_id']
) }}

WITH base AS (
  SELECT
    inventory_id,
    product_id,
    warehouse_id,
    stock_level,
    reorder_level,
    quantity_in_stock,
    safety_stock,
    restock_date,
    CAST(restock_date AS DATE) AS as_of_date,
    load_timestamp,
    conversion_error_flag
  FROM {{ ref('stg_inventory_data_sae') }}
  WHERE inventory_id IS NOT NULL
),

aggregated AS (
  SELECT
    inventory_id,
    product_id,
    warehouse_id,
    as_of_date,
    MAX(stock_level) AS stock_level,
    MAX(reorder_level) AS reorder_level,
    MAX(quantity_in_stock) AS quantity_in_stock,
    MAX(safety_stock) AS safety_stock,
    MAX(load_timestamp) AS last_updated,
    MAX(conversion_error_flag) AS conversion_error_flag
  FROM base
  GROUP BY inventory_id, product_id, warehouse_id, as_of_date
),

-- Deduplicate dim_product_sae in case of duplicates.
deduped_prod AS (
  SELECT 
    product_id,
    product_name,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY load_timestamp DESC) AS rn
  FROM {{ ref('dim_product_sae') }}
)

SELECT
  agg.inventory_id,
  agg.product_id,
  dp.product_name,
  agg.warehouse_id,
  agg.as_of_date,
  agg.stock_level,
  agg.reorder_level,
  agg.quantity_in_stock,
  agg.safety_stock,
  agg.last_updated,
  agg.conversion_error_flag
FROM aggregated AS agg
LEFT JOIN deduped_prod AS dp
  ON agg.product_id = dp.product_id
WHERE dp.rn = 1