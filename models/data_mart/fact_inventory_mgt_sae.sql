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
)

SELECT *
FROM aggregated
