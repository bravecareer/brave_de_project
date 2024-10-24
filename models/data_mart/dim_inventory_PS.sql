-- Dimension Table: dim_inventory

{{ config(
  materialized='table',
  unique_key='product_id'
) }}

WITH inventory_enriched AS (
  SELECT
    p.PRODUCT_ID AS product_id,
    p.PRODUCT_NAME AS product_name,
    p.PRODUCT_CATEGORY AS product_category,
    p.SUPPLIER_ID AS supplier_id,
    i.WAREHOUSE_ID AS warehouse_id,
    i.REORDER_LEVEL AS reorder_level,
    i.STOCK_LEVEL AS stock_level,
    i.DISCOUNTS AS discounts,
    i.STORAGE_CONDITION AS storage_condition,
    i.INVENTORY_STATUS AS inventory_status,
    i.AVERAGE_MONTHLY_DEMAND AS average_monthly_demand,
    i.WEIGHT AS weight,
    i.RATING AS rating
  FROM {{ ref('stg_product_data_PS') }} p
  LEFT JOIN {{ ref('stg_inventory_data_PS') }} i ON p.PRODUCT_ID = i.PRODUCT_ID
  WHERE p.PRODUCT_ID IS NOT NULL
)

SELECT * FROM inventory_enriched
