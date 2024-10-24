-- Fact Table: fact_inventory_management_PS

{{ config(
  materialized='incremental',
  unique_key=['product_id', 'warehouse_id']
) }}

WITH inventory_summary AS (
  SELECT
    i.PRODUCT_ID AS product_id,
    i.WAREHOUSE_ID AS warehouse_id,
    i.QUANTITY_IN_STOCK AS quantity_in_stock,
    i.REORDER_LEVEL AS reorder_level,
    i.SALES_VOLUME AS sales_volume,
    i.AVERAGE_MONTHLY_DEMAND AS average_monthly_demand,
    i.STOCK_LEVEL AS stock_level,
    i.LAST_RESTOCK_DATE AS last_restock_date,
    i.NEXT_RESTOCK_DATE AS next_restock_date,
    i.INVENTORY_STATUS AS inventory_status
  FROM {{ ref('stg_inventory_data_PS') }} i
  WHERE i.QUANTITY_IN_STOCK IS NOT NULL
)

SELECT * FROM inventory_summary

{% if is_incremental() %}
  WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}
