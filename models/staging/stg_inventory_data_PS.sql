{{ config(
   materialized='incremental',
   unique_key='inventory_id'
) }}

WITH inventory_cleaned AS (
   SELECT
       i.INVENTORY_ID AS inventory_id,
       i.REORDER_LEVEL AS reorder_level,
       CAST(i.PRODUCT_ID AS VARCHAR) AS product_id,  -- Cast to VARCHAR
       i.QUANTITY_IN_STOCK AS quantity_in_stock,
       i.WAREHOUSE_ID AS warehouse_id,
       {{ round_to_decimal('i.RATING', 1) }} AS rating,  -- Using the `round_to_decimal` macro
       {{ cast_as_float('i.STOCK_LEVEL') }} AS stock_level,  -- Using the `cast_as_float` macro
       i.SALES_VOLUME AS sales_volume,
       {{ try_to_timestamp('i.RESTOCK_DATE') }} AS restock_date,  -- Using the `try_to_timestamp` macro
       {{ round_to_decimal('i.WEIGHT', 2) }} AS weight,  -- Reusing `round` macro for consistency
       i.SUPPLIER_ID AS supplier_id,
       {{ round_to_decimal('i.DISCOUNTS', 2) }} AS discounts,  -- Reusing `round` macro for consistency
       i.STORAGE_CONDITION AS storage_condition,
       i.SAFETY_STOCK AS safety_stock,
       i.INVENTORY_STATUS AS inventory_status,
       i.AVERAGE_MONTHLY_DEMAND AS average_monthly_demand,
       {{ try_to_timestamp('i.LAST_AUDIT_DATE') }} AS last_audit_date,  -- Using `try_to_timestamp` macro
       {{ try_to_timestamp('i.LAST_RESTOCK_DATE') }} AS last_restock_date,  -- Using `try_to_timestamp` macro
       {{ try_to_timestamp('i.NEXT_RESTOCK_DATE') }} AS next_restock_date  -- Using `try_to_timestamp` macro
   FROM {{ source('de_project', 'inventory_data') }} i
   WHERE i.PRODUCT_ID IS NOT NULL
     AND i.QUANTITY_IN_STOCK IS NOT NULL
     AND i.INVENTORY_STATUS IN ('in-stock', 'backordered')
)

SELECT *
FROM inventory_cleaned
{% if is_incremental() %}
    -- Ensure only new or updated rows are added
    WHERE inventory_id NOT IN (SELECT inventory_id FROM {{ this }})
{% endif %}
