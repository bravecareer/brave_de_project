{{ config(
   materialized='incremental',
   unique_key=['inventory_id', 'last_audit_date'],
   cluster_by=['inventory_id', 'last_audit_date'],   
   on_schema_change='fail'
) }}

WITH inventory_data AS (
    SELECT
        inventory_id,
        reorder_level,
        product_id,
        quantity_in_stock,
        warehouse_id,
        rating,
        stock_level,
        sales_volume,
        restock_date,
        weight,
        supplier_id,
        discounts,
        storage_condition,
        safety_stock,
        inventory_status,
        average_monthly_demand,
        last_audit_date,
        last_restock_date,
        next_restock_date,
        current_timestamp() AS dbt_loaded_at,
        'de_project_inventory_data' AS dbt_source
    FROM {{ source('de_project', 'inventory_data') }}
)

SELECT
    inventory_id,
    reorder_level,
    product_id,
    quantity_in_stock,
    warehouse_id,
    rating,
    stock_level,
    sales_volume,
    restock_date,
    weight,
    supplier_id,
    discounts,
    storage_condition,
    safety_stock,
    inventory_status,
    average_monthly_demand,
    last_audit_date,
    last_restock_date,
    next_restock_date,
    dbt_loaded_at,
    dbt_source
FROM inventory_data
WHERE
{% if is_incremental() %}
    last_audit_date > (SELECT max(last_audit_date) FROM {{ this }})
{% else %}
    1=1  -- run in case there is a full refresh
{% endif %}
