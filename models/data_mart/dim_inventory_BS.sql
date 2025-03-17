{{ config(
   materialized='incremental',
   unique_key=['inventory_id','product_id']
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
       'stg_inventory_data' AS dbt_source 
   FROM {{ ref('stg_inventory_data_BS') }} 
   

)

SELECT * 
FROM inventory_data
 WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
1=1
{% endif %}



