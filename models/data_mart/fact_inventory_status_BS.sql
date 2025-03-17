{{ config(
   materialized='incremental',
   unique_key=['inventory_id', 'last_audit_date'],
   cluster_by='inventory_id'      
) }}


WITH inventory_stats as (
SELECT
    INVENTORY_ID,
    PRODUCT_ID,
    WAREHOUSE_ID,
    STOCK_LEVEL,
    RESTOCK_DATE,
    INVENTORY_STATUS,
    LAST_AUDIT_DATE,
    REORDER_LEVEL,
    SAFETY_STOCK,
    AVERAGE_MONTHLY_DEMAND,  
    case 
        when STOCK_LEVEL < REORDER_LEVEL then 'Reorder Needed'
        when STOCK_LEVEL <= SAFETY_STOCK then 'Low Stock'
        else 'Sufficient Stock'
    end as STOCK_STATUS,
    current_timestamp() AS dbt_loaded_at,
    'stg_inventory_data' AS dbt_source 
FROM {{ ref ('stg_inventory_data_BS') }}
)
SELECT *
FROM inventory_stats
 WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
1=1
{% endif %}

