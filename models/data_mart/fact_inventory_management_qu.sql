{{ config(
    materialized='incremental',
    unique_key=['inventory_id','warehouse_id', 'supplier_id']
)}}

WITH inventory_data AS (

    select * from {{ ref('stg_inventory_data_qu') }}

),

product_data AS (

    select * from {{ ref('stg_product_data_qu') }} 

),


inventory_management AS (
    SELECT
    
    id.inventory_id,
    id.product_id,
    id.warehouse_id,
    id.supplier_id,

    pd.price,
    id.stock_level,
    id.reorder_level,
    id.quantity_in_stock,
    id.sales_volume,
    id.safety_stock,
    id.average_monthly_demand,
    id.rating,
    id.prod_weight,
    id.discounts
                
    FROM inventory_data id
    LEFT JOIN product_data pd ON id.product_id = pd.product_id
)

SELECT * FROM inventory_management
WHERE price >= discounts