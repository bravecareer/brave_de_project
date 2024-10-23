{{ 
    config(
    materialized='view'
) }}


with source as (

    select * from {{ source('de_project', 'inventory_data') }}

),

staging_data as (

    select
        inventory_id,
        warehouse_id,
        stock_level,
        restock_date,
        supplier_id,
        CAST(storage_condition AS VARCHAR(64)) AS storage_condition,
        CAST(inventory_status AS VARCHAR(64)) AS inventory_status,
        reorder_level,
        quantity_in_stock,
        sales_volume,
        safety_stock,
        average_monthly_demand,
        last_restock_date,
        next_restock_date,

        -- Extract product id from varchar string and cast to NUMBER
        {{convert_varchar_to_num('product_id')}} AS product_id, 
        
        -- Extract last audit date from timestamp and
        -- convert to date format
        CAST(last_audit_date AS DATE) AS last_audit_date, 

        -- Change data type from varchar to NUMBER(38,1)
        {{extract_num_with_comma('rating', 1)}} AS rating, 
      
        -- Change data type from varchar to NUMBER(38,1)
        {{extract_num_with_comma('weight', 2)}} AS prod_weight, 
        
        -- Change data type to NUMBER(38,2)
        {{extract_num_with_comma('discounts', 2)}} AS discounts

    from source

)

select * from staging_data