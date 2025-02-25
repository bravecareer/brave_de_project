-- 原 stg_product.sql 的内容 

{{
    config(
        materialized='table',
        unique_key='product_id'
    )
}}

-- Staging layer for product data
with source as (
    select * from {{ source('de_project', 'product_data') }}
),

staged as (
    select
        -- Identifiers
        COALESCE(product_id, 'UNKNOWN') as product_id,
        COALESCE(product_name, 'UNKNOWN') as product_name,
        COALESCE(product_category, 'UNKNOWN') as product_category,
        
        -- Product details
        case 
            when price is null then 0
            when price < 0 then 0 
            else price 
        end as price,
        COALESCE(supplier_id, 'UNKNOWN') as supplier_id,
        COALESCE(product_color, 'UNKNOWN') as product_color,
        
        -- Product dates
        COALESCE(manufacturing_date, CURRENT_DATE()) as manufacturing_date,
        COALESCE(expiration_date, CURRENT_DATE()) as expiration_date,
        COALESCE(warranty_period, 'UNKNOWN') as warranty_period,
        
        -- Product metrics
        case 
            when quantity_in_stock is null then 0
            when quantity_in_stock < 0 then 0 
            else quantity_in_stock 
        end as quantity_in_stock,
        case 
            when rating is null then 0
            when rating < 0 then 0 
            else rating 
        end as rating,
        case 
            when sales_volume is null then 0
            when sales_volume < 0 then 0 
            else sales_volume 
        end as sales_volume,
        case 
            when weight_grams is null then 0
            when weight_grams < 0 then 0 
            else weight_grams 
        end as weight_grams,
        case 
            when discount_percentage is null then 0
            when discount_percentage < 0 then 0
            when discount_percentage > 100 then 100
            else discount_percentage 
        end as discount_percentage,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_product' as dbt_source

    from source
)

select * from staged 