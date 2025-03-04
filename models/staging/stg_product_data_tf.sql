-- 原 stg_product.sql 的内容 

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='delete+insert'
    )
}}

-- Staging layer for product data
with source as (
    select * from {{ source('de_project', 'product_data') }}
),

staged as (
    select
        -- Identifiers
        {{ default_value('product_id', "'UNKNOWN'") }} as product_id,
        {{ default_value('product_name', "'UNKNOWN'") }} as product_name,
        {{ default_value('product_category', "'UNKNOWN'") }} as product_category,
        
        -- Product details
        CASE 
            WHEN price IS NULL THEN 0
            WHEN price < 0 THEN 0
            ELSE price
        END as price,
        {{ default_value('supplier_id', "'UNKNOWN'") }} as supplier_id,
        {{ default_value('product_color', "'UNKNOWN'") }} as product_color,
        
        -- Product dates
        {{ default_value('manufacturing_date', 'CURRENT_DATE()') }} as manufacturing_date,
        {{ default_value('expiration_date', 'CURRENT_DATE()') }} as expiration_date,
        {{ default_value('warranty_period', "'UNKNOWN'") }} as warranty_period,
        
        -- Product metrics
        CASE 
            WHEN quantity_in_stock IS NULL THEN 0
            WHEN quantity_in_stock < 0 THEN 0
            ELSE quantity_in_stock
        END as quantity_in_stock,
        CASE 
            WHEN rating IS NULL THEN 0
            WHEN rating < 0 THEN 0
            WHEN rating > 5 THEN 5
            ELSE rating
        END as rating,
        CASE 
            WHEN sales_volume IS NULL THEN 0
            WHEN sales_volume < 0 THEN 0
            ELSE sales_volume
        END as sales_volume,
        CASE 
            WHEN weight_grams IS NULL THEN 0
            WHEN weight_grams < 0 THEN 0
            ELSE weight_grams
        END as weight_grams,
        CASE 
            WHEN discount_percentage IS NULL THEN 0
            WHEN discount_percentage < 0 THEN 0
            WHEN discount_percentage > 100 THEN 100
            ELSE discount_percentage
        END as discount_percentage,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_product' as dbt_source

    from source
)

select * from staged 