-- 原 stg_product.sql 的内容 

{{
    config(
        materialized='view'
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
        {{ safe_numeric('price', min_value=0) }} as price,
        {{ default_value('supplier_id', "'UNKNOWN'") }} as supplier_id,
        {{ default_value('product_color', "'UNKNOWN'") }} as product_color,
        
        -- Product dates
        {{ default_value('manufacturing_date', 'CURRENT_DATE()') }} as manufacturing_date,
        {{ default_value('expiration_date', 'CURRENT_DATE()') }} as expiration_date,
        {{ default_value('warranty_period', "'UNKNOWN'") }} as warranty_period,
        
        -- Product metrics
        {{ safe_numeric('quantity_in_stock', min_value=0) }} as quantity_in_stock,
        {{ safe_numeric('rating', min_value=0, max_value=5) }} as rating,
        {{ safe_numeric('sales_volume', min_value=0) }} as sales_volume,
        {{ safe_numeric('weight_grams', min_value=0) }} as weight_grams,
        {{ safe_numeric('discount_percentage', min_value=0, max_value=100) }} as discount_percentage,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_product' as dbt_source

    from source
)

select * from staged 