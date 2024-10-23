{{ config(
    materialized='view'
) }}

with source as (

    select * from {{ source('de_project', 'product_data') }}

),

staging_data as (

    select
        -- Extract product id from varchar string and cast to NUMBER
        {{ convert_varchar_to_num('product_id')}} AS product_id, 
        
        -- Extract warranty_period from varchar to NUMBER
        {{convert_varchar_to_num('warranty_period')}} AS warranty_period_months, 
      
        -- Converting to smaller string
        CAST(product_name AS VARCHAR(64)) AS product_name,
        CAST(product_category AS VARCHAR(64)) AS product_category,
        CAST(product_color AS VARCHAR(64)) AS product_color,

        price,
        CAST(supplier_id AS VARCHAR(64)) AS supplier_id,       
        manufacturing_date,
        expiration_date,
        quantity_in_stock,
        rating,
        sales_volume,
        weight_grams,
        discount_percentage

    from source

)

select * from staging_data