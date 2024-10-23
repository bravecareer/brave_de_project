{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}

with source as (

   select * from {{ref('stg_product_data_qu')}}
),

product_data AS (
   SELECT
   
       product_id,
       product_name,
       product_category,
       product_color,
       supplier_id,
       manufacturing_date,
       expiration_date,
       warranty_period_months
   
   FROM source
)


SELECT * FROM product_data