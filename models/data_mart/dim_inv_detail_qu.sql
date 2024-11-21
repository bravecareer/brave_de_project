{{ config(
   materialized='incremental',
   unique_key='inv_detail_id'
) }}

with source as (

   select * from {{ ref('stg_inventory_data_qu') }}
),


inventory_data AS (
   SELECT
      {{dbt_utils.generate_surrogate_key(['storage_condition',
               'inventory_status'])}} as inv_detail_id,
      storage_condition,
      inventory_status
      
   FROM source
   GROUP BY storage_condition, inventory_status
)


SELECT * FROM inventory_data