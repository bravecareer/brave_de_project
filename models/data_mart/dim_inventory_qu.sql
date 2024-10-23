{{ config(
   materialized='incremental',
   unique_key='inventory_id'
) }}

with source as (

   select * from {{ ref('stg_inventory_data_qu') }}
),


inventory_data AS (
   SELECT
      inventory_id,
      storage_condition,
      inventory_status,
      last_audit_date,
      last_restock_date,
      restock_date,
      next_restock_date
   FROM source
)


SELECT * FROM inventory_data