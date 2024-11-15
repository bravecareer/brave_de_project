{{ config(
    materialized='incremental',
    unique_key=['inventory_id', 'product_id', 'warehouse_id']
) }}

WITH inventory_data AS (
    SELECT
        inv.inventory_id,
        inv.product_id,
        inv.warehouse_id,
        inv.storage_condition,
    FROM {{ source('de_project', 'inventory_data') }} inv
),

product_data AS (
    SELECT
        p.product_id,
        p.product_name,
        p.product_category
    FROM {{ source('de_project', 'product_data') }} p
),

final AS (
    SELECT
        inv.inventory_id,
        inv.product_id,
        inv.warehouse_id,
        p.product_name,
        p.product_category,
        inv.storage_condition
   FROM inventory_data inv
   LEFT JOIN product_data p ON inv.product_id = p.product_id
)

SELECT * FROM final