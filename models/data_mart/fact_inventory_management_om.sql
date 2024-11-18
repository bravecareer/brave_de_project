{{ config(
    materialized='incremental',
    unique_key=['inventory_id', 'product_id', 'warehouse_id']
) }}

WITH inventory_data AS (
    SELECT
        i.inventory_id,
        i.product_id,
        i.warehouse_id,
        i.supplier_id,
        p.price,
        i.stock_level,
        i.reorder_level,
        SUM(i.quantity_in_stock) AS quantity_in_stock,
        SUM(i.sales_volume) AS sales_volume,
        i.safety_stock,
        i.average_monthly_demand,
        p.rating,
        p.weight_grams AS prod_weight,
        i.discounts,
        i.last_restock_date,
        i.next_restock_date,
        CASE
            WHEN i.last_restock_date < CURRENT_DATE AND CURRENT_DATE < i.next_restock_date THEN 'On Track'
            ELSE 'Action Needed'
        END AS restock_status
    FROM
       --brave_database.de_project.dim_inventory i
       {{ source('de_project', 'dim_inventory') }} i
       --    {{ ref('brave_database.de_project', 'dim_inventory') }} i
    JOIN
       --brave_database.de_project.dim_product p
        {{ source('de_project', 'dim_product_om') }} p
        --{{ ref('brave_database.de_project', 'dim_product') }} p
    ON
        i.product_id = p.product_id
    GROUP BY
        i.inventory_id, i.product_id, i.warehouse_id, i.supplier_id, p.price, i.stock_level, i.reorder_level, i.safety_stock, i.average_monthly_demand, p.rating, p.weight_grams, i.discounts, i.last_restock_date, i.next_restock_date
)

SELECT
    *
FROM
    inventory_data
