SELECT DISTINCT inventory_status
FROM {{ ref('dim_inventory') }}
WHERE inventory_status NOT IN ('in-stock', 'backordered')
