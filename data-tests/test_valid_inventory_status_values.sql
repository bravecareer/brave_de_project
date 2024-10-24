-- Test for valid inventory status values in fact_inventory_management_PS
SELECT
  DISTINCT inventory_status
FROM {{ ref('fact_inventory_management_PS') }}
WHERE inventory_status NOT IN ('in-stock', 'backordered')
