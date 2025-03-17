-- Ensure all product_ids in the inventory model exist in the valid products source
SELECT
  p.product_id
FROM {{ ref('fact_inventory_bl') }}
LEFT JOIN {{ source('de_project', 'product_data') }} p
ON {{ ref('fact_inventory_bl') }}.product_id = p.product_id
WHERE p.product_id IS NULL