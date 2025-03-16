-- Test for unique inventory_id
SELECT inventory_id, COUNT(*) 
FROM {{ ref('stg_inventory_status_nb') }}  
GROUP BY inventory_id 
HAVING COUNT(*) > 1;
