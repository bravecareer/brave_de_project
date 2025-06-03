{{config(
	materialized ='view',
	unique_keys ='inventory_id'
) }}


WITH purchased_products AS(


SELECT uj.product_id,
       COUNT(uj.has_purchase) AS quantity_sold,

FROM {{source('de_project', 'user_journey')}} uj INNER JOIN {{source('de_project', 'product_data')}} p 
	ON uj.product_id = p.product_id
WHERE uj.has_purchase =TRUE
GROUP BY uj.product_id

),

inventory_by_product AS(
SELECT 
    i.inventory_id,
	i.quantity_in_stock,
	i.stock_level,
	i.restock_date,
	i.safety_stock,
    q.quantity_sold,
	i.product_id
FROM {{source('de_project', 'inventory_data') }} i LEFT JOIN purchased_products q
	ON i.product_id = q.product_id
--WHERE last_audit_date >= CURRENT_DATE() - 5    
ORDER BY i.quantity_in_stock ASC

)

Select * from inventory_by_product