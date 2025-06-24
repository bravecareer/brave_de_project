{{config(
	materialized ='view',
	unique_keys ='inventory_id'
) }}

SELECT 
    i.inventory_id,
	i.quantity_in_stock,
	i.stock_level,
	i.restock_date,
	i.safety_stock,
	i.product_id
FROM {{ref ('raw_inventory_data_sh') }} i LEFT JOIN {{ref ('raw_purchased_products_sh') }} p 
	ON i.product_id = p.product_id
--WHERE last_audit_date >= CURRENT_DATE() - 5    
