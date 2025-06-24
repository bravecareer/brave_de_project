SELECT 
    inventory_id,
	quantity_in_stock,
	stock_level,
	restock_date,
	safety_stock,
	product_id
FROM {{source('de_project', 'inventory_data') }}
--WHERE last_audit_date >= CURRENT_DATE() - 5    
