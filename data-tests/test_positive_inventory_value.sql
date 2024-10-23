select stock_level, reorder_level, sales_volume, safety_stock, average_monthly_demand
from {{ ref('fact_inventory_management_michael_w') }}
where stock_level <= 0
OR reorder_level <= 0
OR sales_volume < 0
OR safety_stock <= 0
OR average_monthly_demand <= 0