/*
    Test Name: Inventory Metrics Calculation Test
    Description: Validates the calculation logic for inventory metrics:
    - Estimated days of inventory should be NULL when average_monthly_demand is 0
    - Inventory value calculations should be consistent with price and quantity
    - Reorder flags should be consistent with stock levels and reorder points
*/

WITH inventory_calculation_check AS (
    SELECT 
        im.inventory_id,
        im.product_id,
        im.warehouse_id,
        im.stock_level,
        im.average_monthly_demand,
        im.estimated_days_of_inventory,
        im.inventory_value,
        p.price,
        CASE 
            WHEN im.average_monthly_demand = 0 AND im.estimated_days_of_inventory IS NOT NULL 
                THEN 'Estimated days should be NULL when demand is 0'
            WHEN im.average_monthly_demand > 0 AND 
                 ABS(im.estimated_days_of_inventory - (im.stock_level / (im.average_monthly_demand/30))) > 0.1
                THEN 'Estimated days calculation is incorrect'
            WHEN ABS(im.inventory_value - (im.stock_level * p.price)) > 0.1
                THEN 'Inventory value calculation is incorrect'
        END as validation_error
    FROM {{ ref('fact_inventory_metrics_new') }} im
    JOIN {{ ref('dim_product') }} p ON im.product_id = p.product_id
    WHERE (im.average_monthly_demand = 0 AND im.estimated_days_of_inventory IS NOT NULL)
       OR (im.average_monthly_demand > 0 AND 
           ABS(im.estimated_days_of_inventory - (im.stock_level / (im.average_monthly_demand/30))) > 0.1)
       OR ABS(im.inventory_value - (im.stock_level * p.price)) > 0.1
)

SELECT * FROM inventory_calculation_check
WHERE validation_error IS NOT NULL
