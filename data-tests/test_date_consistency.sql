WITH validation AS (
    SELECT 
        inventory_id,
        CASE 
            WHEN LAST_RESTOCK_DATE < RESTOCK_DATE AND RESTOCK_DATE < NEXT_RESTOCK_DATE 
            THEN 1
            ELSE 0
        END AS valid_dates
    FROM {{ ref('stg_inventory_data_sae') }}
)
SELECT *
FROM validation
WHERE valid_dates = 0
