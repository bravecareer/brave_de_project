WITH input_data AS (
    SELECT 40.00 AS price
),
transformation AS (
    SELECT 
        CASE 
            WHEN price < 50 THEN 'Low'
            WHEN price >= 50 AND price < 150 THEN 'Medium'
            ELSE 'High'
        END AS price_tier
    FROM input_data
)
SELECT *
FROM transformation
WHERE price_tier <> 'Low';
