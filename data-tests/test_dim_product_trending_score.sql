WITH input_data AS (
    SELECT 4.5 AS rating, 100 AS sales_volume
),
transformation AS (
    SELECT ROUND(rating * LOG(10, sales_volume + 1), 2) AS trending_score
    FROM input_data
)
SELECT *
FROM transformation
WHERE trending_score <> 9.02;
