WITH input_data AS (
    SELECT CAST('1980-01-01' AS DATE) AS dob
),
transformation AS (
    SELECT DATEDIFF('year', dob, CAST('2024-01-01' AS DATE)) AS age_years
    FROM input_data
)
SELECT *
FROM transformation
WHERE age_years <> 44;
