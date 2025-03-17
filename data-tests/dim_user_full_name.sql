WITH input_data AS (
    SELECT 'john' AS first_name, 'doe' AS last_name
),
transformation AS (
    -- This replicates the logic in your dim_user model
    SELECT CONCAT(INITCAP(LOWER(first_name)), ' ', INITCAP(LOWER(last_name))) AS full_name
    FROM input_data
)
SELECT *
FROM transformation
WHERE full_name <> 'John Doe';
