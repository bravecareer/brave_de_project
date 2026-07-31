-- data-tests/test_dim_date_kl_weekend_logic.sql
-- 验证weekend逻辑正确 (业务关键)

SELECT 
    date_key,
    day_of_week,
    is_weekend
FROM {{ ref('dim_date_kl') }}
WHERE (day_of_week IN (0, 6) AND is_weekend = FALSE)
   OR (day_of_week NOT IN (0, 6) AND is_weekend = TRUE)