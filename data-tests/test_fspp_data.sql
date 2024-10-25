-- Test: test_fspp_data
-- Description: This test checks if the user_id in the fact_search_product_performance_qu
-- table is present in the dim_user_qu table and if the discount_percentage in the
-- fact_search_product_performance_qu table is between 0 and 100, given the account_status
-- in the dim_user_qu table is 'active'.
SELECT
    COUNT(*) AS invalid_rows
FROM
    {{ ref('fact_search_product_performance_qu') }} fsp
JOIN
    {{ ref('dim_user_qu') }} du
ON
    fsp.user_id = du.user_id
WHERE
    (discount_percentage < 0 OR discount_percentage > 100)
    OR du.account_status != 'active'
HAVING
    invalid_rows > 0    