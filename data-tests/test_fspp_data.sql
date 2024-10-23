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