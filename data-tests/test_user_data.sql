SELECT
    COUNT(*) AS invalid_rows
FROM
    {{ ref('dim_user_qu') }}
WHERE
    account_status != 'active'
    OR NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    OR loyalty_points_balance < 0
HAVING
    invalid_rows > 0    