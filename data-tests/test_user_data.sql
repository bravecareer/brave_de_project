-- Test: test_user_data
-- Description: This test checks if the account_status in the dim_user_qu table is 'active',
-- if the email in the dim_user_qu table is a valid email address, and if the loyalty_points_balance
-- in the dim_user_qu table is greater than or equal to 0.
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