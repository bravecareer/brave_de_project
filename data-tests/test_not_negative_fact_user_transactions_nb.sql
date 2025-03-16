-- Test to ensure total_amount is always non-negative
SELECT * FROM {{ ref('fact_user_transaction_nb') }} 
WHERE total_amount < 0;
