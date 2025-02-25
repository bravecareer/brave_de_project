-- Verify transaction amount consistency
-- Ensures that transaction total_amount matches with quantity_sold * product price

WITH transaction_check AS (
    SELECT 
        t.user_id,
        t.product_id,
        t.search_event_id,
        t.timestamp,
        t.total_amount,
        t.quantity_sold,
        p.price,
        ABS(t.total_amount - (t.quantity_sold * p.price)) as amount_diff
    FROM {{ ref('fact_user_transaction') }} t
    JOIN {{ ref('dim_product') }} p ON t.product_id = p.product_id
    WHERE ABS(t.total_amount - (t.quantity_sold * p.price)) > 0.01  -- Allow for small rounding differences
)

SELECT * FROM transaction_check
