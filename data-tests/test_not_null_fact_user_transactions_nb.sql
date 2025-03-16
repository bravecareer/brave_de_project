-- Test for NOT NULL constraints
SELECT * FROM {{ ref('fact_user_transaction_nb') }}  
WHERE user_id IS NULL OR product_id IS NULL OR search_event_id IS NULL OR timestamp IS NULL;