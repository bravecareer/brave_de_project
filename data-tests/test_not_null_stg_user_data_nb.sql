-- Test for NOT NULL constraints
SELECT * FROM {{ ref('stg_user_data_nb') }}  
WHERE user_id IS NULL OR email IS NULL;

