-- Test for valid email format
SELECT * FROM {{ ref('stg_user_data_nb') }}  
WHERE email NOT LIKE '%@%.%';
