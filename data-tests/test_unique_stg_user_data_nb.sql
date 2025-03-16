-- Test for unique user_id
SELECT user_id, COUNT(*) 
FROM {{ ref('stg_user_data_nb') }}  
GROUP BY user_id 
HAVING COUNT(*) > 1;

