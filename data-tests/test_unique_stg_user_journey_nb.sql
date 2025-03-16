-- Test for unique search_event_id
SELECT search_event_id, COUNT(*) 
FROM {{ ref('stg_user_journey') }}  
GROUP BY search_event_id 
HAVING COUNT(*) > 1;
