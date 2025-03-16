-- Test for NOT NULL constraints
SELECT * FROM {{ ref('stg_user_journey') }}  
WHERE search_event_id IS NULL OR user_id IS NULL OR timestamp IS NULL;
