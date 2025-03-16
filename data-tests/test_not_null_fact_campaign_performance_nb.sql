-- Test for NOT NULL constraints
SELECT * FROM {{ ref('fact_campaign_performance') }}  
WHERE user_id IS NULL OR search_event_id IS NULL OR timestamp IS NULL;
