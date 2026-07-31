-- test: fact_user_engagement_kl的unique_key are true unique
-- Test: Validate composite unique_key is actually unique for incremental materialization
-- Business Value: Prevent duplicate records in incremental refresh
WITH duplicate_check AS (
    SELECT 
        user_id,
        product_id,
        search_event_id,
        timestamp,
        COUNT(*) AS record_count
    FROM {{ ref('fact_user_engagement_kl') }}
    GROUP BY 
        user_id,
        product_id,
        search_event_id,
        timestamp
    HAVING COUNT(*) > 1
)
SELECT * FROM duplicate_check