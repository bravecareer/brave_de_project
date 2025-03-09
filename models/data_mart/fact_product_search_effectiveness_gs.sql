{{ config(
    materialized='table',
    unique_keys=['product_id']
) }}

WITH user_journey_clean AS (
    SELECT 
        search_event_id,
        product_id,
        COALESCE(has_atc, FALSE) AS has_atc  
    FROM {{ ref('view_user_journey_transformed_gs') }}  -- Corrected reference
    WHERE 
        product_id IS NOT NULL
        AND search_event_id IS NOT NULL
)

SELECT 
    ujc.product_id,
    dpd.product_name,
    COUNT(DISTINCT ujc.search_event_id) AS total_searches,  
    SUM(CASE WHEN ujc.has_atc THEN 1 ELSE 0 END) AS total_atc_events,  
    CASE 
        WHEN COUNT(DISTINCT ujc.search_event_id) = 0 THEN 0
        ELSE SUM(CASE WHEN ujc.has_atc THEN 1 ELSE 0 END) / COUNT(DISTINCT ujc.search_event_id)::FLOAT
    END AS atc_rate
FROM user_journey_clean ujc
JOIN {{ ref('dim_product_data_gs') }} dpd  -- Using the incremental dimension table
ON ujc.product_id = dpd.product_id
GROUP BY ujc.product_id, dpd.product_name
