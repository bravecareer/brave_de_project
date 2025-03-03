{{ config(
    materialized='incremental',
    unique_key='date_key',
    incremental_strategy='merge'
) }}

-- Simplified search metrics fact table focusing on ATC rate
WITH search_events AS (
    SELECT
        search_event_id,
        DATE(event_timestamp) as date_key,
        search_terms,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase
    FROM {{ ref('stg_user_journey') }}
    {% if is_incremental() %}
    WHERE DATE(event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

daily_search_metrics AS (
    SELECT
        date_key,
        COUNT(DISTINCT search_event_id) as total_searches,
        COUNT(DISTINCT CASE WHEN has_qv = TRUE THEN search_event_id END) as total_views,
        COUNT(DISTINCT CASE WHEN has_atc = TRUE THEN search_event_id END) as total_add_to_cart,
        COUNT(DISTINCT CASE WHEN has_purchase = TRUE THEN search_event_id END) as total_purchases,
        
        -- ATC rate - key metric for search effectiveness
        ROUND(
            COUNT(DISTINCT CASE WHEN has_atc = TRUE THEN search_event_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT CASE WHEN has_qv = TRUE THEN search_event_id END), 0), 
            2
        ) as atc_rate
    FROM search_events
    GROUP BY date_key
)

SELECT * FROM daily_search_metrics
