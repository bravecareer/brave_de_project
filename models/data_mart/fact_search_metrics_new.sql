{{
    config(
        materialized='incremental',
        unique_key=['search_event_id', 'date_key'],
        incremental_strategy='delete+insert'
    )
}}

-- Integrated search event dimensions and metrics in a single fact table
-- This combines the previous dim_search_event and fact_search_metrics_new tables
WITH search_events AS (
    SELECT
        search_event_id,
        DATE(event_timestamp) as date_key,
        event_timestamp,
        search_terms,
        search_results_count,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase,
        product_id
    FROM {{ ref('stg_user_journey') }}
    {% if is_incremental() %}
    WHERE DATE(event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

daily_search_metrics AS (
    SELECT
        search_event_id,
        date_key,
        MAX(event_timestamp) as event_timestamp,
        search_terms,
        search_results_count,
        -- Calculate engagement metrics
        COUNT(*) as total_searches,
        COUNT(CASE WHEN has_qv = TRUE THEN 1 END) as total_quick_views,
        COUNT(CASE WHEN has_pdp = TRUE THEN 1 END) as total_product_detail_views,
        COUNT(CASE WHEN has_atc = TRUE THEN 1 END) as total_add_to_cart,
        COUNT(CASE WHEN has_purchase = TRUE THEN 1 END) as total_purchases,
        
        -- Calculate ATC rate (key metric from screenshot)
        ROUND(COUNT(CASE WHEN has_atc = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN has_qv = TRUE THEN 1 END), 0), 2) as atc_rate
    FROM search_events
    GROUP BY 
        search_event_id,
        date_key,
        search_terms,
        search_results_count
)

SELECT * FROM daily_search_metrics
