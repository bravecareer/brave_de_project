{{
    config(
        materialized='incremental',
        unique_key=['search_event_id', 'date_key'],
        incremental_strategy='delete+insert'
    )
}}

WITH search_events AS (
    SELECT
        search_event_id,
        DATE(timestamp) as date_key,
        search_terms,
        search_type,
        search_terms_type,
        search_results_count,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase,
        product_id
    FROM {{ ref('stg_user_journey') }}
    {% if is_incremental() %}
    WHERE DATE(timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

daily_search_metrics AS (
    SELECT
        search_event_id,
        date_key,
        search_terms,
        search_type,
        search_terms_type,
        search_results_count,
        -- Calculate engagement metrics
        COUNT(*) as total_searches,
        COUNT(CASE WHEN has_qv = TRUE THEN 1 END) as total_quick_views,
        COUNT(CASE WHEN has_pdp = TRUE THEN 1 END) as total_product_detail_views,
        COUNT(CASE WHEN has_atc = TRUE THEN 1 END) as total_add_to_cart,
        COUNT(CASE WHEN has_purchase = TRUE THEN 1 END) as total_purchases,
        
        -- Calculate conversion rates using SQL
        ROUND(COUNT(CASE WHEN has_qv = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as quick_view_rate,
        ROUND(COUNT(CASE WHEN has_atc = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN has_qv = TRUE THEN 1 END), 0), 2) as atc_rate,
        ROUND(COUNT(CASE WHEN has_purchase = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN has_atc = TRUE THEN 1 END), 0), 2) as purchase_rate
    FROM search_events
    GROUP BY 
        search_event_id,
        date_key,
        search_terms,
        search_type,
        search_terms_type,
        search_results_count
)

SELECT * FROM daily_search_metrics
