{{ config(
    materialized='incremental',
    unique_key='search_event_id',
    incremental_strategy='merge'
) }}

-- Filter and prepare search events data
WITH filtered_search_events AS (
    SELECT
        search_event_id,
        user_id,
        product_id,
        session_id,
        event_timestamp,
        search_request_id,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase,
        mkt_campaign as campaign_id
    FROM {{ ref('stg_user_journey_tf') }} uj
    WHERE uj.search_event_id IS NOT NULL
    AND uj.mkt_campaign != 'UNKNOWN'
    AND uj.search_request_id IS NOT NULL
    AND uj.search_request_id != 'UNKNOWN'
    {% if is_incremental() %}
    AND DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

-- Add derived metrics and transformations
search_metrics AS (
    SELECT
        search_event_id,
        user_id,
        product_id,
        session_id,
        DATE(event_timestamp) AS date_key,
        search_request_id,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase,
        campaign_id,
        -- Add derived metrics for easier analysis
        CASE 
            WHEN has_purchase THEN 1 
            ELSE 0 
        END as conversion_flag,
        CASE
            WHEN has_pdp OR has_qv THEN 1
            ELSE 0
        END as engagement_flag
    FROM filtered_search_events
)

-- Final selection
SELECT
    search_event_id,
    user_id,
    product_id,
    session_id,
    date_key,
    search_request_id,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
    campaign_id,
    conversion_flag,
    engagement_flag
FROM search_metrics