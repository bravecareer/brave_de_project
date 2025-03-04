{{ config(
    materialized='incremental',
    unique_key='search_event_id',
    incremental_strategy='merge'
) }}

-- Simplified search metrics fact table
WITH search_events AS (
    SELECT
        uj.search_event_id,
        uj.user_id,
        uj.product_id,
        uj.event_timestamp,
        DATE(uj.event_timestamp) as date_key,
        -- Use search_request_id as foreign key to dim_search_terms_tf
        uj.search_request_id,
        uj.search_results_count,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        -- Add campaign_id to track which campaign influenced the search behavior
        COALESCE(uj.mkt_campaign, 'Unknown') as campaign_id
    FROM {{ ref('stg_user_journey_tf') }} uj
    WHERE uj.search_event_id IS NOT NULL
    AND uj.search_request_id IS NOT NULL
    AND uj.search_request_id != 'UNKNOWN'
    {% if is_incremental() %}
    AND DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
)

-- Only include raw metrics, move calculations to BI layer
SELECT
    search_event_id,
    user_id,
    product_id,
    event_timestamp,
    search_request_id,
    search_results_count,
    campaign_id  -- Added campaign_id to enable campaign influence analysis
FROM search_events
GROUP BY
    search_event_id,
    user_id,
    product_id,
    event_timestamp,
    search_request_id,
    search_results_count,
    campaign_id
