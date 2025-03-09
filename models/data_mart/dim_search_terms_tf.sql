{{ config(
   materialized='incremental',
   unique_key='search_request_id'
) }}

-- Dimension table for search terms
-- Standardizes search terms to reduce storage of duplicate data
WITH search_terms_raw AS (
    SELECT DISTINCT
        search_request_id,
        search_terms,
        search_terms_type,
        search_type,
        search_feature,
        search_model,
        MAX(event_timestamp) as last_search_time
    FROM {{ ref('stg_user_journey_tf') }}
    WHERE search_terms IS NOT NULL 
    AND search_terms != 'UNKNOWN'
    AND search_request_id IS NOT NULL
    AND search_request_id != 'UNKNOWN'
    {% if is_incremental() %}
    -- Only process new search terms in incremental runs
    AND event_timestamp >= CURRENT_DATE() - 3
    {% endif %}
    GROUP BY 1,2,3,4,5,6
)

-- Process search terms with additional metadata
SELECT 
    search_request_id,
    search_terms,
    search_terms_type,
    search_type,
    search_feature,
    search_model,
    last_search_time
FROM search_terms_raw
