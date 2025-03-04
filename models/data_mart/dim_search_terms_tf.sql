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
        search_model
    FROM {{ ref('stg_user_journey_tf') }}
    WHERE search_terms IS NOT NULL 
    AND search_terms != 'UNKNOWN'
    AND search_request_id IS NOT NULL
    AND search_request_id != 'UNKNOWN'
    {% if is_incremental() %}
    -- Only process new search requests in incremental runs
    AND search_request_id NOT IN (SELECT search_request_id FROM {{ this }})
    {% endif %}
)

-- Process search terms with additional metadata
SELECT
    search_request_id,
    search_terms,
    search_terms_type,
    search_type,
    search_feature,
    search_model,
    CURRENT_TIMESTAMP() as first_seen_at,
    CURRENT_TIMESTAMP() as last_updated_at
FROM search_terms_raw
