{{ config(
   materialized='view',
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
)

-- Process search terms with additional metadata
SELECT 
    search_request_id,
    search_terms,
    search_terms_type,
    search_type,
    search_feature,
    search_model
FROM search_terms_raw
