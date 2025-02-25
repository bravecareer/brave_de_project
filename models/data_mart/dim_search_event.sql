{{ config(
    materialized='view',
    unique_key='search_event_id'
) }}

-- Get search event data with search details
WITH search_events AS (
    SELECT
        uj.search_event_id,
        uj.search_terms,
        uj.search_type,
        uj.search_feature,
        uj.search_terms_type,
        uj.search_results_count,
        uj.event_timestamp
    FROM {{ ref('stg_user_journey') }} uj
)

SELECT * FROM search_events