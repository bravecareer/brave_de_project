{{ config(
    materialized='table',
    unique_key='search_event_id'
) }}

WITH cleaned_search_data AS (
    SELECT
        s.search_event_id,
        s.session_id,
        s.search_request_id,
        s.search_terms,
        s.search_feature,
        s.search_terms_type,
        s.search_type,
        s.search_results_count
    FROM {{ source('de_project', 'user_journey') }} s
    WHERE s.search_event_id IS NOT NULL
)

SELECT
    search_event_id,
    session_id,
    search_request_id,
    search_terms,
    search_feature,
    search_terms_type,
    search_type,
    search_results_count
FROM cleaned_search_data