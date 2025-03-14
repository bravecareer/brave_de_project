{{ config(
    materialized='table',
    unique_key='search_event_id'
) }}

WITH search_effectiveness AS (
    SELECT
        s.search_event_id,
        s.user_id,
        s.product_id,
        s.has_qv,
        s.has_pdp,
        s.has_atc,
        s.has_purchase,
        s.search_results_count
    FROM {{ source('de_project', 'user_journey') }} s
)

SELECT
    search_event_id,
    user_id,
    product_id,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
    search_results_count
FROM search_effectiveness