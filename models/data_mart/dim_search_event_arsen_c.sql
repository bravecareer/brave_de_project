{{ config(
   materialized='incremental',
   unique_key='search_event_id'
) }}

WITH search_event_data AS (
   SELECT
       se.search_event_id,
       se.session_id,
       se.cart_id,
       se.search_terms,
       se.search_results_count,
       se.search_type,
       to_timestamp(replace(timestamp,' UTC','')) AS timestamp,
       se.search_model,
       se.search_terms_type,
       se.search_feature,
       se.search_request_id
   FROM {{ source('de_project', 'user_journey') }} se
)

SELECT * FROM search_event_data
