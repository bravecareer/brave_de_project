{{ config(
   materialized='incremental',
   unique_key=['search_event_id','timestamp']
) }}


WITH search_event_data AS (
   SELECT DISTINCT
       se.search_event_id,
       se.session_id,
       se.cart_id,
       se.search_terms,
       se.search_results_count AS search_results,
       se.search_type,
       to_timestamp(replace(se.timestamp,' UTC','')) AS timestamp
   FROM {{ source('de_project', 'user_journey') }} se
   WHERE se.search_event_id IS NOT NULL
)


SELECT * FROM search_event_data
