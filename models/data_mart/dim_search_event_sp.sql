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
       se.search_results_count AS search_results, -- Renaming column for clarity
       se.search_type,
       se.timestamp,
       se.device_class,
       se.app_id,
   FROM {{ ref('stg_userjourney_sp') }} se
   WHERE se.search_event_id IS NOT NULL
)


SELECT * FROM search_event_data