{{ config(materialized='incremental',
          unique_key='search_event_id',
          incremental_strategy='merge') }}


WITH search_event_data AS (
   SELECT
       search_event_id,
       session_id,
       cart_id,
       search_terms,
       search_results_count AS search_results, -- Renaming column for clarity
       search_type,
       event_ts,
       collector_ts
   FROM {{ ref('stg_user_journey_yl') }}
  {% if is_incremental() %}
    -- Process only recent rows on incremental runs (2-day sliding window)
    WHERE date(collector_ts) >= CURRENT_DATE() - 2
  {% endif %}
)


SELECT *, '{{ run_started_at }}'::timestamp_tz as dbt_processed_at FROM search_event_data