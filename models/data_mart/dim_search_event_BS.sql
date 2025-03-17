{{ config(
   materialized='incremental',
   unique_key=['search_terms','search_event_id'],
   cluster_by=['search_terms','search_event_id']
) }}


WITH search_event_data AS (
   SELECT
       search_event_id,
       session_id,
       cart_id,
       search_terms,
       search_results_count AS search_results, -- Renaming column for clarity
       search_type,
       timestamp,
       current_timestamp() AS dbt_loaded_at,
      'stg_user_journey' AS dbt_source
   FROM {{ ref('stg_user_journey_BS') }} 
   WHERE search_event_id IS NOT NULL     
)

SELECT * 
FROM search_event_data
WHERE
{% if is_incremental() %}
   dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
1=1
{% endif %}