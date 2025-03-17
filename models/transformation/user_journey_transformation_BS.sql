{{ config(
   materialized='incremental',
   unique_key =['SEARCH_TERMS','PRODUCT_ID', 'SEARCH_MODEL'],
   cluster_by= [ 'dbt_loaded_at','SEARCH_TERMS']          
) }}

WITH search_events AS (
    SELECT *
    FROM {{ ref('stg_user_journey_BS') }} -- Reference to the user_journey table
)

SELECT
    SEARCH_TERMS,
    SEARCH_MODEL,
    PRODUCT_ID,    
    COUNT(DISTINCT SEARCH_EVENT_ID) AS total_searches,
    COUNT(DISTINCT CASE WHEN HAS_PDP THEN SEARCH_EVENT_ID END) AS searches_with_pdp,
    COUNT(DISTINCT CASE WHEN HAS_ATC THEN SEARCH_EVENT_ID END) AS searches_with_atc,
    COUNT(DISTINCT CASE WHEN HAS_PURCHASE THEN SEARCH_EVENT_ID END) AS searches_with_purchase,
    AVG(SEARCH_RESULTS_COUNT) AS average_search_result_count,  
    COUNT(DISTINCT CASE WHEN REGISTRATION_STATUS = 'registered' THEN SEARCH_EVENT_ID END) AS searches_by_registered_users,
    COUNT(DISTINCT CASE WHEN REGISTRATION_STATUS = 'unknown' THEN SEARCH_EVENT_ID END) AS searches_by_un_registered_users,
    COUNT(DISTINCT USER_ID) AS total_interested_users,  
    current_timestamp() AS dbt_loaded_at,
    'stg_user_journey' AS dbt_source
FROM search_events
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
TRUE
{% endif %}

GROUP BY 
    SEARCH_TERMS,
    SEARCH_MODEL,
    PRODUCT_ID
