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
    SEARCH_EVENT_ID,
    HAS_PDP ,
    HAS_ATC ,
    HAS_PURCHASE ,
    SEARCH_RESULTS_COUNT,  
    CASE 
        WHEN REGISTRATION_STATUS ='registered' THEN 'registered'
        ELSE 'unregistered'
    END  AS USER_REGISTRATION_STATUS,
    USER_ID,  
    current_timestamp() AS dbt_loaded_at,
    'stg_user_journey' AS dbt_source
FROM search_events
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
TRUE
{% endif %}

