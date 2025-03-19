{{ config(
   materialized='incremental',
   unique_key ='PRODUCT_ID',
   cluster_by= 'PRODUCT_ID'          
) }}

WITH search_events AS (
    SELECT *
    FROM {{ ref('stg_user_journey_BS') }} -- Reference to the user_journey table
)

SELECT
    PRODUCT_ID,    
    SEARCH_EVENT_ID,
    HAS_PDP ,
    HAS_ATC,
    HAS_PURCHASE,
    SEARCH_RESULTS_COUNT,  
    USER_ID,  
    current_timestamp() AS dbt_loaded_at,
    'stg_user_journey' AS dbt_source
FROM search_events
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
1=1
{% endif %}

