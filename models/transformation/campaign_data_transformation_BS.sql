{{ config(
   materialized='incremental',
   unique_key =['MKT_CAMPAIGN','PRODUCT_ID'],
   cluster_by= ['MKT_CAMPAIGN','PRODUCT_ID']             
) }}

WITH search_events AS (
    SELECT *
    FROM {{ ref('stg_user_journey_BS') }} 
    WHERE MKT_CAMPAIGN != 'Unknown'
)

SELECT
    MKT_CAMPAIGN,
    MKT_MEDIUM,
    MKT_SOURCE,
    MKT_CONTENT,
    PRODUCT_ID,
    SEARCH_TERMS,
    SEARCH_EVENT_ID,
    HAS_PURCHASE,
    HAS_ATC,
    USER_ID,
    current_timestamp() AS dbt_loaded_at,
    'stg_user_journey' AS dbt_source
    
FROM search_events sd
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
TRUE
{% endif %}




