{{ config(
   materialized='incremental',
   unique_key=['MKT_CAMPAIGN','PRODUCT_ID','SEARCH_EVENT_ID'],  
   cluster_by=['MKT_CAMPAIGN','PRODUCT_ID']    
) }}

WITH CAMPAIGN_METRICS AS(
  SELECT 
    MKT_CAMPAIGN,
    MKT_MEDIUM,
    MKT_SOURCE,
    MKT_CONTENT,
    PRODUCT_ID,
    SEARCH_EVENT_ID,
    current_timestamp() AS dbt_loaded_at,
    'stg_user_journey' AS dbt_source
   FROM {{ ref('stg_user_journey_BS') }} 
    
   
    
)

SELECT * 
FROM CAMPAIGN_METRICS
WHERE
{% if is_incremental() %}
   dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
TRUE
{% endif %}