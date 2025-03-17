{{ config(
   materialized='incremental',
   unique_key =['MKT_CAMPAIGN','PRODUCT_ID'],
   cluster_by= ['MKT_CAMPAIGN','PRODUCT_ID']             
) }}

WITH search_events AS (
    SELECT *
    FROM {{ ref('stg_user_journey_BS') }} -- Reference to the user_journey table
)

SELECT
    MKT_CAMPAIGN,
    MKT_MEDIUM,
    MKT_SOURCE,
    MKT_CONTENT,
    PRODUCT_ID,
    COUNT(DISTINCT sd.SEARCH_TERMS) AS UNIQUE_ITEMS_SEARCH,
    COUNT(DISTINCT sd.SEARCH_EVENT_ID) AS TOTAL_SEARCH_EVENTS,
    COUNT( CASE WHEN sd.HAS_PURCHASE THEN 1  END) AS PURCHASE_EVENTS,
    COUNT( CASE WHEN sd.HAS_ATC THEN 1  END) AS ATC_EVENTS,
    COUNT( CASE WHEN sd.HAS_PDP THEN 1  END) AS PDP_EVENTS,
    count( DISTINCT sd.user_id ) as total_distinct_users,
    current_timestamp() AS dbt_loaded_at,
    'stg_user_journey' AS dbt_source
    
FROM search_events sd
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
TRUE
{% endif %}

GROUP BY
    sd.MKT_CAMPAIGN, sd.MKT_MEDIUM, sd.MKT_SOURCE, sd.MKT_CONTENT, sd.product_id


