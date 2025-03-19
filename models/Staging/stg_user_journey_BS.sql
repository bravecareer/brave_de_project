{{ config(
   materialized='incremental',
   unique_key=['timestamp', 'SEARCH_EVENT_ID'],
   cluster_by=['SEARCH_EVENT_ID','PRODUCT_ID','SEARCH_TERMS','MKT_CAMPAIGN'],
   on_schema_change='fail'

) }}

WITH search_events AS (
    SELECT 
        SEARCH_EVENT_ID,
        TIMESTAMP,
        APP_ID,
        HAS_PDP,
        HAS_ATC,
        HAS_PURCHASE,
        CART_ID,
        SESSION_ID,
        SEARCH_REQUEST_ID,
        SEARCH_RESULTS_COUNT,
        SEARCH_TERMS,
        SEARCH_FEATURE,
        SEARCH_TERMS_TYPE,
        SEARCH_TYPE,
        LOGIN_STATUS,
        USER_ID,
        BR_LANG, 
        PAGE_LANGUAGE, 
        GEO_COUNTRY, 
        GEO_REGION, 
        GEO_CITY,
        REGISTRATION_STATUS,
        SEARCH_MODEL,
        MKT_MEDIUM,
        COALESCE(MKT_SOURCE, 'Unknown') AS MKT_SOURCE,
        COALESCE(MKT_CONTENT, 'Unknown') AS MKT_CONTENT,
        COALESCE(MKT_CAMPAIGN, 'Unknown') AS MKT_CAMPAIGN,
        PRODUCT_ID,
        current_timestamp() AS dbt_loaded_at,
        'de_project_user_journey' AS dbt_source
    FROM {{ source('de_project', 'user_journey') }}
    WHERE MKT_MEDIUM IS NOT NULL
)

SELECT *
FROM search_events
WHERE
{% if is_incremental() %}
    timestamp > (SELECT max(timestamp) FROM {{ this }})
{% else %}
    TRUE
{% endif %}
