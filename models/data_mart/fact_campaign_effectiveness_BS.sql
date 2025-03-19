{{ config(
   materialized='incremental',
   unique_key=['MKT_CAMPAIGN', 'PRODUCT_ID','SEARCH_EVENT_ID'],
   cluster_by=['MKT_CAMPAIGN', 'PRODUCT_ID']    
) }}

WITH search_events AS(
    SELECT *
    FROM {{ ref('campaign_data_transformation_BS') }} -- Reference to the user_journey_transformation table
),
    valid_products AS (
    SELECT 
        p.product_id, p.price    --Early Filtering for required columns
    FROM {{ source('de_project', 'product_data') }} p
),
   campaign_summary as (
   SELECT
    se.MKT_CAMPAIGN,
    se.MKT_MEDIUM,
    se.MKT_SOURCE,
    se.MKT_CONTENT,
    se.PRODUCT_ID,
    se.SEARCH_TERMS,
    se.SEARCH_EVENT_ID,
    se.HAS_PURCHASE, 
    se.HAS_ATC, 
    p.price,
    current_timestamp() AS dbt_loaded_at,
    'user_journey_transform_BS' AS dbt_source
FROM search_events se
JOIN valid_products p ON se.product_id = p.product_id

)

select  * from campaign_summary
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
TRUE                              
{% endif %}